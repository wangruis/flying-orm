package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.flying.orm.rdb.schema.SchemaMigrationResult.addRows;

/**
 * Schema 的同步 JDBC 执行编排。
 *
 * <p>这里不把 JDBC 调用包装成 Reactor，也不设置或恢复会话锁等待参数。
 * 执行时限和会话治理由上层负责，Schema 只执行计划 SQL 并报告实际结果。</p>
 *
 * <p>元数据失效会尝试全部目标。DDL 成功而失效失败时报告缓存一致性失败；两者都失败时，数据库失败保持
 * primary，失效失败作为 suppressed 保留。</p>
 */
final class JdbcSchemaMigrationExecutor {
    private final SyncSqlExecutor executor;
    private final SchemaMigrationObserver observer;

    JdbcSchemaMigrationExecutor(SyncSqlExecutor executor,
                                SchemaMigrationObserver observer) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
    }
    long execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return 0L;
        }
        ExecutionState state = new ExecutionState(false);
        return executeWork(requests, options, state);
    }
    <T> long executeWithInvalidation(List<SqlRequest> requests,
                                 List<T> tables,
                                 Consumer<T> invalidator,
                                 SqlExecutionOptions options) {
        return executeWithInvalidation(requests, tables, invalidator, options, new ExecutionState(false));
    }

    private <T> long executeWithInvalidation(List<SqlRequest> requests, List<T> tables,
                                         Consumer<T> invalidator, SqlExecutionOptions options,
                                         ExecutionState state) {
        boolean reportSteps = state.steps != null;
        if (requests.isEmpty()) {
            return 0L;
        }
        Consumer<T> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        Throwable primaryFailure = null;
        try {
            return executeWork(requests, options, state);
        } catch (RuntimeException failure) {
            RuntimeException reported = reportSteps ? sequenceFailure(state, requests.size(), failure) : failure;
            primaryFailure = reported;
            throw reported;
        } catch (Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                if (state.started) {
                    SchemaCacheInvalidationCoordinator.invalidateTables(safeInvalidator, tables, primaryFailure);
                }
            } catch (RuntimeException failure) {
                if (!reportSteps) throw failure;
                throw new SqlExecutionSequenceException(
                        SqlExecutionPhase.CLEANUP, state.steps.size(), state.steps, failure);
            }
        }
    }

    private static RuntimeException sequenceFailure(ExecutionState state, int requestCount,
                                                     RuntimeException failure) {
        if (state.failedStepIndex >= 0) {
            return new SqlExecutionSequenceException(
                    SqlExecutionPhase.WORK, state.failedStepIndex, state.steps, failure);
        }
        return state.steps.size() == requestCount
                ? new SqlExecutionSequenceException(SqlExecutionPhase.CLEANUP, requestCount, state.steps, failure)
                : failure;
    }
    <T> SchemaMigrationResult execute(SchemaMigrationPlan plan, List<T> tables,
                                      Consumer<T> invalidator, SqlExecutionOptions options) {
        SchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "schema migration plan must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        ExecutionState state = new ExecutionState(true);
        long startedAt = System.nanoTime();
        try {
            long rows = executeWithInvalidation(
                    safePlan.requestsForExecution(), tables, invalidator, safeOptions, state);
            SchemaMigrationResult result = new SchemaMigrationResult(safePlan, rows, state.steps);
            SchemaMigrationObservers.observe(observer, safePlan, result, state.steps,
                    startedAt, SqlExecutionStatus.SUCCESS, null);
            return result;
        } catch (RuntimeException error) {
            SchemaMigrationObservers.observe(observer, safePlan, null, state.steps,
                    startedAt, SqlExecutionStatus.ERROR, error);
            throw error;
        }
    }

    private long executeWork(List<SqlRequest> requests,
                             SqlExecutionOptions options,
                             ExecutionState state) {
        return MySqlSchemaCommentSupport.execute(executor, requests, options, bound -> {
            state.started = true;
            return executeBoundWork(bound, requests, options, state);
        });
    }

    private long executeBoundWork(SyncSqlExecutor bound, List<SqlRequest> requests,
                                  SqlExecutionOptions options, ExecutionState state) {
        long rows = 0L;
        for (int index = 0; index < requests.size(); index++) {
            SqlRequest request = requests.get(index);
            long startedAt = state.steps == null ? 0L : System.nanoTime();
            try {
                long affectedRows = Math.max(0L, bound.rowsUpdated(request, options));
                if (state.steps != null) {
                    state.steps.add(new SqlExecutionStepResult(
                            index, request, affectedRows, System.nanoTime() - startedAt));
                }
                rows = addRows(rows, affectedRows);
            } catch (RuntimeException error) {
                state.failedStepIndex = index;
                throw error;
            }
        }
        return rows;
    }

    private static final class ExecutionState {
        private final List<SqlExecutionStepResult> steps;
        private int failedStepIndex = -1;
        private boolean started;

        private ExecutionState(boolean reportSteps) {
            steps = reportSteps ? new ArrayList<>() : null;
        }
    }
}
