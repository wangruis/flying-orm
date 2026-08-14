package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Schema 的同步 JDBC 执行编排。
 *
 * <p>这里故意不把 JDBC 调用包装成 Reactor。每条 DDL 都直接交给同步执行器，
 * 只在需要锁超时的场景把 setup、work、cleanup 连续交给同一个外部事务连接。
 * 没有这样的连接能力时，方法会在第一条 SQL 前拒绝，而不是假装多个连接属于同一个会话。</p>
 *
 * <p>元数据失效是旁路动作。DDL 已经发给数据库后，即使失效回调失败，也不能把已经发生的结构变化说成失败。</p>
 */
final class JdbcSchemaMigrationExecutor {

    private final SyncSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final SchemaMigrationObserver observer;
    private final SchemaDdlTransactionSupport ddlTransactionSupport;
    private final JdbcTransactionParticipant transactionParticipant;
    private final Consumer<String> metadataInvalidator;

    JdbcSchemaMigrationExecutor(SyncSqlExecutor executor,
                                FormSchemaSqlRenderer renderer,
                                SchemaMigrationObserver observer,
                                SchemaDdlTransactionSupport ddlTransactionSupport,
                                JdbcTransactionParticipant transactionParticipant,
                                Consumer<String> metadataInvalidator) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
        this.ddlTransactionSupport = Objects.requireNonNull(
                ddlTransactionSupport, "DDL transaction support must not be null");
        this.transactionParticipant = Objects.requireNonNull(
                transactionParticipant, "jdbc transaction participant must not be null");
        this.metadataInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
    }

    long execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        List<SqlRequest> safeRequests = safeRequests(requests);
        if (safeRequests.isEmpty()) {
            return 0L;
        }
        guardExternalTransaction(safeRequests, false);
        ExecutionState state = new ExecutionState();
        state.started = true;
        return executeWork(safeRequests, options, state);
    }

    long executeWithInvalidation(List<SqlRequest> requests,
                                 List<String> tables,
                                 Consumer<String> invalidator,
                                 SqlExecutionOptions options) {
        List<SqlRequest> safeRequests = safeRequests(requests);
        if (safeRequests.isEmpty()) {
            return 0L;
        }
        List<String> safeTables = safeTables(tables);
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        guardExternalTransaction(safeRequests, false);
        ExecutionState state = new ExecutionState();
        state.started = true;
        try {
            return executeWork(safeRequests, options, state);
        } finally {
            invalidateAfterExecution(state, safeTables, safeInvalidator);
        }
    }

    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan plan,
                                          List<String> tables,
                                          SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema migration plan must not be null");
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        List<String> safeTables = safeTables(tables);
        ExecutionState state = new ExecutionState();
        long startedAt = System.nanoTime();
        try {
            List<SqlRequest> requests = safePlan.requestsForExecution(safeOptions.approval());
            guardExternalTransaction(requests, safePlan.onlineDdl().requiresNonTransactionalExecution());
            long rows = requests.isEmpty()
                    ? 0L
                    : executeReviewedRequests(requests, safePlan, safeOptions, state);
            SchemaMigrationResult result = new SchemaMigrationResult(safePlan.migration(), rows, state.steps);
            observe(state, safePlan, result, startedAt, SqlExecutionStatus.SUCCESS, null);
            return result;
        } catch (RuntimeException error) {
            observe(state, safePlan, null, startedAt, SqlExecutionStatus.ERROR, error);
            throw error;
        } finally {
            invalidateAfterExecution(state, safeTables, metadataInvalidator);
        }
    }

    private long executeReviewedRequests(List<SqlRequest> requests,
                                         ReviewedSchemaMigrationPlan plan,
                                         SchemaMigrationExecutionOptions options,
                                         ExecutionState state) {
        if (!options.hasLockTimeout()) {
            state.started = true;
            return executeWork(requests, options.sqlExecutionOptions(), state);
        }
        if (transactionParticipant.currentTransactionForExecution().isEmpty()) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                    plan.fingerprint(),
                    "JDBC DDL lock timeout requires one externally managed transaction connection");
        }
        state.started = true;
        SchemaDdlSessionGuard guard = renderer.lockTimeoutGuard(options.lockTimeout());
        return executeSession(guard, requests, options.sqlExecutionOptions(), state);
    }

    private long executeSession(SchemaDdlSessionGuard guard,
                                List<SqlRequest> requests,
                                SqlExecutionOptions options,
                                ExecutionState state) {
        Throwable primaryFailure = null;
        long rows = 0L;
        state.phase = SqlExecutionPhase.SETUP;
        try {
            executePhase(guard.setup(), options, state);
            state.phase = SqlExecutionPhase.WORK;
            rows = executeWork(requests, options, state);
        } catch (RuntimeException error) {
            primaryFailure = sequenceFailure(state, error);
        } catch (Error error) {
            primaryFailure = error;
        }
        state.phase = SqlExecutionPhase.CLEANUP;
        try {
            executePhase(guard.cleanup(), options, state);
        } catch (RuntimeException cleanupFailure) {
            SqlExecutionSequenceException sequenceFailure = sequenceFailure(state, cleanupFailure);
            VirtualMachineError primaryFatal = SchemaMigrationObservers.findVirtualMachineError(primaryFailure);
            if (primaryFatal != null) {
                addSuppressedIfAcyclic(primaryFatal, sequenceFailure);
                throw primaryFatal;
            }
            VirtualMachineError cleanupFatal = SchemaMigrationObservers.findVirtualMachineError(sequenceFailure);
            if (cleanupFatal != null) {
                addSuppressedIfAcyclic(cleanupFatal, primaryFailure);
                throw cleanupFatal;
            }
            if (primaryFailure != null) {
                addSuppressedIfAcyclic(sequenceFailure, primaryFailure);
            }
            throw sequenceFailure;
        } catch (Error cleanupFailure) {
            VirtualMachineError primaryFatal = SchemaMigrationObservers.findVirtualMachineError(primaryFailure);
            if (primaryFatal != null) {
                addSuppressedIfAcyclic(primaryFatal, cleanupFailure);
                throw primaryFatal;
            }
            VirtualMachineError cleanupFatal = SchemaMigrationObservers.findVirtualMachineError(cleanupFailure);
            if (cleanupFatal != null) {
                addSuppressedIfAcyclic(cleanupFatal, primaryFailure);
                throw cleanupFatal;
            }
            addSuppressedIfAcyclic(cleanupFailure, primaryFailure);
            throw cleanupFailure;
        }
        if (primaryFailure != null) {
            SchemaMigrationObservers.rethrowVirtualMachineError(primaryFailure);
            rethrow(primaryFailure);
        }
        return rows;
    }

    /** VME/Error 的清理诊断只能连接为无环的 Throwable 图，避免观察或日志遍历时再次失败。 */
    private static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (secondary == null || primary == secondary || reaches(primary, secondary) || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    private static boolean reaches(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        throw (RuntimeException) failure;
    }

    private static SqlExecutionSequenceException sequenceFailure(ExecutionState state,
                                                                  RuntimeException failure) {
        if (failure instanceof SqlExecutionSequenceException sequenceFailure) {
            return sequenceFailure;
        }
        int stepIndex = state.phase == SqlExecutionPhase.WORK
                ? Math.max(0, state.failedStepIndex)
                : 0;
        return new SqlExecutionSequenceException(state.phase, stepIndex, state.steps, failure);
    }

    private void executePhase(List<SqlRequest> requests,
                              SqlExecutionOptions options,
                              ExecutionState state) {
        for (int index = 0; index < requests.size(); index++) {
            try {
                executor.rowsUpdated(requests.get(index), options);
            } catch (RuntimeException failure) {
                throw new SqlExecutionSequenceException(state.phase, index, state.steps, failure);
            }
        }
    }

    private long executeWork(List<SqlRequest> requests,
                             SqlExecutionOptions options,
                             ExecutionState state) {
        long rows = 0L;
        state.phase = SqlExecutionPhase.WORK;
        for (int index = 0; index < requests.size(); index++) {
            SqlRequest request = requests.get(index);
            long startedAt = System.nanoTime();
            try {
                long affectedRows = executor.rowsUpdated(request, options);
                state.steps.add(new SqlExecutionStepResult(index,
                                                            request,
                                                            affectedRows,
                                                            System.nanoTime() - startedAt));
                rows = addExact(rows, affectedRows);
            } catch (RuntimeException error) {
                state.failedStepIndex = index;
                throw error;
            }
        }
        return rows;
    }

    private void guardExternalTransaction(List<SqlRequest> requests,
                                          boolean requiresNonTransactionalExecution) {
        if (requests.isEmpty()
                || (ddlTransactionSupport.allowsExternalTransaction()
                    && !requiresNonTransactionalExecution)) {
            return;
        }
        if (transactionParticipant.currentTransactionForExecution().isPresent()) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    "JDBC DDL cannot join the current external transaction: support="
                            + ddlTransactionSupport);
        }
    }

    private void observe(ExecutionState state,
                         ReviewedSchemaMigrationPlan plan,
                         SchemaMigrationResult result,
                         long startedAt,
                         SqlExecutionStatus status,
                         Throwable error) {
        SqlFailureCategory category = status == SqlExecutionStatus.SUCCESS
                ? SqlFailureCategory.NONE
                : SqlFailureCategory.classify(error);
        long observedRows;
        try {
            observedRows = result == null ? state.steps.stream()
                                                 .mapToLong(SqlExecutionStepResult::rowsUpdated)
                                                 .reduce(0L, JdbcSchemaMigrationExecutor::addExact)
                                           : result.rowsUpdated();
        } catch (RdbException overflow) {
            // 失败路径没有可精确表示的汇总时宁可不发观测，也不能覆盖原始数据库异常。
            return;
        }
        observer.onMigration(new SchemaMigrationObservation(
                plan.fingerprint(),
                plan.riskLevel(),
                status,
                plan.migration().executableSqlCount(),
                result == null ? state.steps.size() : result.steps().size(),
                observedRows,
                System.nanoTime() - startedAt,
                category,
                error == null || state.phase == null ? null : state.phase,
                error == null || state.failedStepIndex < 0 ? null : state.failedStepIndex,
                error));
    }

    private static void invalidateAfterExecution(ExecutionState state,
                                                 List<String> tables,
                                                 Consumer<String> invalidator) {
        if (!state.started || state.invalidated) {
            return;
        }
        state.invalidated = true;
        for (String table : tables) {
            try {
                invalidator.accept(table);
            } catch (RuntimeException failure) {
                SchemaMigrationObservers.rethrowVirtualMachineError(failure);
            }
            // DDL 已经执行后，普通缓存清理失败只能记录在上层监控，不能伪造成 DDL 失败。
        }
    }

    private static List<SqlRequest> safeRequests(List<SqlRequest> requests) {
        return List.copyOf(Objects.requireNonNull(requests, "schema SQL requests must not be null"));
    }

    private static List<String> safeTables(List<String> tables) {
        List<String> copied = List.copyOf(Objects.requireNonNull(
                tables, "schema metadata tables must not be null"));
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String table : copied) {
            if (table.isBlank()) {
                throw new IllegalArgumentException("schema metadata table must not be blank");
            }
            unique.add(table);
        }
        if (unique.isEmpty()) {
            throw new IllegalArgumentException("schema metadata tables must not be empty");
        }
        return List.copyOf(unique);
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }

    private static final class ExecutionState {
        private final List<SqlExecutionStepResult> steps = new ArrayList<>();
        private SqlExecutionPhase phase;
        private int failedStepIndex = -1;
        private boolean started;
        private boolean invalidated;
    }
}
