package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Schema 的同步 JDBC 执行编排。
 *
 * <p>这里故意不把 JDBC 调用包装成 Reactor。每条 DDL 都直接交给同步执行器，
 * 只在需要锁超时的场景把 setup、work、cleanup 连续交给同一个外部事务连接。
 * 没有这样的连接能力时，方法会在第一条 SQL 前拒绝，而不是假装多个连接属于同一个会话。</p>
 *
 * <p>元数据失效会尝试全部目标。DDL 成功而失效失败时明确报告缓存一致性失败；DDL 同时失败时，数据库失败
 * 保持 primary，失效失败作为 suppressed 保留。</p>
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
        this.ddlTransactionSupport = Objects.requireNonNull(ddlTransactionSupport,
                                                            "DDL transaction support must not be null");
        this.transactionParticipant = Objects.requireNonNull(transactionParticipant,
                                                             "jdbc transaction participant must not be null");
        this.metadataInvalidator = Objects.requireNonNull(metadataInvalidator,
                                                          "schema metadata invalidator must not be null");
    }
    long execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return 0L;
        }
        guardExternalTransaction(requests, false);
        ExecutionState state = new ExecutionState();
        state.started = true;
        return executeWork(requests, options, state);
    }
    long executeWithInvalidation(List<SqlRequest> requests,
                                 List<String> tables,
                                 Consumer<String> invalidator,
                                 SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return 0L;
        }
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                invalidator, "schema metadata invalidator must not be null");
        ExecutionState state = new ExecutionState();
        guardExternalTransaction(requests, false, tables, safeInvalidator, null, state);
        state.started = true;
        Throwable primaryFailure = null;
        try {
            return executeWork(requests, options, state);
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            invalidateAfterExecution(state, tables, safeInvalidator, primaryFailure);
        }
    }
    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan plan,
                                          List<String> tables,
                                          SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema migration plan must not be null");
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        ExecutionState state = new ExecutionState();
        long startedAt = System.nanoTime();
        String planFingerprint = safePlan.fingerprint();
        Throwable primaryFailure = null;
        try {
            List<SqlRequest> requests = safePlan.requestsForExecution(
                    safeOptions.approval(), planFingerprint);
            guardExternalTransaction(requests,
                                     safePlan.onlineDdl().requiresNonTransactionalExecution(),
                                     tables, metadataInvalidator, planFingerprint, state);
            long rows = requests.isEmpty()
                    ? 0L
                    : executeReviewedRequests(requests, safeOptions, state, planFingerprint);
            SchemaMigrationResult result = new SchemaMigrationResult(safePlan.migration(), rows, state.steps);
            observe(state, safePlan, planFingerprint, result, startedAt, SqlExecutionStatus.SUCCESS, null);
            return result;
        } catch (RuntimeException error) {
            primaryFailure = error;
            observe(state, safePlan, planFingerprint, null, startedAt, SqlExecutionStatus.ERROR, error);
            throw error;
        } catch (Error error) {
            primaryFailure = error;
            throw error;
        } finally {
            invalidateAfterExecution(state, tables, metadataInvalidator, primaryFailure);
        }
    }
    private long executeReviewedRequests(List<SqlRequest> requests,
                                         SchemaMigrationExecutionOptions options,
                                         ExecutionState state,
                                         String planFingerprint) {
        if (!options.hasLockTimeout()) {
            state.started = true;
            return executeWork(requests, options.sqlExecutionOptions(), state);
        }
        if (transactionParticipant.currentTransaction().isEmpty()) {
            throw new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                    planFingerprint,
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
            executePhase(guard.cleanup(), options.withTimeout(options.cleanupTimeout()), state);
        } catch (RuntimeException cleanupFailure) {
            SqlExecutionSequenceException sequenceFailure = sequenceFailure(state, cleanupFailure);
            VirtualMachineError primaryFatal = directVirtualMachineError(primaryFailure);
            if (primaryFatal != null) {
                suppress(primaryFatal, sequenceFailure);
                throw primaryFatal;
            }
            VirtualMachineError cleanupFatal = directVirtualMachineError(sequenceFailure);
            if (cleanupFatal != null) {
                suppress(cleanupFatal, primaryFailure);
                throw cleanupFatal;
            }
            if (primaryFailure != null) {
                suppress(primaryFailure, sequenceFailure);
                rethrow(primaryFailure);
            }
            throw sequenceFailure;
        } catch (Error cleanupFailure) {
            VirtualMachineError primaryFatal = directVirtualMachineError(primaryFailure);
            if (primaryFatal != null) {
                suppress(primaryFatal, cleanupFailure);
                throw primaryFatal;
            }
            VirtualMachineError cleanupFatal = directVirtualMachineError(cleanupFailure);
            if (cleanupFatal != null) {
                suppress(cleanupFatal, primaryFailure);
                throw cleanupFatal;
            }
            if (primaryFailure != null) {
                suppress(primaryFailure, cleanupFailure);
                rethrow(primaryFailure);
            }
            throw cleanupFailure;
        }
        if (primaryFailure != null) {
            rethrow(primaryFailure);
        }
        return rows;
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof Error fatal) {
            throw fatal;
        }
        if (failure instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        throw new IllegalStateException("schema migration failed with an unsupported checked exception", failure);
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
                state.steps.add(new SqlExecutionStepResult(
                        index, request, affectedRows, System.nanoTime() - startedAt));
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
        guardExternalTransaction(
                requests, requiresNonTransactionalExecution, null, null, null, null);
    }

    private void guardExternalTransaction(List<SqlRequest> requests,
                                          boolean requiresNonTransactionalExecution,
                                          List<String> tables,
                                          Consumer<String> invalidator,
                                          String planFingerprint,
                                          ExecutionState state) {
        if (requests.isEmpty()) {
            return;
        }
        java.util.Optional<JdbcTransactionContext> current = transactionParticipant.currentTransaction();
        if (current.isEmpty()) {
            return;
        }
        if (!ddlTransactionSupport.allowsExternalTransaction() || requiresNonTransactionalExecution) {
            throw rejection(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    planFingerprint,
                    "JDBC DDL cannot join the current external transaction: support="
                            + ddlTransactionSupport);
        }
        if (tables != null) {
            ExecutionState invalidationState = Objects.requireNonNull(
                    state, "schema invalidation state must not be null");
            if (!current.get().completion().register(
                    ignored -> Mono.fromRunnable(() -> invalidateAtTransactionCompletion(
                            invalidationState, tables, invalidator)))) {
                throw rejection(
                        SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                        planFingerprint,
                        "external JDBC DDL requires transaction completion notification for metadata consistency");
            }
            invalidationState.transactionCompletionRegistered = true;
        }
    }

    private static SchemaMigrationRejectedException rejection(SchemaMigrationFailureCode failureCode,
                                                               String planFingerprint,
                                                               String message) {
        return planFingerprint == null
                ? new SchemaMigrationRejectedException(failureCode, message)
                : new SchemaMigrationRejectedException(failureCode, planFingerprint, message);
    }

    private void observe(ExecutionState state,
                         ReviewedSchemaMigrationPlan plan,
                         String planFingerprint,
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
        SqlExecutionPhase failedPhase = null;
        Integer failedStepIndex = null;
        if (error instanceof SqlExecutionSequenceException sequenceFailure) {
            failedPhase = sequenceFailure.phase();
            failedStepIndex = sequenceFailure.stepIndex();
        } else if (error != null && state.failedStepIndex >= 0) {
            failedPhase = SqlExecutionPhase.WORK;
            failedStepIndex = state.failedStepIndex;
        }
        observer.onMigration(new SchemaMigrationObservation(
                planFingerprint,
                plan.riskLevel(),
                status,
                plan.migration().executableSqlCount(),
                result == null ? state.steps.size() : result.steps().size(),
                observedRows,
                System.nanoTime() - startedAt,
                category, failedPhase, failedStepIndex,
                error));
    }

    private static void invalidateAfterExecution(ExecutionState state,
                                                 List<String> tables,
                                                 Consumer<String> invalidator,
                                                 Throwable primaryFailure) {
        if (!state.started || state.transactionCompletionRegistered) {
            return;
        }
        try {
            invalidateTables(invalidator, tables);
        } catch (RuntimeException | Error invalidationFailure) {
            if (primaryFailure == null) {
                throw invalidationFailure;
            }
            suppress(primaryFailure, invalidationFailure);
        }
    }

    private static void invalidateAtTransactionCompletion(ExecutionState state,
                                                          List<String> tables,
                                                          Consumer<String> invalidator) {
        if (state.started) {
            invalidateTables(invalidator, tables);
        }
    }

    private static void invalidateTables(Consumer<String> invalidator, List<String> tables) {
        SchemaCacheInvalidationCoordinator.invalidateTables(invalidator, tables);
    }

    private static VirtualMachineError directVirtualMachineError(Throwable failure) {
        return failure instanceof VirtualMachineError fatal ? fatal : null;
    }

    private static void suppress(Throwable primary, Throwable secondary) {
        if (primary != null && secondary != null && primary != secondary) {
            primary.addSuppressed(secondary);
        }
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null, null, overflow);
        }
    }

    private static final class ExecutionState {
        private final List<SqlExecutionStepResult> steps = new ArrayList<>();
        private SqlExecutionPhase phase;
        private int failedStepIndex = -1;
        private volatile boolean started;
        private volatile boolean transactionCompletionRegistered;
    }
}
