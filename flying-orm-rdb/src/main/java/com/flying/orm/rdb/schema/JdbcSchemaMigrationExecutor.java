package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.addExact;
import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.directVirtualMachineError;
import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.rejection;
import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.rethrow;
import static com.flying.orm.rdb.schema.JdbcSchemaExecutionSupport.suppress;

/**
 * Schema 的同步 JDBC 执行编排。
 *
 * <p>这里不把 JDBC 调用包装成 Reactor。锁超时时，setup、work、cleanup 连续交给同一个外部事务连接；
 * 没有这种能力就在第一条 SQL 前拒绝，不把多个连接冒充为同一会话。</p>
 *
 * <p>元数据失效会尝试全部目标。DDL 成功而失效失败时报告缓存一致性失败；两者都失败时，数据库失败保持
 * primary，失效失败作为 suppressed 保留。</p>
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
        MySqlSchemaCommentSupport.validate(executor, requests, options, null);
        JdbcSchemaExecutionState state = new JdbcSchemaExecutionState();
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
        JdbcSchemaExecutionState state = new JdbcSchemaExecutionState();
        guardExternalTransaction(requests, false, tables, safeInvalidator, null, state);
        MySqlSchemaCommentSupport.validate(executor, requests, options, null);
        state.started = true;
        Throwable primaryFailure = null;
        try {
            return executeWork(requests, options, state);
        } catch (RuntimeException | Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            state.invalidateAfterExecution(tables, safeInvalidator, primaryFailure);
        }
    }
    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan plan,
                                          List<String> tables,
                                          SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema migration plan must not be null");
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        JdbcSchemaExecutionState state = new JdbcSchemaExecutionState();
        long startedAt = System.nanoTime();
        String planFingerprint = safePlan.fingerprint();
        Throwable primaryFailure = null;
        try {
            List<SqlRequest> requests = safePlan.requestsForExecution(
                    safeOptions.approval(), planFingerprint);
            guardExternalTransaction(requests,
                                     safePlan.onlineDdl().requiresNonTransactionalExecution(),
                                     tables, metadataInvalidator, planFingerprint, state);
            MySqlSchemaCommentSupport.validate(executor, requests, safeOptions.sqlExecutionOptions(), planFingerprint);
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
            state.invalidateAfterExecution(tables, metadataInvalidator, primaryFailure);
        }
    }
    SchemaExecutionReport executeReviewed(ReviewedSchemaPlan plan, Supplier<SchemaSnapshot> snapshotReader,
                                          Supplier<SchemaSnapshotCoverage> coverageReader, Runnable metadataInvalidator,
                                          SchemaMigrationExecutionOptions options) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(plan, "reviewed schema plan must not be null");
        Runnable safeInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        return VerifiedSchemaPlanExecutor.executeJdbcGuarded(
                safePlan, snapshotReader, coverageReader, safeInvalidator, safeOptions,
                steps -> executeVerifiedSteps(safePlan, steps, safeInvalidator, safeOptions));
    }
    private VerifiedSchemaPlanExecutor.ExecutionAttempt executeVerifiedSteps(
            ReviewedSchemaPlan plan, List<SchemaPlanStep> steps, Runnable invalidator,
            SchemaMigrationExecutionOptions options) {
        if (steps.isEmpty()) {
            return VerifiedSchemaPlanExecutor.successfulAttempt(steps, List.of());
        }
        List<SqlRequest> requests = steps.stream()
                .map(step -> step.request().orElseThrow(() -> new IllegalStateException(
                        "reviewed schema execution contains a non-executable step")))
                .toList();
        List<String> tables = List.of(plan.desiredTable().orElseThrow().identity().table());
        Consumer<String> tableInvalidator = ignored -> invalidator.run();
        JdbcSchemaExecutionState state = new JdbcSchemaExecutionState();
        Throwable primaryFailure = null;
        RuntimeException invalidationFailure = null;
        VerifiedSchemaPlanExecutor.ExecutionAttempt attempt;
        guardExternalTransaction(requests, false, tables, tableInvalidator, plan.fingerprint(), state);
        MySqlSchemaCommentSupport.validate(executor, requests, options.sqlExecutionOptions(), plan.fingerprint());
        try {
            executeReviewedRequests(requests, options, state, plan.fingerprint());
            attempt = state.transactionCompletionRegistered
                    ? VerifiedSchemaPlanExecutor.externalTransactionAttempt(steps, state.steps)
                    : VerifiedSchemaPlanExecutor.successfulAttempt(steps, state.steps);
        } catch (SchemaMigrationRejectedException rejection) {
            primaryFailure = rejection;
            throw rejection;
        } catch (RuntimeException failure) {
            primaryFailure = failure;
            SqlExecutionSequenceException sequenceFailure = failure instanceof SqlExecutionSequenceException sequence
                    ? sequence : null;
            List<SqlExecutionStepResult> completed = sequenceFailure == null
                    ? state.steps : sequenceFailure.completedWorkSteps();
            if (sequenceFailure != null && sequenceFailure.phase() == SqlExecutionPhase.CLEANUP) {
                attempt = VerifiedSchemaPlanExecutor.cleanupFailedAttempt(steps, completed);
            } else {
                int failedIndex = sequenceFailure == null
                        ? Math.max(0, state.failedStepIndex) : sequenceFailure.stepIndex();
                boolean sqlSent = sequenceFailure == null || sequenceFailure.phase() == SqlExecutionPhase.WORK;
                attempt = VerifiedSchemaPlanExecutor.failedAttempt(steps, completed, failedIndex, sqlSent, failure);
            }
        } catch (Error failure) {
            primaryFailure = failure;
            throw failure;
        } finally {
            try {
                state.invalidateAfterExecution(tables, tableInvalidator, primaryFailure);
            } catch (RuntimeException failure) {
                invalidationFailure = failure;
            }
        }
        return invalidationFailure == null ? attempt
                : VerifiedSchemaPlanExecutor.cleanupFailedAttempt(steps, state.steps);
    }
    private long executeReviewedRequests(List<SqlRequest> requests,
                                         SchemaMigrationExecutionOptions options,
                                         JdbcSchemaExecutionState state,
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
                                JdbcSchemaExecutionState state) {
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

    private static SqlExecutionSequenceException sequenceFailure(JdbcSchemaExecutionState state,
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
                              JdbcSchemaExecutionState state) {
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
                             JdbcSchemaExecutionState state) {
        long rows = 0L;
        state.phase = SqlExecutionPhase.WORK;
        for (int index = 0; index < requests.size(); index++) {
            SqlRequest request = requests.get(index);
            long startedAt = System.nanoTime();
            try {
                long affectedRows = ddlRowsUpdated(executor.rowsUpdated(request, options));
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

    private static long ddlRowsUpdated(long rowsUpdated) {
        return Math.max(0L, rowsUpdated);
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
                                          JdbcSchemaExecutionState state) {
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
            JdbcSchemaExecutionState invalidationState = Objects.requireNonNull(
                    state, "schema invalidation state must not be null");
            if (!current.get().completion().register(
                    ignored -> Mono.fromRunnable(() -> invalidationState
                            .invalidateAtTransactionCompletion(tables, invalidator)))) {
                throw rejection(
                        SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                        planFingerprint,
                        "external JDBC DDL requires transaction completion notification for metadata consistency");
            }
            invalidationState.transactionCompletionRegistered = true;
        }
    }

    private void observe(JdbcSchemaExecutionState state,
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
                                                 .reduce(0L, JdbcSchemaExecutionSupport::addExact)
                                           : result.rowsUpdated();
        } catch (com.flying.orm.rdb.exception.RdbException overflow) {
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

}
