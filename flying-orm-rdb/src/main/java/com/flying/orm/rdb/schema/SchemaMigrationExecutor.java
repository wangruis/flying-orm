package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.reactive.ConnectionScopedReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 负责按顺序执行迁移 SQL，并统一处理观测和元数据缓存失效。
 *
 * <p>批准校验发生在第一条 SQL 之前；一旦真正开始执行，无论成功、失败还是取消都保守失效目标表缓存。
 * observer 不能覆盖数据库结果；缓存失效会尝试全部目标并报告失败，若 DDL 同时失败则作为 suppressed
 * 附着在数据库主失败上。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class SchemaMigrationExecutor {

    private final ReactiveSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final SchemaMigrationObserver observer;
    private final SchemaDdlTransactionSupport ddlTransactionSupport;

    SchemaMigrationExecutor(ReactiveSqlExecutor executor,
                            FormSchemaSqlRenderer renderer,
                            SchemaMigrationObserver observer,
                            SchemaDdlTransactionSupport ddlTransactionSupport) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema sql renderer must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
        this.ddlTransactionSupport = Objects.requireNonNull(
                ddlTransactionSupport, "DDL transaction support must not be null");
    }

    Mono<Long> execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        return guardExternalTransaction(requests, false)
                .then(Mono.defer(() -> executeRequests(requests, options)));
    }

    Mono<Long> execute(List<SqlRequest> requests) {
        return guardExternalTransaction(requests, false)
                .then(Mono.defer(() -> executeRequests(requests)));
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests, SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .concatMap(request -> executor.rowsUpdated(request, options))
                   .reduce(0L, SchemaMigrationExecutor::addExact);
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests) {
        return Flux.fromIterable(requests)
                   .concatMap(executor::rowsUpdated)
                   .reduce(0L, SchemaMigrationExecutor::addExact);
    }

    Mono<Long> executeWithInvalidation(List<SqlRequest> requests,
                                       List<String> tables,
                                       Consumer<String> metadataInvalidator,
                                       SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return Mono.just(0L);
        }
        return Mono.using(
                () -> new InvalidationScope(metadataInvalidator, tables),
                scope -> guardExternalTransaction(requests, false, scope)
                        .then(Mono.defer(() -> {
                            scope.executionStarted();
                            return executeRequests(requests, options);
                        }))
                        .doOnError(scope::executionFailed),
                InvalidationScope::publisherTerminated,
                true);
    }

    Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan plan,
                                                List<String> tables,
                                                Consumer<String> metadataInvalidator,
                                                SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema migration plan must not be null");
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return Mono.using(() -> new InvalidationScope(safeInvalidator, tables), scope -> {
            String planFingerprint = safePlan.fingerprint();
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            return executeReviewedPlan(
                    safePlan, safeOptions, scope, planFingerprint)
                    .doOnError(scope::executionFailed)
                    .doOnSuccess(result -> observe(observed, safePlan, planFingerprint, result, startedAt,
                                                   SqlExecutionStatus.SUCCESS, null))
                    .doOnError(error -> observe(observed, safePlan, planFingerprint, null, startedAt,
                                                SqlExecutionStatus.ERROR, error))
                    .doOnCancel(() -> observe(observed, safePlan, planFingerprint, null, startedAt,
                                              SqlExecutionStatus.CANCELLED, null));
        }, InvalidationScope::publisherTerminated, true);
    }

    private Mono<SchemaMigrationResult> executeReviewedPlan(ReviewedSchemaMigrationPlan plan,
                                                             SchemaMigrationExecutionOptions options,
                                                             InvalidationScope invalidationScope,
                                                             String planFingerprint) {
        List<SqlRequest> requests = plan.requestsForExecution(options.approval(), planFingerprint);
        if (requests.isEmpty()) {
            return Mono.just(new SchemaMigrationResult(plan.migration(), 0L, List.of()));
        }
        return guardExternalTransaction(
                requests,
                plan.onlineDdl().requiresNonTransactionalExecution(),
                invalidationScope,
                planFingerprint)
                .then(Mono.defer(() -> executeReviewedRequests(
                        plan, options, invalidationScope, requests, planFingerprint)));
    }

    private Mono<SchemaMigrationResult> executeReviewedRequests(ReviewedSchemaMigrationPlan plan,
                                                                  SchemaMigrationExecutionOptions options,
                                                                  InvalidationScope invalidationScope,
                                                                  List<SqlRequest> requests,
                                                                  String planFingerprint) {
        if (!options.hasLockTimeout()) {
            return executeRequests(requests, options.sqlExecutionOptions())
                    .doOnSubscribe(ignored -> invalidationScope.executionStarted())
                    .map(rows -> new SchemaMigrationResult(plan.migration(), rows, List.of()));
        }
        if (!(executor instanceof ConnectionScopedReactiveSqlExecutor scoped)) {
            return Mono.error(new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                    planFingerprint,
                    "DDL lock timeout requires a connection-scoped reactive SQL executor"));
        }
        SchemaDdlSessionGuard guard = renderer.lockTimeoutGuard(options.lockTimeout());
        SqlExecutionSequence sequence = new SqlExecutionSequence(guard.setup(), requests, guard.cleanup());
        return scoped.executeInConnection(sequence, options.sqlExecutionOptions())
                     .doOnSubscribe(ignored -> invalidationScope.executionStarted())
                     .map(result -> new SchemaMigrationResult(
                             plan.migration(), result.rowsUpdated(), result.workSteps()));
    }

    /**
     * 外部事务存在时，只允许已确认支持事务的普通 DDL。隐式提交、未知方言和计划明确标出的非事务语句
     * 都在第一条 SQL 前拒绝；没有外部事务时保持原来的执行行为。
     */
    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution) {
        return guardExternalTransaction(requests, requiresNonTransactionalExecution, null, null);
    }

    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution,
                                                InvalidationScope invalidationScope) {
        return guardExternalTransaction(
                requests, requiresNonTransactionalExecution, invalidationScope, null);
    }

    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution,
                                                InvalidationScope invalidationScope,
                                                String planFingerprint) {
        if (requests.isEmpty()) {
            return Mono.empty();
        }
        return executor.currentTransaction()
                       .flatMap(transaction -> guardExternalTransaction(
                               transaction,
                               requiresNonTransactionalExecution,
                               invalidationScope,
                               planFingerprint))
                       .then();
    }

    private Mono<Void> guardExternalTransaction(R2dbcTransactionContext transaction,
                                                boolean requiresNonTransactionalExecution,
                                                InvalidationScope invalidationScope,
                                                String planFingerprint) {
        if (!ddlTransactionSupport.allowsExternalTransaction() || requiresNonTransactionalExecution) {
            return Mono.error(rejection(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    planFingerprint,
                    "DDL cannot join the current external transaction: support=" + ddlTransactionSupport));
        }
        if (invalidationScope != null
                && !invalidationScope.registerTransactionCompletion(transaction)) {
            return Mono.error(rejection(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    planFingerprint,
                    "external DDL requires transaction completion notification for metadata consistency"));
        }
        return Mono.empty();
    }

    private static SchemaMigrationRejectedException rejection(SchemaMigrationFailureCode failureCode,
                                                               String planFingerprint,
                                                               String message) {
        return planFingerprint == null
                ? new SchemaMigrationRejectedException(failureCode, message)
                : new SchemaMigrationRejectedException(failureCode, planFingerprint, message);
    }

    private void observe(AtomicBoolean observed,
                         ReviewedSchemaMigrationPlan plan,
                         String planFingerprint,
                         SchemaMigrationResult result,
                         long startedAt,
                         SqlExecutionStatus status,
                         Throwable error) {
        if (!observed.compareAndSet(false, true)) {
            return;
        }
        if (result == null && isAffectedRowsOverflow(error)) {
            // 汇总失败后无法在 long 中精确表达已执行行数；不把它伪装为零行失败观测。
            return;
        }
        SqlExecutionSequenceException sequenceFailure = findSequenceFailure(error);
        int completed = result == null
                ? sequenceFailure == null ? 0 : sequenceFailure.completedWorkSteps().size()
                : result.steps().isEmpty() ? plan.migration().executableSqlCount() : result.steps().size();
        long rows;
        try {
            rows = result == null
                    ? sequenceFailure == null ? 0L : sequenceFailure.completedWorkSteps().stream()
                                                                      .mapToLong(step -> step.rowsUpdated())
                                                                      .reduce(0L, SchemaMigrationExecutor::addExact)
                    : result.rowsUpdated();
        } catch (RdbException overflow) {
            // 已经处于失败路径时不伪造截断后的观测值，也不能让旁路观测覆盖原始数据库异常。
            return;
        }
        SqlFailureCategory category = status == SqlExecutionStatus.SUCCESS
                ? SqlFailureCategory.NONE
                : status == SqlExecutionStatus.CANCELLED
                        ? SqlFailureCategory.CANCELLED : SqlFailureCategory.classify(error);
        observer.onMigration(new SchemaMigrationObservation(
                planFingerprint, plan.riskLevel(), status, plan.migration().executableSqlCount(), completed,
                rows, System.nanoTime() - startedAt, category,
                sequenceFailure == null ? null : sequenceFailure.phase(),
                sequenceFailure == null ? null : sequenceFailure.stepIndex(), error));
    }

    private static SqlExecutionSequenceException findSequenceFailure(Throwable error) {
        return ThrowableGraph.findCause(error, SqlExecutionSequenceException.class);
    }

    private static final class InvalidationScope {

        private final Consumer<String> metadataInvalidator;

        private final List<String> tables;

        private volatile boolean executionStarted;

        private volatile boolean transactionCompletionRegistered;

        private volatile Throwable executionFailure;

        private InvalidationScope(Consumer<String> metadataInvalidator, List<String> tables) {
            this.metadataInvalidator = metadataInvalidator;
            this.tables = tables;
        }

        private void executionStarted() {
            executionStarted = true;
        }

        private void executionFailed(Throwable failure) {
            executionFailure = failure;
        }

        private boolean registerTransactionCompletion(R2dbcTransactionContext transaction) {
            boolean registered = transaction.completion().register(
                    ignored -> Mono.fromRunnable(this::invalidate));
            if (registered) {
                transactionCompletionRegistered = true;
            }
            return registered;
        }

        private void publisherTerminated() {
            if (!transactionCompletionRegistered) {
                invalidatePreservingPrimary();
            }
        }

        private void invalidatePreservingPrimary() {
            try {
                invalidate();
            } catch (RuntimeException | Error invalidationFailure) {
                Throwable primary = executionFailure;
                if (primary == null) {
                    throw invalidationFailure;
                }
                if (primary != invalidationFailure) {
                    primary.addSuppressed(invalidationFailure);
                }
            }
        }

        private void invalidate() {
            if (!executionStarted) {
                return;
            }
            SchemaCacheInvalidationCoordinator.invalidateTables(metadataInvalidator, tables);
        }
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

    private static boolean isAffectedRowsOverflow(Throwable error) {
        return error instanceof RdbException rdb
                && "database execution count exceeds supported range".equals(rdb.getMessage())
                && rdb.getCause() instanceof ArithmeticException;
    }
}
