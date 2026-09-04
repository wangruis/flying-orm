package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.reactive.ConnectionScopedReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.flying.orm.rdb.schema.ReactiveSchemaMigrationObservation.findSequenceFailure;
import static com.flying.orm.rdb.schema.ReactiveSchemaMigrationObservation.observe;

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
                .then(MySqlSchemaCommentSupport.validate(executor, requests, options, null))
                .then(Mono.defer(() -> executeRequests(requests, options)));
    }

    Mono<Long> execute(List<SqlRequest> requests) {
        return guardExternalTransaction(requests, false)
                .then(MySqlSchemaCommentSupport.validate(executor, requests, null, null))
                .then(Mono.defer(() -> executeRequests(requests)));
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests, SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .concatMap(request -> executor.rowsUpdated(request, options))
                   .map(SchemaMigrationExecutor::ddlRowsUpdated)
                   .reduce(0L, ReactiveSchemaMigrationObservation::addExact);
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests) {
        return Flux.fromIterable(requests)
                   .concatMap(executor::rowsUpdated)
                   .map(SchemaMigrationExecutor::ddlRowsUpdated)
                   .reduce(0L, ReactiveSchemaMigrationObservation::addExact);
    }

    Mono<Long> executeWithInvalidation(List<SqlRequest> requests,
                                       List<String> tables,
                                       Consumer<String> metadataInvalidator,
                                       SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return Mono.just(0L);
        }
        return Mono.using(
                () -> new ReactiveSchemaInvalidationScope(metadataInvalidator, tables),
                scope -> guardExternalTransaction(requests, false, scope)
                        .then(MySqlSchemaCommentSupport.validate(executor, requests, options, null))
                        .then(Mono.defer(() -> {
                            scope.executionStarted();
                            return executeRequests(requests, options);
                        }))
                        .doOnError(scope::executionFailed),
                ReactiveSchemaInvalidationScope::publisherTerminated,
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
        return Mono.using(() -> new ReactiveSchemaInvalidationScope(safeInvalidator, tables), scope -> {
            String planFingerprint = safePlan.fingerprint();
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            return executeReviewedPlan(
                    safePlan, safeOptions, scope, planFingerprint)
                    .doOnError(scope::executionFailed)
                    .doOnSuccess(result -> observe(observer, observed, safePlan, planFingerprint,
                                                   result, startedAt, SqlExecutionStatus.SUCCESS, null))
                    .doOnError(error -> observe(observer, observed, safePlan, planFingerprint,
                                                null, startedAt, SqlExecutionStatus.ERROR, error))
                    .doOnCancel(() -> observe(observer, observed, safePlan, planFingerprint,
                                              null, startedAt, SqlExecutionStatus.CANCELLED, null));
        }, ReactiveSchemaInvalidationScope::publisherTerminated, true);
    }

    Mono<SchemaExecutionReport> executeReviewed(ReviewedSchemaPlan plan,
                                                Supplier<Mono<SchemaSnapshot>> snapshotReader,
                                                Supplier<SchemaSnapshotCoverage> coverageReader,
                                                Runnable metadataInvalidator,
                                                SchemaMigrationExecutionOptions options) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema plan must not be null");
        Runnable safeInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        return VerifiedSchemaPlanExecutor.executeReactiveGuarded(
                safePlan,
                snapshotReader,
                coverageReader,
                safeInvalidator,
                safeOptions,
                steps -> executeVerifiedSteps(
                        safePlan, steps, safeInvalidator, safeOptions));
    }

    private Mono<VerifiedSchemaPlanExecutor.ExecutionAttempt> executeVerifiedSteps(
            ReviewedSchemaPlan plan,
            List<SchemaPlanStep> steps,
            Runnable invalidator,
            SchemaMigrationExecutionOptions options) {
        if (steps.isEmpty()) {
            return Mono.just(VerifiedSchemaPlanExecutor.successfulAttempt(steps, List.of()));
        }
        List<SqlRequest> requests = steps.stream()
                .map(step -> step.request().orElseThrow(() -> new IllegalStateException(
                        "reviewed schema execution contains a non-executable step")))
                .toList();
        List<String> tables = List.of(plan.desiredTable().orElseThrow().identity().table());
        Consumer<String> tableInvalidator = ignored -> invalidator.run();
        return Mono.defer(() -> {
            List<SqlExecutionStepResult> completed = new ArrayList<>(requests.size());
            return Mono.using(
                    () -> new ReactiveSchemaInvalidationScope(tableInvalidator, tables),
                    scope -> guardExternalTransaction(requests, false, scope, plan.fingerprint())
                            .then(MySqlSchemaCommentSupport.validate(
                                    executor, requests, options.sqlExecutionOptions(), plan.fingerprint()))
                            .then(Mono.defer(() -> executeVerifiedRequests(
                                    steps, requests, options, scope, plan.fingerprint(), completed))),
                    ReactiveSchemaInvalidationScope::publisherTerminated,
                    true)
                    .onErrorResume(RuntimeException.class, failure -> completed.size() == steps.size()
                            ? Mono.just(VerifiedSchemaPlanExecutor.cleanupFailedAttempt(steps, completed))
                            : Mono.error(failure));
        });
    }

    private Mono<VerifiedSchemaPlanExecutor.ExecutionAttempt> executeVerifiedRequests(
            List<SchemaPlanStep> steps,
            List<SqlRequest> requests,
            SchemaMigrationExecutionOptions options,
            ReactiveSchemaInvalidationScope invalidationScope,
            String planFingerprint,
            List<SqlExecutionStepResult> completed) {
        Mono<SqlExecutionSequenceResult> execution;
        if (!options.hasLockTimeout()) {
            execution = executeRequestSequence(requests, options.sqlExecutionOptions(), completed);
        } else if (executor instanceof ConnectionScopedReactiveSqlExecutor scoped) {
            SchemaDdlSessionGuard guard = renderer.lockTimeoutGuard(options.lockTimeout());
            execution = scoped.executeInConnection(
                    new SqlExecutionSequence(guard.setup(), requests, guard.cleanup()),
                    options.sqlExecutionOptions());
        } else {
            return Mono.error(new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                    planFingerprint,
                    "DDL lock timeout requires a connection-scoped reactive SQL executor"));
        }
        return execution
                .doOnSubscribe(ignored -> invalidationScope.executionStarted())
                .doOnNext(result -> {
                    if (completed.isEmpty()) {
                        completed.addAll(result.workSteps());
                    }
                })
                .map(result -> invalidationScope.externalTransactionRegistered()
                        ? VerifiedSchemaPlanExecutor.externalTransactionAttempt(
                                steps, result.workSteps())
                        : VerifiedSchemaPlanExecutor.successfulAttempt(
                                steps, result.workSteps()))
                .switchIfEmpty(Mono.defer(() -> {
                    IllegalStateException failure = new IllegalStateException(
                            "reactive SQL executor returned no result");
                    invalidationScope.executionFailed(failure);
                    return Mono.just(VerifiedSchemaPlanExecutor.failedAttempt(
                            steps, completed, completed.size(), true, failure));
                }))
                .doOnError(invalidationScope::executionFailed)
                .onErrorResume(RuntimeException.class, failure -> Mono.just(
                        failedVerifiedAttempt(steps, completed, failure)));
    }

    private static VerifiedSchemaPlanExecutor.ExecutionAttempt failedVerifiedAttempt(
            List<SchemaPlanStep> steps,
            List<SqlExecutionStepResult> completed,
            RuntimeException failure) {
        SqlExecutionSequenceException sequence = findSequenceFailure(failure);
        if (sequence != null) {
            completed.clear();
            completed.addAll(sequence.completedWorkSteps());
            if (sequence.phase() == com.flying.orm.rdb.execution.SqlExecutionPhase.CLEANUP) {
                return VerifiedSchemaPlanExecutor.cleanupFailedAttempt(steps, completed);
            }
            return VerifiedSchemaPlanExecutor.failedAttempt(
                    steps, completed, sequence.stepIndex(),
                    sequence.phase() == com.flying.orm.rdb.execution.SqlExecutionPhase.WORK,
                    failure);
        }
        return VerifiedSchemaPlanExecutor.failedAttempt(
                steps, completed, completed.size(), true, failure);
    }

    private Mono<SqlExecutionSequenceResult> executeRequestSequence(
            List<SqlRequest> requests,
            SqlExecutionOptions options,
            List<SqlExecutionStepResult> completed) {
        return executeRequestSequence(requests, options, completed, 0);
    }

    private Mono<SqlExecutionSequenceResult> executeRequestSequence(
            List<SqlRequest> requests,
            SqlExecutionOptions options,
            List<SqlExecutionStepResult> completed,
            int index) {
        if (index == requests.size()) {
            return Mono.just(new SqlExecutionSequenceResult(completed));
        }
        SqlRequest request = requests.get(index);
        return Mono.defer(() -> {
                    long startedAt = System.nanoTime();
                    return executor.rowsUpdated(request, options)
                            .map(rows -> new SqlExecutionStepResult(
                                    index, request, ddlRowsUpdated(rows),
                                    System.nanoTime() - startedAt));
                })
                .flatMap(step -> {
                    completed.add(step);
                    return executeRequestSequence(requests, options, completed, index + 1);
                });
    }

    private Mono<SchemaMigrationResult> executeReviewedPlan(ReviewedSchemaMigrationPlan plan,
                                                             SchemaMigrationExecutionOptions options,
                                                             ReactiveSchemaInvalidationScope invalidationScope,
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
                .then(MySqlSchemaCommentSupport.validate(
                        executor, requests, options.sqlExecutionOptions(), planFingerprint))
                .then(Mono.defer(() -> executeReviewedRequests(
                        plan, options, invalidationScope, requests, planFingerprint)));
    }

    private Mono<SchemaMigrationResult> executeReviewedRequests(ReviewedSchemaMigrationPlan plan,
                                                                  SchemaMigrationExecutionOptions options,
                                                                  ReactiveSchemaInvalidationScope invalidationScope,
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
                                                ReactiveSchemaInvalidationScope invalidationScope) {
        return guardExternalTransaction(
                requests, requiresNonTransactionalExecution, invalidationScope, null);
    }

    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution,
                                                ReactiveSchemaInvalidationScope invalidationScope,
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
                                                ReactiveSchemaInvalidationScope invalidationScope,
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

    private static long ddlRowsUpdated(long rowsUpdated) {
        return Math.max(0L, rowsUpdated);
    }
}
