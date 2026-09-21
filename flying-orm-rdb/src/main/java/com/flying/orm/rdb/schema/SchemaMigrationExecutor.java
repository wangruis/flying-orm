package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
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
    private final SchemaMigrationObserver observer;

    SchemaMigrationExecutor(ReactiveSqlExecutor executor,
                            SchemaMigrationObserver observer) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
    }

    Mono<Long> execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        return MySqlSchemaCommentSupport.execute(executor, requests, options, null,
                bound -> executeRequests(bound, requests, options));
    }

    Mono<Long> execute(List<SqlRequest> requests) {
        return MySqlSchemaCommentSupport.execute(executor, requests, null, null,
                bound -> executeRequests(bound, requests));
    }

    private Mono<Long> executeRequests(ReactiveSqlExecutor bound, List<SqlRequest> requests, SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .concatMap(request -> bound.rowsUpdated(request, options))
                   .map(SchemaMigrationExecutor::ddlRowsUpdated)
                   .reduce(0L, ReactiveSchemaMigrationObservation::addExact);
    }

    private Mono<Long> executeRequests(ReactiveSqlExecutor bound, List<SqlRequest> requests) {
        return Flux.fromIterable(requests)
                   .concatMap(bound::rowsUpdated)
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
                scope -> MySqlSchemaCommentSupport.execute(executor, requests, options, null, bound -> {
                            scope.executionStarted();
                            return executeRequests(bound, requests, options);
                        })
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
            List<SqlExecutionStepResult> completed = Collections.synchronizedList(new ArrayList<>());
            return executeReviewedPlan(
                    safePlan, safeOptions, scope, planFingerprint, completed)
                    .doOnError(scope::executionFailed)
                    .doOnSuccess(result -> observe(observer, observed, safePlan, planFingerprint,
                                                   result, completed, startedAt, SqlExecutionStatus.SUCCESS, null))
                    .doOnError(error -> observe(observer, observed, safePlan, planFingerprint,
                                                null, completed, startedAt, SqlExecutionStatus.ERROR, error))
                    .doOnCancel(() -> observe(observer, observed, safePlan, planFingerprint,
                                              null, completed, startedAt, SqlExecutionStatus.CANCELLED, null));
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
        List<String> tables = List.of(plan.targetIdentity().orElseThrow().table());
        Consumer<String> tableInvalidator = ignored -> invalidator.run();
        return Mono.defer(() -> {
            List<SqlExecutionStepResult> completed = new ArrayList<>(requests.size());
            return Mono.using(
                    () -> new ReactiveSchemaInvalidationScope(tableInvalidator, tables),
                    scope -> MySqlSchemaCommentSupport.execute(
                                    executor, requests, options.sqlExecutionOptions(), plan.fingerprint(),
                                    bound -> executeVerifiedRequests(bound, steps, requests, options, scope, completed)),
                    ReactiveSchemaInvalidationScope::publisherTerminated,
                    true)
                    .onErrorResume(RuntimeException.class, failure -> completed.size() == steps.size()
                            ? Mono.just(VerifiedSchemaPlanExecutor.cleanupFailedAttempt(steps, completed))
                            : Mono.error(failure));
        });
    }

    private Mono<VerifiedSchemaPlanExecutor.ExecutionAttempt> executeVerifiedRequests(
            ReactiveSqlExecutor bound, List<SchemaPlanStep> steps,
            List<SqlRequest> requests,
            SchemaMigrationExecutionOptions options,
            ReactiveSchemaInvalidationScope invalidationScope,
            List<SqlExecutionStepResult> completed) {
        return executeRequestSequence(bound, requests, options.sqlExecutionOptions(), completed)
                .doOnSubscribe(ignored -> invalidationScope.executionStarted())
                .map(result -> VerifiedSchemaPlanExecutor.successfulAttempt(steps, result.workSteps()))
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
            ReactiveSqlExecutor bound, List<SqlRequest> requests,
            SqlExecutionOptions options,
            List<SqlExecutionStepResult> completed) {
        return Flux.range(0, requests.size())
                .concatMap(index -> Mono.defer(() -> {
                    SqlRequest request = requests.get(index);
                    long startedAt = System.nanoTime();
                    return bound.rowsUpdated(request, options)
                            .map(rows -> new SqlExecutionStepResult(
                                    index, request, ddlRowsUpdated(rows),
                                    System.nanoTime() - startedAt))
                            .switchIfEmpty(Mono.error(() -> new IllegalStateException(
                                    "reactive SQL executor returned no result")));
                }).doOnNext(completed::add), 1)
                .then(Mono.fromSupplier(() -> new SqlExecutionSequenceResult(completed)));
    }

    private Mono<SchemaMigrationResult> executeReviewedPlan(ReviewedSchemaMigrationPlan plan,
                                                             SchemaMigrationExecutionOptions options,
                                                             ReactiveSchemaInvalidationScope invalidationScope,
                                                             String planFingerprint,
                                                             List<SqlExecutionStepResult> completed) {
        List<SqlRequest> requests = plan.requestsForExecution(options.approval(), planFingerprint);
        if (requests.isEmpty()) {
            return Mono.just(new SchemaMigrationResult(plan.migration(), 0L, List.of()));
        }
        return MySqlSchemaCommentSupport.execute(
                        executor, requests, options.sqlExecutionOptions(), planFingerprint,
                        bound -> executeReviewedRequests(bound, plan, options, invalidationScope, requests, completed));
    }

    private Mono<SchemaMigrationResult> executeReviewedRequests(ReactiveSqlExecutor bound, ReviewedSchemaMigrationPlan plan,
                                                                  SchemaMigrationExecutionOptions options,
                                                                  ReactiveSchemaInvalidationScope invalidationScope,
                                                                  List<SqlRequest> requests,
                                                                  List<SqlExecutionStepResult> completed) {
        return executeRequestSequence(bound, requests, options.sqlExecutionOptions(), completed)
                .doOnSubscribe(ignored -> invalidationScope.executionStarted())
                .map(result -> new SchemaMigrationResult(
                        plan.migration(),
                        result.workSteps().stream().mapToLong(SqlExecutionStepResult::rowsUpdated)
                              .reduce(0L, ReactiveSchemaMigrationObservation::addExact),
                        result.workSteps()));
    }

    private static long ddlRowsUpdated(long rowsUpdated) {
        return Math.max(0L, rowsUpdated);
    }
}
