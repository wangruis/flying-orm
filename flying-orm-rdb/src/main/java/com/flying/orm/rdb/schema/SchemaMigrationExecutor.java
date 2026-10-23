package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
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
import java.util.function.Function;

/**
 * 负责按顺序执行迁移 SQL，并统一处理观测和元数据缓存失效。
 *
 * <p>一旦开始执行，无论成功、失败还是取消都失效目标表缓存。
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
        return MySqlSchemaCommentSupport.execute(executor, requests, options,
                bound -> executeRequests(bound, requests, options));
    }

    Mono<Long> execute(List<SqlRequest> requests) {
        return execute(requests, null);
    }

    private Mono<Long> executeRequests(ReactiveSqlExecutor bound, List<SqlRequest> requests, SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .concatMap(request -> updatedRows(bound, request, options))
                   .reduce(0L, SchemaMigrationResult::addRows);
    }

    private static Mono<Long> updatedRows(ReactiveSqlExecutor bound, SqlRequest request,
                                          SqlExecutionOptions options) {
        Mono<Long> result = options == null ? bound.rowsUpdated(request) : bound.rowsUpdated(request, options);
        return result
                .map(rows -> Math.max(0L, rows))
                .switchIfEmpty(Mono.error(() -> new IllegalStateException(
                        "reactive SQL executor returned no result")));
    }

    <T> Mono<Long> executeWithInvalidation(List<SqlRequest> requests,
                                       List<T> tables,
                                       Consumer<T> metadataInvalidator,
                                       SqlExecutionOptions options) {
        return executeWithInvalidation(requests, tables, metadataInvalidator, options,
                bound -> executeRequests(bound, requests, options));
    }

    private <T> Mono<Long> executeWithInvalidation(List<SqlRequest> requests, List<T> tables,
            Consumer<T> metadataInvalidator, SqlExecutionOptions options,
            Function<ReactiveSqlExecutor, Mono<Long>> work) {
        if (requests.isEmpty()) {
            return Mono.just(0L);
        }
        return Mono.using(
                () -> new ReactiveSchemaInvalidationScope<>(metadataInvalidator, tables),
                scope -> MySqlSchemaCommentSupport.execute(executor, requests, options, bound -> {
                            scope.executionStarted();
                            return work.apply(bound);
                        })
                        .doOnError(scope::executionFailed),
                ReactiveSchemaInvalidationScope::publisherTerminated,
                true);
    }

    <T> Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan,
                                                List<T> tables,
                                                Consumer<T> metadataInvalidator,
                                                SqlExecutionOptions options) {
        SchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "schema migration plan must not be null");
        Consumer<T> safeInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            List<SqlExecutionStepResult> completed = Collections.synchronizedList(new ArrayList<>());
            return Mono.defer(() -> executeSequence(safePlan.requestsForExecution(), tables,
                            safeInvalidator, safeOptions, completed))
                    .map(rows -> new SchemaMigrationResult(safePlan, rows, completed))
                    .doOnSuccess(result -> observe(observed, safePlan,
                                                   result, completed, startedAt, SqlExecutionStatus.SUCCESS, null))
                    .doOnError(error -> observe(observed, safePlan,
                                                null, completed, startedAt, SqlExecutionStatus.ERROR, error))
                    .doOnCancel(() -> observe(observed, safePlan,
                                              null, completed, startedAt, SqlExecutionStatus.CANCELLED, null));
        });
    }


    private <T> Mono<Long> executeSequence(
            List<SqlRequest> requests, List<T> tables, Consumer<T> metadataInvalidator,
            SqlExecutionOptions options, List<SqlExecutionStepResult> completed) {
        return executeWithInvalidation(requests, tables, metadataInvalidator, options,
                    bound -> executeRequestSequence(bound, requests, options, completed))
                    .onErrorMap(RuntimeException.class, failure ->
                            !(failure instanceof SqlExecutionSequenceException) && completed.size() == requests.size()
                                    ? new SqlExecutionSequenceException(
                                            SqlExecutionPhase.CLEANUP,
                                            completed.size(), completed, failure)
                                    : failure);
    }

    private Mono<Long> executeRequestSequence(
            ReactiveSqlExecutor bound, List<SqlRequest> requests,
            SqlExecutionOptions options,
            List<SqlExecutionStepResult> completed) {
        return Flux.range(0, requests.size())
                .concatMap(index -> Mono.defer(() -> {
                    SqlRequest request = requests.get(index);
                    long startedAt = System.nanoTime();
                    return updatedRows(bound, request, options)
                            .map(rows -> new SqlExecutionStepResult(
                                    index, request, rows, System.nanoTime() - startedAt));
                }).onErrorMap(RuntimeException.class, failure -> new SqlExecutionSequenceException(
                        SqlExecutionPhase.WORK, index, completed, failure))
                  .doOnNext(completed::add), 1)
                .then(Mono.fromSupplier(() -> completed.stream().mapToLong(SqlExecutionStepResult::rowsUpdated)
                        .reduce(0L, SchemaMigrationResult::addRows)));
    }

    private void observe(AtomicBoolean observed, SchemaMigrationPlan plan,
                         SchemaMigrationResult result, List<SqlExecutionStepResult> completed,
                         long startedAt, SqlExecutionStatus status, Throwable error) {
        if (observed.compareAndSet(false, true)) {
            SchemaMigrationObservers.observe(observer, plan, result, completed, startedAt, status, error);
        }
    }
}
