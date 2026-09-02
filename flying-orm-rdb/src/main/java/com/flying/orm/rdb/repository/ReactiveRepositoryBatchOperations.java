package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Repository 的批量操作编排。
 *
 * <p>这里负责把实体批量操作转换成对应的 {@link BatchSpec}，并明确每种操作该使用的生命周期阶段。
 * 真正的实体映射、主键准备、生命周期回调、有界缓存、事务参与和 SQL 执行仍由
 * {@link ReactiveRepositoryBatchCoordinator} 与 {@link ReactiveFormClient} 完成。这样 Repository 门面
 * 不必同时知道所有批量细节，也不会另起一套批量实现。</p>
 *
 * @param <T> 实体类型
 */
final class ReactiveRepositoryBatchOperations<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final ReactiveRepositoryBatchCoordinator<T> coordinator;

    ReactiveRepositoryBatchOperations(ReactiveFormClient client,
                                      DynamicForm form,
                                      ReactiveRepositoryBatchCoordinator<T> coordinator) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.coordinator = Objects.requireNonNull(coordinator, "repository batch coordinator must not be null");
    }

    Mono<BatchWriteResult> insert(List<T> entities) {
        return insert(Flux.fromIterable(List.copyOf(Objects.requireNonNull(
                entities, "repository entities must not be null"))));
    }

    Mono<BatchWriteResult> insert(Publisher<T> entities) {
        return insert(entities, client.defaultBatchWriteOptions());
    }

    Mono<BatchWriteResult> insert(Publisher<T> entities, BatchWriteOptions options) {
        return Mono.defer(() -> {
            coordinator.requireStableInsertLayout();
            return write(entities, coordinator::insertValues, EntityLifecyclePhase.PRE_PERSIST,
                         EntityLifecyclePhase.POST_PERSIST, options, true,
                         (rows, completion, generatedKeys) -> BatchSpec.insert(form, rows)
                                 .withOptions(options).withGeneratedKeys(generatedKeys).withCompletion(completion));
        });
    }

    Flux<BatchChunkResult> insertChunks(Publisher<T> entities, BatchWriteOptions options) {
        return Flux.defer(() -> {
            coordinator.requireStableInsertLayout();
            return chunks(entities, coordinator::insertValues, EntityLifecyclePhase.PRE_PERSIST,
                          EntityLifecyclePhase.POST_PERSIST, options, true,
                          (rows, completion, generatedKeys) -> BatchSpec.insert(form, rows)
                                  .withOptions(options).withGeneratedKeys(generatedKeys).withCompletion(completion));
        });
    }

    Mono<BatchWriteResult> upsert(List<T> entities) {
        return upsert(Flux.fromIterable(List.copyOf(Objects.requireNonNull(
                entities, "repository entities must not be null"))));
    }

    Mono<BatchWriteResult> upsert(Publisher<T> entities) {
        return upsert(entities, client.defaultBatchWriteOptions());
    }

    Mono<BatchWriteResult> upsert(Publisher<T> entities, BatchWriteOptions options) {
        return Mono.defer(() -> {
            coordinator.requireSupportedUpsertId();
            coordinator.requireStableUpsertLayout();
            return write(entities, coordinator::upsertValues, EntityLifecyclePhase.PRE_PERSIST,
                         EntityLifecyclePhase.POST_PERSIST, options, false,
                         (rows, completion, generatedKeys) -> BatchSpec.upsert(form, rows)
                                 .withOptions(options).withCompletion(completion));
        });
    }

    Flux<BatchChunkResult> upsertChunks(Publisher<T> entities, BatchWriteOptions options) {
        return Flux.defer(() -> {
            coordinator.requireSupportedUpsertId();
            coordinator.requireStableUpsertLayout();
            return chunks(entities, coordinator::upsertValues, EntityLifecyclePhase.PRE_PERSIST,
                          EntityLifecyclePhase.POST_PERSIST, options, false,
                          (rows, completion, generatedKeys) -> BatchSpec.upsert(form, rows)
                                  .withOptions(options).withCompletion(completion));
        });
    }

    Mono<BatchWriteResult> update(List<T> entities) {
        return update(Flux.fromIterable(List.copyOf(Objects.requireNonNull(
                entities, "repository entities must not be null"))));
    }

    Mono<BatchWriteResult> update(Publisher<T> entities) {
        return write(entities, coordinator::optimisticUpdate, EntityLifecyclePhase.PRE_UPDATE,
                     EntityLifecyclePhase.POST_UPDATE, client.defaultBatchWriteOptions(), false,
                     (rows, completion, generatedKeys) -> BatchSpec.update(form, rows).withCompletion(completion));
    }

    Mono<BatchWriteResult> update(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, coordinator::optimisticUpdate, EntityLifecyclePhase.PRE_UPDATE,
                     EntityLifecyclePhase.POST_UPDATE, options, false,
                     (rows, completion, generatedKeys) -> BatchSpec.update(form, rows)
                             .withOptions(options).withCompletion(completion));
    }

    Mono<BatchWriteResult> update(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return write(entities, coordinator::optimisticUpdate, EntityLifecyclePhase.PRE_UPDATE,
                     EntityLifecyclePhase.POST_UPDATE, options, false,
                     (rows, completion, generatedKeys) -> BatchSpec.update(form, rows).withScope(scope)
                                                        .withOptions(options).withCompletion(completion));
    }

    Flux<BatchChunkResult> updateChunks(Publisher<T> entities, BatchWriteOptions options) {
        return chunks(entities, coordinator::optimisticUpdate, EntityLifecyclePhase.PRE_UPDATE,
                      EntityLifecyclePhase.POST_UPDATE, options, false,
                      (rows, completion, generatedKeys) -> BatchSpec.update(form, rows)
                              .withOptions(options).withCompletion(completion));
    }

    Flux<BatchChunkResult> updateChunks(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return chunks(entities, coordinator::optimisticUpdate, EntityLifecyclePhase.PRE_UPDATE,
                      EntityLifecyclePhase.POST_UPDATE, options, false,
                      (rows, completion, generatedKeys) -> BatchSpec.update(form, rows).withScope(scope)
                              .withOptions(options).withCompletion(completion));
    }

    private <R> Mono<BatchWriteResult> write(Publisher<T> entities,
                                              Function<T, R> mapper,
                                              EntityLifecyclePhase before,
                                              EntityLifecyclePhase after,
                                              BatchWriteOptions options,
                                              boolean generatedKeyInsert,
                                              BatchSpecFactory<R> spec) {
        return coordinator.write(entities, mapper, before, after, options, generatedKeyInsert,
                (rows, completion, generatedKeys) -> client.writeBatch(
                        spec.apply(rows, completion, generatedKeys)));
    }

    private <R> Flux<BatchChunkResult> chunks(Publisher<T> entities,
                                               Function<T, R> mapper,
                                               EntityLifecyclePhase before,
                                               EntityLifecyclePhase after,
                                               BatchWriteOptions options,
                                               boolean generatedKeyInsert,
                                               BatchSpecFactory<R> spec) {
        return coordinator.chunks(entities, mapper, before, after, options, generatedKeyInsert,
                (rows, completion, generatedKeys) -> client.writeBatchChunks(
                        spec.apply(rows, completion, generatedKeys)));
    }

    @FunctionalInterface
    private interface BatchSpecFactory<R> {
        BatchSpec apply(Publisher<R> rows,
                        BatchWriteCompletion completion,
                        BatchGeneratedKeys generatedKeys);
    }

}
