package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * 负责 Repository 批量写入的协调，不负责渲染 SQL 或管理事务。
 *
 * <p>实体先经过生命周期 tracker，再交给 ReactiveFormClient 的批量入口。tracker 继续负责实体引用、
 * 参数对象和生命周期回调的有界保留；ATOMIC 等待最终汇总后才发 after，INDEPENDENT 按已提交分片发 after。
 * 取消、回滚和 UNKNOWN 的判断仍由表单客户端和底层批量执行器完成。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryBatchCoordinator<T> {

    private final EntityMetadata<T> metadata;
    private final Function<T, Map<String, Object>> values;
    private final Function<T, Map<String, Object>> insertValues;
    private final Function<T, Map<String, Object>> upsertValues;
    private final Function<T, Map<String, Object>> updateValues;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryBatchCoordinator(EntityMetadata<T> metadata,
                                       Function<T, Map<String, Object>> values,
                                       Function<T, Map<String, Object>> insertValues,
                                       Function<T, Map<String, Object>> upsertValues,
                                       Function<T, Map<String, Object>> updateValues,
                                       ReactiveRepositoryLifecycleSupport<T> lifecycle,
                                       RepositoryEntityIdSupport<T> ids) {
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
        this.insertValues = Objects.requireNonNull(insertValues, "repository insert values must not be null");
        this.upsertValues = Objects.requireNonNull(upsertValues, "repository upsert values must not be null");
        this.updateValues = Objects.requireNonNull(updateValues, "repository update values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
    }

    /**
     * 批量 insert 的每一行都要先走和单条 insert 一样的主键规则，再生成参数 Map。
     *
     * <p>这个方法由上游批量 Publisher 按需求调用，不会提前遍历或缓存实体。主键校验或生成失败时，
     * 当前行不会进入 SQL 执行器；上游会收到错误并取消后续输入。</p>
     */
    Map<String, Object> insertValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        ids.prepare(safeEntity);
        return insertValues.apply(safeEntity);
    }

    /** upsert 的 insert 部分也必须在 SQL 前执行同一套主键规则。 */
    Map<String, Object> upsertValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        ids.prepare(safeEntity);
        return upsertValues.apply(safeEntity);
    }

    void requireStableInsertLayout() {
        RepositoryBatchLayoutPolicy.requireStableInsertLayout(metadata);
    }

    void requireStableUpsertLayout() {
        RepositoryBatchLayoutPolicy.requireStableUpsertLayout(metadata);
    }

    /**
     * 数据库生成主键必须为空并由 insert 返回，无法同时作为 upsert 的冲突目标。必须在订阅输入和 SQL 前拒绝，
     * 调用方可改用 ASSIGN_ID、ASSIGN_UUID 或 INPUT 主键表达明确的 upsert 身份。
     */
    void requireSupportedUpsertId() {
        if (ids.databaseGenerated()) {
            throw new MappingException(
                    "repository batch upsert does not support AUTO or sequence-generated primary keys");
        }
    }

    BatchOptimisticUpdate optimisticUpdate(T entity) {
        return RepositoryOptimisticLocks.batchUpdate(metadata, values.apply(entity), updateValues.apply(entity));
    }

    <R> Mono<BatchWriteResult> write(Publisher<T> entities,
                                     Function<T, R> mapper,
                                     EntityLifecyclePhase before,
                                     EntityLifecyclePhase after,
                                     BatchWriteOptions options,
                                     boolean generatedKeyInsert,
                                     BatchWriter<R> writer) {
        return Mono.defer(() -> {
            BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
            boolean returnGeneratedKeys = generatedKeyInsert && ids.databaseGenerated();
            BatchLifecycleTracker<T> tracker = tracker(after, safeOptions, returnGeneratedKeys);
            Publisher<R> rows = tracker.rows(entities, mapper, before);
            AtomicBoolean awaitingExternalCompletion = new AtomicBoolean();
            BatchWriteCompletion completion = result -> tracker.finish(result, after);
            BatchGeneratedKeys generatedKeys = tracker.generatedKeys();
            return writer.apply(rows, completion, generatedKeys)
                    .flatMap(result -> {
                        if (result.status() == BatchWriteResult.Status.ENLISTED) {
                            // 完成回调已经交给外部事务管理器，当前调用不能提前释放实体或执行 after。
                            awaitingExternalCompletion.set(true);
                            return Mono.just(result);
                        }
                        return tracker.finish(result, after).thenReturn(result);
                    })
                    .onErrorMap(error -> awaitingExternalCompletion.get()
                            ? error
                            : RepositoryFailureSupport.afterCleanup(error, tracker::abort))
                    .doOnCancel(() -> {
                        if (!awaitingExternalCompletion.get()) {
                            RepositoryFailureSupport.cleanupAfterCancellation(tracker::abort);
                        }
                    });
        });
    }

    <R> Flux<BatchChunkResult> chunks(Publisher<T> entities,
                                     Function<T, R> mapper,
                                     EntityLifecyclePhase before,
                                     EntityLifecyclePhase after,
                                     BatchWriteOptions options,
                                     boolean generatedKeyInsert,
                                     ChunkWriter<R> writer) {
        return Flux.defer(() -> {
            BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
            boolean returnGeneratedKeys = generatedKeyInsert && ids.databaseGenerated();
            BatchLifecycleTracker<T> tracker = tracker(after, safeOptions, returnGeneratedKeys);
            Publisher<R> rows = tracker.rows(entities, mapper, before);
            Flux<BatchChunkResult> chunks = writer.apply(rows, tracker.generatedKeys());
            if (safeOptions.mode() == BatchWriteOptions.Mode.ATOMIC) {
                // ATOMIC 的中间 chunk 可能最终随整批回滚，必须等汇总状态后再触发 after。
                return chunks.collectList()
                        .flatMapMany(results -> tracker.finish(
                                BatchWriteResult.from(safeOptions.mode(), results), after)
                                .thenMany(Flux.fromIterable(results)))
                        .onErrorMap(error -> RepositoryFailureSupport.afterCleanup(error, tracker::abort))
                        .doOnCancel(() -> RepositoryFailureSupport.cleanupAfterCancellation(tracker::abort));
            }
            return chunks.concatMap(chunk -> tracker.finish(chunk, after).thenReturn(chunk))
                    .onErrorMap(error -> RepositoryFailureSupport.afterCleanup(error, tracker::abort))
                    .doOnCancel(() -> RepositoryFailureSupport.cleanupAfterCancellation(tracker::abort));
        });
    }

    private BatchLifecycleTracker<T> tracker(EntityLifecyclePhase after,
                                             BatchWriteOptions options,
                                             boolean retainForGeneratedKeys) {
        return new BatchLifecycleTracker<>(lifecycle.dispatcher(), after, ids, retainForGeneratedKeys,
                                           options.maxBufferedBytes(),
                                           this::retainedEntityBytes);
    }

    private long retainedEntityBytes(T entity) {
        Map<String, Object> snapshot = Objects.requireNonNull(values.apply(entity),
                                                               "repository entity values must not be null");
        long valuesBytes = BatchMemoryBudget.estimateRowBytes(snapshot.values().toArray());
        return valuesBytes > Long.MAX_VALUE - 64L ? Long.MAX_VALUE : valuesBytes + 64L;
    }

    @FunctionalInterface
    interface BatchWriter<R> {
        Mono<BatchWriteResult> apply(Publisher<R> rows,
                                     BatchWriteCompletion completion,
                                     BatchGeneratedKeys generatedKeys);
    }

    @FunctionalInterface
    interface ChunkWriter<R> {
        Flux<BatchChunkResult> apply(Publisher<R> rows, BatchGeneratedKeys generatedKeys);
    }
}
