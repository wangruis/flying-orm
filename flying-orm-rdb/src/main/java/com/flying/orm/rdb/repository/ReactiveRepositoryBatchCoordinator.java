package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final EntityValues<T> entityValues;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryBatchCoordinator(EntityMetadata<T> metadata,
                                       EntityValues<T> entityValues,
                                       ReactiveRepositoryLifecycleSupport<T> lifecycle,
                                       RepositoryEntityIdSupport<T> ids) {
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
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
        return entityValues.readForInsert(safeEntity);
    }

    /** upsert 的 insert 部分也必须在 SQL 前执行同一套主键规则。 */
    Map<String, Object> upsertValues(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        ids.prepare(safeEntity);
        return entityValues.repositoryUpsertValues(safeEntity);
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
        return RepositoryOptimisticLocks.batchUpdate(
                metadata, entityValues.read(entity), entityValues.readForUpdate(entity));
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
            RepositoryBatchLifecyclePlan plan = lifecyclePlan(after, returnGeneratedKeys);
            if (!plan.tracked()) {
                return writer.apply(directRows(entities, mapper),
                                    BatchWriteCompletion.noop(), BatchGeneratedKeys.none());
            }
            BatchLifecycleTracker<T> tracker = tracker(after, safeOptions, returnGeneratedKeys);
            Publisher<R> rows = tracker.rows(entities, mapper, before);
            AtomicBoolean awaitingExternalCompletion = new AtomicBoolean();
            BatchWriteCompletion completion = tracker.completion(after, true);
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
                    .onErrorResume(error -> finishFailure(
                            error, awaitingExternalCompletion.get(), tracker, after))
                    .doOnCancel(() -> {
                        if (!awaitingExternalCompletion.get()) {
                            RepositoryFailureSupport.cleanupAfterCancellation(tracker::abort);
                        }
                    });
        });
    }

    <R> Mono<BatchExecutionEvidence> evidence(Publisher<T> entities,
                                              Function<T, R> mapper,
                                              EntityLifecyclePhase after,
                                              BatchWriteOptions options,
                                              boolean generatedKeyInsert,
                                              EvidenceWriter<R> writer) {
        return Mono.defer(() -> {
            BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
            boolean returnGeneratedKeys = generatedKeyInsert && ids.databaseGenerated();
            if (lifecyclePlan(after, returnGeneratedKeys).tracked()) {
                return Mono.error(new UnsupportedOperationException(
                        "repository batch evidence cannot complete entity lifecycle or generated-key assignment"));
            }
            return writer.apply(directRows(entities, mapper),
                                BatchWriteCompletion.noop(), BatchGeneratedKeys.none());
        });
    }

    private Mono<BatchWriteResult> finishFailure(Throwable error,
                                                 boolean awaitingExternalCompletion,
                                                 BatchLifecycleTracker<T> tracker,
                                                 EntityLifecyclePhase after) {
        if (awaitingExternalCompletion) {
            return Mono.error(error);
        }
        if (error instanceof BatchWriteException batchFailure
                && batchFailure.result().mode() == BatchWriteOptions.Mode.INDEPENDENT) {
            return tracker.finishFailure(batchFailure.result(), after)
                    .onErrorMap(cleanupFailure -> RepositoryFailureSupport.merge(error, cleanupFailure))
                    .then(Mono.error(error));
        }
        return Mono.error(RepositoryFailureSupport.afterCleanup(error, tracker::abort));
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
            RepositoryBatchLifecyclePlan plan = lifecyclePlan(after, returnGeneratedKeys);
            if (!plan.tracked()) {
                return writer.apply(directRows(entities, mapper),
                                    BatchWriteCompletion.noop(), BatchGeneratedKeys.none());
            }
            BatchLifecycleTracker<T> tracker = tracker(after, safeOptions, returnGeneratedKeys);
            Publisher<R> rows = tracker.rows(entities, mapper, before);
            Flux<BatchChunkResult> chunks = writer.apply(
                    rows, tracker.completion(after, false), tracker.generatedKeys());
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

    private RepositoryBatchLifecyclePlan lifecyclePlan(EntityLifecyclePhase after,
                                                        boolean returnGeneratedKeys) {
        return RepositoryBatchLifecyclePlan.select(lifecycle.hasWork(after), returnGeneratedKeys);
    }

    private static <T, R> Publisher<R> directRows(Publisher<T> entities, Function<T, R> mapper) {
        Function<T, R> safeMapper = Objects.requireNonNull(mapper, "batch entity mapper must not be null");
        return Flux.from(Objects.requireNonNull(entities, "repository entities must not be null"))
                   .map(entity -> Objects.requireNonNull(
                           safeMapper.apply(entity), "mapped batch row must not be null"));
    }

    private BatchLifecycleTracker<T> tracker(EntityLifecyclePhase after,
                                             BatchWriteOptions options,
                                             boolean retainForGeneratedKeys) {
        return new BatchLifecycleTracker<>(lifecycle.dispatcher(), after, ids, retainForGeneratedKeys,
                                           options.maxBufferedBytes(),
                                           this::retainedMemory);
    }

    private BatchLifecycleTracker.RetainedMemory retainedMemory(T entity, Object row) {
        Map<String, Object> snapshot = entityValues.read(entity);
        Object[] entityValuesRoot = snapshot.values().toArray();
        long entityBytes = saturatedAdd(64L, BatchMemoryBudget.estimateRowBytes(entityValuesRoot));
        long retainedValueBytes;
        long rowOverhead = 0L;
        if (row instanceof BatchOptimisticUpdate update) {
            retainedValueBytes = update.estimatedRetainedBytes(entityValuesRoot);
            rowOverhead = 128L;
        } else {
            retainedValueBytes = BatchMemoryBudget.estimateValueBytes(new Object[]{entityValuesRoot, row});
        }
        long combinedBytes = saturatedAdd(
                64L + rowOverhead, retainedValueBytes);
        return new BatchLifecycleTracker.RetainedMemory(entityBytes, combinedBytes);
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    @FunctionalInterface
    interface BatchWriter<R> {
        Mono<BatchWriteResult> apply(Publisher<R> rows,
                                     BatchWriteCompletion completion,
                                     BatchGeneratedKeys generatedKeys);
    }

    @FunctionalInterface
    interface EvidenceWriter<R> {
        Mono<BatchExecutionEvidence> apply(Publisher<R> rows,
                                           BatchWriteCompletion completion,
                                           BatchGeneratedKeys generatedKeys);
    }

    @FunctionalInterface
    interface ChunkWriter<R> {
        Flux<BatchChunkResult> apply(Publisher<R> rows,
                                    BatchWriteCompletion completion,
                                    BatchGeneratedKeys generatedKeys);
    }
}
