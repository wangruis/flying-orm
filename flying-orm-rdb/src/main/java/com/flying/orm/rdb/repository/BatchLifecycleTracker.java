package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 在批量 Publisher 与分片回执之间保存最小的“输入偏移 -> 实体”关系。
 *
 * <p>before 回调完成后才读取实体并编号；chunk 回来后立即移除对应引用。只有数据库明确返回 COMMITTED
 * 才执行 after，UNKNOWN、冲突、失败和回滚都不会伪装成成功。tracker 属于一次订阅，绝不跨请求共享。</p>
 */
final class BatchLifecycleTracker<T> {

    private final EntityLifecycleDispatcher<T> lifecycle;

    private final AtomicLong nextOffset = new AtomicLong();

    private final Map<Long, RetainedEntity<T>> entities = new ConcurrentHashMap<>();

    private final boolean retainForLifecycle;

    private final boolean retainForGeneratedKeys;

    private final RepositoryEntityIdSupport<T> ids;

    private final long maxRetainedBytes;

    private final RetainedMemoryEstimator<T> retainedMemory;

    /** 只统计实体本身，保留原有的生命周期内存诊断。 */
    private final AtomicLong currentEntityBytes = new AtomicLong();

    /** 统计实体和由它生成的参数对象，这是一次 Repository 批量调用真正的总保留量。 */
    private final AtomicLong currentRetainedBytes = new AtomicLong();

    /** 汇总入口的普通 POST 失败不取消已经启动或后续的独立分片。 */
    private final AtomicReference<Throwable> completionFailure = new AtomicReference<>();

    BatchLifecycleTracker(EntityLifecycleDispatcher<T> lifecycle,
                          EntityLifecyclePhase afterPhase,
                          RepositoryEntityIdSupport<T> ids,
                          boolean retainForGeneratedKeys,
                          long maxRetainedBytes,
                          RetainedMemoryEstimator<T> retainedMemory) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "batch lifecycle dispatcher must not be null");
        this.retainForLifecycle = lifecycle.hasWork(Objects.requireNonNull(
                afterPhase, "batch after lifecycle phase must not be null"));
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
        this.retainForGeneratedKeys = retainForGeneratedKeys;
        if (maxRetainedBytes <= 0L) {
            throw new IllegalArgumentException("batch lifecycle retained byte limit must be positive");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.retainedMemory = Objects.requireNonNull(
                retainedMemory, "batch lifecycle retained byte estimator must not be null");
    }

    /**
     * 为数据库生成主键创建按全局输入偏移回填的协作。普通批量返回 none，不会让执行内核进入取键路径。
     */
    BatchGeneratedKeys generatedKeys() {
        if (!retainForGeneratedKeys) {
            return BatchGeneratedKeys.none();
        }
        return BatchGeneratedKeys.required(ids.generatedKeyColumn(), this::applyGeneratedKey);
    }

    BatchChunkCompletion completion(EntityLifecyclePhase afterPhase, boolean delayCallbackErrors) {
        return new BatchChunkCompletion() {
            @Override
            public void afterChunk(BatchChunkResult result) {
                confirmChunk(result);
            }

            @Override
            public Publisher<Void> afterChunkReleased(BatchChunkResult result) {
                Mono<Void> callbacks = finish(result, afterPhase);
                return delayCallbackErrors
                        ? callbacks.onErrorResume(error -> rememberFailure(completionFailure, error)) : callbacks;
            }

            @Override
            public Publisher<Void> afterCompletion(BatchWriteResult result) {
                return finish(result, afterPhase);
            }
        };
    }

    /** 先确认整片，再进入可能异步等待的 POST，取消不能恢复已提交实体的主键。 */
    private void confirmChunk(BatchChunkResult chunk) {
        if (!retainForGeneratedKeys || chunk.status() != BatchChunkResult.Status.COMMITTED) {
            return;
        }
        for (int index = 0; index < chunk.inputCount(); index++) {
            RetainedEntity<T> retained = entities.get(chunk.startOffset() + index);
            if (retained != null) {
                retained.committed = true;
            }
        }
    }

    <R> Publisher<R> rows(Publisher<T> source,
                          Function<T, R> mapper,
                          EntityLifecyclePhase beforePhase) {
        Function<T, R> safeMapper = Objects.requireNonNull(mapper, "batch entity mapper must not be null");
        return Flux.from(Objects.requireNonNull(source, "repository entities must not be null"))
                   // concatMap 保证 offset 和提交给批量渲染器的行顺序完全一致。
                   .concatMap(entity -> lifecycle.fire(beforePhase, entity, null)
                                                 .then(Mono.fromSupplier(() -> remember(entity, safeMapper))));
    }

    Mono<Void> finish(BatchWriteResult result, EntityLifecyclePhase afterPhase) {
        if (result.mode() == BatchWriteOptions.Mode.ATOMIC
                && result.status() != BatchWriteResult.Status.COMMITTED) {
            // 原子模式只认整批最终提交。某个 chunk 曾执行成功，也可能被后续失败带着一起回滚。
            restoreAndClear();
            return Mono.empty();
        }
        return finishChunks(result.chunks(), afterPhase, false);
    }

    /** 按 INDEPENDENT 失败回执处理已确认分片，再撤销没有回执的剩余实体。 */
    Mono<Void> finishFailure(BatchWriteResult result, EntityLifecyclePhase afterPhase) {
        BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
        if (safeResult.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Mono.error(new IllegalArgumentException(
                    "batch failure lifecycle requires independent mode"));
        }
        return finishChunks(safeResult.chunks(), afterPhase, true);
    }

    Mono<Void> finish(BatchChunkResult chunk, EntityLifecyclePhase afterPhase) {
        confirmChunk(chunk);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        return Flux.range(0, chunk.inputCount())
                   .concatMap(index -> finishEntity(chunk, index, afterPhase)
                           .onErrorResume(error -> rememberFailure(callbackFailure, error)))
                   .then(Mono.defer(() -> callbackFailure.get() == null
                           ? Mono.empty() : Mono.error(callbackFailure.get())));
    }

    private Mono<Void> finishChunks(List<BatchChunkResult> chunks,
                                    EntityLifecyclePhase afterPhase,
                                    boolean restoreRemaining) {
        if (entities.isEmpty()) {
            return finishTracked(null, restoreRemaining);
        }
        chunks.forEach(this::confirmChunk);
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        Mono<Void> callbacks = Flux.fromIterable(chunks)
                .concatMap(chunk -> finish(chunk, afterPhase)
                        .onErrorResume(error -> rememberFailure(callbackFailure, error)))
                .then();
        return callbacks
                .onErrorMap(error -> RepositoryFailureSupport.afterCleanup(
                        error, restoreRemaining ? this::restoreAndClear : this::clear))
                .then(Mono.defer(() -> finishTracked(callbackFailure.get(), restoreRemaining)))
                .doOnCancel(() -> RepositoryFailureSupport.cleanupAfterCancellation(
                        restoreRemaining ? this::restoreAndClear : this::clear));
    }

    private Mono<Void> finishTracked(Throwable callbackFailure, boolean restoreRemaining) {
        Throwable failure = completionFailure.getAndSet(null);
        if (callbackFailure != null) {
            failure = failure == null ? callbackFailure : RepositoryFailureSupport.merge(failure, callbackFailure);
        }
        try {
            if (restoreRemaining) {
                restoreAndClear();
            } else {
                clear();
            }
        } catch (RuntimeException cleanupFailure) {
            failure = failure == null
                    ? cleanupFailure : RepositoryFailureSupport.merge(failure, cleanupFailure);
        }
        return failure == null ? Mono.empty() : Mono.error(failure);
    }

    private static Mono<Void> rememberFailure(AtomicReference<Throwable> failures, Throwable error) {
        Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(error);
        if (preferred instanceof Error) {
            return Mono.error(preferred);
        }
        failures.updateAndGet(current -> current == null
                ? error : RepositoryFailureSupport.merge(current, error));
        return Mono.empty();
    }

    private Mono<Void> finishEntity(BatchChunkResult chunk,
                                    int index,
                                    EntityLifecyclePhase afterPhase) {
        long offset = chunk.startOffset() + index;
        RetainedEntity<T> retained = remove(offset);
        if (retained == null) {
            return Mono.empty();
        }
        if (chunk.status() != BatchChunkResult.Status.COMMITTED) {
            restoreGeneratedKey(retained);
            return Mono.empty();
        }
        if (retainForGeneratedKeys && !retained.generatedKeyApplied()) {
            return Mono.error(new MappingException(
                    "batch executor committed without returning a generated key at offset " + offset));
        }
        if (!retainForLifecycle) {
            return Mono.empty();
        }
        return lifecycle.fire(afterPhase, retained.entity(), chunk);
    }

    void clear() {
        entities.clear();
        currentEntityBytes.set(0L);
        currentRetainedBytes.set(0L);
    }

    /** 执行异常或订阅取消没有最终回执时，所有尚未确认提交的生成键都必须撤销。 */
    void abort() {
        Throwable failure = completionFailure.getAndSet(null);
        if (failure != null) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(failure, this::restoreAndClear));
        }
        restoreAndClear();
    }

    private <R> R remember(T entity, Function<T, R> mapper) {
        R row = Objects.requireNonNull(mapper, "batch entity mapper must not be null").apply(entity);
        long offset = nextOffset.getAndIncrement();
        if (!retainForLifecycle && !retainForGeneratedKeys) {
            return row;
        }
        RetainedMemory memory = Objects.requireNonNull(
                retainedMemory.estimate(entity, row), "batch retained memory estimate must not be null");
        long entityBytes = Math.max(1L, memory.entityBytes());
        long entityTotal = addBytes(currentEntityBytes, entityBytes);
        if (entityTotal == Long.MAX_VALUE || entityTotal > maxRetainedBytes) {
            currentEntityBytes.addAndGet(-entityBytes);
            throw new BatchMemoryLimitExceededException(
                    "lifecycleRetainedBytes", maxRetainedBytes, entityTotal);
        }

        // 参数对象会和实体一起存活到 chunk 回执。两者必须共用上限，不能各自都卡在上限以内。
        long combinedBytes = Math.max(1L, memory.combinedBytes());
        long combinedTotal = addBytes(currentRetainedBytes, combinedBytes);
        if (combinedTotal == Long.MAX_VALUE || combinedTotal > maxRetainedBytes) {
            currentEntityBytes.addAndGet(-entityBytes);
            currentRetainedBytes.addAndGet(-combinedBytes);
            throw new BatchMemoryLimitExceededException(
                    "combinedRetainedBytes", maxRetainedBytes, combinedTotal);
        }
        entities.put(offset, new RetainedEntity<>(entity, entityBytes, combinedBytes));
        return row;
    }

    private void applyGeneratedKey(long offset, DynamicRow generatedKey) {
        RetainedEntity<T> retained = entities.get(offset);
        if (retained == null) {
            throw new MappingException("generated key does not match a retained batch entity at offset " + offset);
        }
        Object originalValue = ids.currentGeneratedKey(retained.entity());
        if (!retained.markGeneratedKeyApplied(originalValue)) {
            throw new MappingException("generated key was delivered more than once at batch offset " + offset);
        }
        try {
            ids.applyGeneratedKey(retained.entity(), generatedKey);
        } catch (RuntimeException error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> restoreGeneratedKey(retained)));
        }
    }

    private void restoreAndClear() {
        Throwable failure = null;
        try {
            for (RetainedEntity<T> retained : entities.values()) {
                try {
                    restoreGeneratedKey(retained);
                } catch (RuntimeException error) {
                    failure = failure == null ? error : RepositoryFailureSupport.merge(failure, error);
                }
            }
        } finally {
            clear();
        }
        if (failure != null) {
            throw RepositoryFailureSupport.propagate(failure);
        }
    }

    private void restoreGeneratedKey(RetainedEntity<T> retained) {
        if (retainForGeneratedKeys && retained.generatedKeyApplied() && !retained.committed) {
            ids.restoreGeneratedKey(retained.entity(), retained.originalGeneratedKey());
        }
    }

    private RetainedEntity<T> remove(long offset) {
        RetainedEntity<T> retained = entities.remove(offset);
        if (retained != null) {
            currentEntityBytes.addAndGet(-retained.entityBytes());
            currentRetainedBytes.addAndGet(-retained.combinedBytes());
        }
        return retained;
    }

    private static long addBytes(AtomicLong counter, long bytes) {
        while (true) {
            long current = counter.get();
            long updated = saturatedAdd(current, bytes);
            if (counter.compareAndSet(current, updated)) {
                return updated;
            }
        }
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    @FunctionalInterface
    interface RetainedMemoryEstimator<T> {
        RetainedMemory estimate(T entity, Object row);
    }

    record RetainedMemory(long entityBytes, long combinedBytes) {
    }

    private static final class RetainedEntity<T> {

        private final T entity;
        private final long entityBytes;
        private final long combinedBytes;
        private final AtomicBoolean generatedKeyApplied = new AtomicBoolean();
        private volatile Object originalGeneratedKey;
        private volatile boolean committed;

        private RetainedEntity(T entity, long entityBytes, long combinedBytes) {
            this.entity = entity;
            this.entityBytes = entityBytes;
            this.combinedBytes = combinedBytes;
        }

        private T entity() { return entity; }
        private long entityBytes() { return entityBytes; }
        private long combinedBytes() { return combinedBytes; }
        private synchronized boolean markGeneratedKeyApplied(Object originalValue) {
            if (generatedKeyApplied.get()) {
                return false;
            }
            originalGeneratedKey = originalValue;
            generatedKeyApplied.set(true);
            return true;
        }
        private boolean generatedKeyApplied() { return generatedKeyApplied.get(); }
        private Object originalGeneratedKey() { return originalGeneratedKey; }
    }

}
