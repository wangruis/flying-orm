package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.ToLongFunction;

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

    private final ToLongFunction<T> retainedBytes;

    /** 只统计实体本身，保留原有的生命周期内存诊断。 */
    private final AtomicLong currentEntityBytes = new AtomicLong();

    /** 统计实体和由它生成的参数对象，这是一次 Repository 批量调用真正的总保留量。 */
    private final AtomicLong currentRetainedBytes = new AtomicLong();

    BatchLifecycleTracker(EntityLifecycleDispatcher<T> lifecycle,
                          EntityLifecyclePhase afterPhase,
                          RepositoryEntityIdSupport<T> ids,
                          boolean retainForGeneratedKeys,
                          long maxRetainedBytes,
                          ToLongFunction<T> retainedBytes) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "batch lifecycle dispatcher must not be null");
        this.retainForLifecycle = lifecycle.hasWork(Objects.requireNonNull(
                afterPhase, "batch after lifecycle phase must not be null"));
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
        this.retainForGeneratedKeys = retainForGeneratedKeys;
        if (maxRetainedBytes <= 0L) {
            throw new IllegalArgumentException("batch lifecycle retained byte limit must be positive");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.retainedBytes = Objects.requireNonNull(retainedBytes,
                                                    "batch lifecycle retained byte estimator must not be null");
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

    <R> Publisher<R> rows(Publisher<T> source,
                          Function<T, R> mapper,
                          EntityLifecyclePhase beforePhase) {
        return Flux.from(Objects.requireNonNull(source, "repository entities must not be null"))
                   // concatMap 保证 offset 和提交给批量渲染器的行顺序完全一致。
                   .concatMap(entity -> lifecycle.fire(beforePhase, entity, null)
                                                 .then(Mono.fromSupplier(() -> remember(entity, mapper))));
    }

    Mono<Void> finish(BatchWriteResult result, EntityLifecyclePhase afterPhase) {
        if (result.mode() == BatchWriteOptions.Mode.ATOMIC
                && result.status() != BatchWriteResult.Status.COMMITTED) {
            // 原子模式只认整批最终提交。某个 chunk 曾执行成功，也可能被后续失败带着一起回滚。
            restoreAndClear();
            return Mono.empty();
        }
        return Flux.fromIterable(result.chunks())
                   .concatMap(chunk -> finish(chunk, afterPhase))
                   .then()
                   .doFinally(ignored -> clear());
    }

    Mono<Void> finish(BatchChunkResult chunk, EntityLifecyclePhase afterPhase) {
        return Flux.range(0, chunk.inputCount())
                   .concatMap(index -> {
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
                   })
                   .then();
    }

    void clear() {
        entities.clear();
        currentEntityBytes.set(0L);
        currentRetainedBytes.set(0L);
    }

    /** 执行异常或订阅取消没有最终回执时，所有尚未确认提交的生成键都必须撤销。 */
    void abort() {
        restoreAndClear();
    }

    private <R> R remember(T entity, Function<T, R> mapper) {
        R row = Objects.requireNonNull(mapper, "batch entity mapper must not be null").apply(entity);
        long offset = nextOffset.getAndIncrement();
        if (!retainForLifecycle && !retainForGeneratedKeys) {
            return row;
        }
        long entityBytes = Math.max(1L, retainedBytes.applyAsLong(entity));
        long entityTotal = addBytes(currentEntityBytes, entityBytes);
        if (entityTotal == Long.MAX_VALUE || entityTotal > maxRetainedBytes) {
            currentEntityBytes.addAndGet(-entityBytes);
            throw new BatchMemoryLimitExceededException(
                    "lifecycleRetainedBytes", maxRetainedBytes, entityTotal);
        }

        // 参数对象会和实体一起存活到 chunk 回执。两者必须共用上限，不能各自都卡在上限以内。
        long rowBytes = mappedRowBytes(row);
        long combinedBytes = saturatedAdd(entityBytes, rowBytes);
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
        } catch (Throwable error) {
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
                } catch (Throwable error) {
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
        if (retainForGeneratedKeys && retained.generatedKeyApplied()) {
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

    private static long mappedRowBytes(Object row) {
        if (row instanceof BatchOptimisticUpdate update) {
            // where 中的主键和版本值来自实体快照，已经计入 entityBytes；这里补上新建的更新 Map 和包装对象。
            return saturatedAdd(128L, BatchMemoryBudget.estimateValueBytes(update.values()));
        }
        return BatchMemoryBudget.estimateValueBytes(row);
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

    private static final class RetainedEntity<T> {

        private final T entity;
        private final long entityBytes;
        private final long combinedBytes;
        private final AtomicBoolean generatedKeyApplied = new AtomicBoolean();
        private volatile Object originalGeneratedKey;

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
