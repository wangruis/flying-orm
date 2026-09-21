package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/** Per-subscription, bounded retention until each row's ORM work completes. */
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

    <R> Publisher<R> rows(Publisher<T> source,
                          Function<T, R> mapper,
                          EntityLifecyclePhase beforePhase) {
        Function<T, R> safeMapper = Objects.requireNonNull(mapper, "batch entity mapper must not be null");
        return Flux.from(Objects.requireNonNull(source, "repository entities must not be null"))
                   // concatMap 保证 offset 和提交给批量渲染器的行顺序完全一致。
                   .concatMap(entity -> lifecycle.fire(beforePhase, entity, null)
                                                 .then(Mono.fromSupplier(() -> remember(entity, safeMapper))));
    }

    Mono<Void> rowCompleted(long offset, EntityLifecyclePhase afterPhase) {
        return Mono.defer(() -> {
            RetainedEntity<T> retained = remove(offset);
            if (retained == null) {
                return Mono.empty();
            }
            if (retainForGeneratedKeys && !retained.generatedKeyApplied()) {
                return Mono.error(new MappingException(
                        "batch executor completed without returning a generated key at offset " + offset));
            }
            return retainForLifecycle ? lifecycle.fire(afterPhase, retained.entity(), offset) : Mono.empty();
        });
    }

    void clear() {
        entities.clear();
        currentEntityBytes.set(0L);
        currentRetainedBytes.set(0L);
    }

    void abort() {
        clear();
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

        // 参数对象会和实体一起存活到该行 ORM 工作完成。两者必须共用上限，不能各自都卡在上限以内。
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
        retained.apply(ids, generatedKey, offset);
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

    private static final class RetainedEntity<T> extends RepositoryEntityIdSupport.BatchState<T> {

        private final long entityBytes;
        private final long combinedBytes;

        private RetainedEntity(T entity, long entityBytes, long combinedBytes) {
            super(entity);
            this.entityBytes = entityBytes;
            this.combinedBytes = combinedBytes;
        }

        private long entityBytes() { return entityBytes; }
        private long combinedBytes() { return combinedBytes; }
    }

}
