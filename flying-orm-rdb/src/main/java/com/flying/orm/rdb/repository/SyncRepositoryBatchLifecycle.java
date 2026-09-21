package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** Retains only in-flight entities until the executor completes each row's ORM work. */
final class SyncRepositoryBatchLifecycle<T> {

    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final EntityLifecyclePhase afterPhase;
    private final EntityValues<T> entityValues;
    private final long maxRetainedBytes;
    private final boolean retainForLifecycle;
    private final boolean retainForGeneratedKeys;
    private final RepositoryEntityIdSupport<T> ids;
    private final Map<Long, RetainedEntity<T>> retained = new ConcurrentHashMap<>();
    private final AtomicLong retainedBytes = new AtomicLong();
    SyncRepositoryBatchLifecycle(SyncRepositoryLifecycleSupport<T> lifecycle,
                                 EntityLifecyclePhase afterPhase,
                                 EntityValues<T> entityValues,
                                 RepositoryEntityIdSupport<T> ids,
                                 boolean retainForGeneratedKeys,
                                 long maxRetainedBytes) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.afterPhase = Objects.requireNonNull(afterPhase, "batch after lifecycle phase must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
        if (maxRetainedBytes <= 0L) {
            throw new IllegalArgumentException("batch lifecycle retained byte limit must be positive");
        }
        this.maxRetainedBytes = maxRetainedBytes;
        this.retainForLifecycle = lifecycle.hasWork(afterPhase);
        this.retainForGeneratedKeys = retainForGeneratedKeys;
    }

    void remember(long offset, T entity) {
        if (!retainForLifecycle && !retainForGeneratedKeys) {
            return;
        }
        T safeEntity = Objects.requireNonNull(entity, "repository batch entity must not be null");
        long bytes = estimate(safeEntity);
        long total = addBytes(bytes);
        if (total == Long.MAX_VALUE || total > maxRetainedBytes) {
            retainedBytes.addAndGet(-bytes);
            throw new BatchMemoryLimitExceededException("lifecycleRetainedBytes", maxRetainedBytes, total);
        }
        retained.put(offset, new RetainedEntity<>(safeEntity, bytes));
    }

    /** 为数据库生成主键创建按整批输入偏移回填的同步协作。 */
    BatchGeneratedKeys generatedKeys() {
        if (!retainForGeneratedKeys) {
            return BatchGeneratedKeys.none();
        }
        return BatchGeneratedKeys.required(ids.generatedKeyColumn(), this::applyGeneratedKey);
    }

    void rowCompleted(long offset) {
        RetainedEntity<T> entity = remove(offset);
        if (entity == null) {
            return;
        }
        if (retainForGeneratedKeys && !entity.generatedKeyApplied()) {
            throw new MappingException(
                    "batch executor completed without returning a generated key at offset " + offset);
        }
        if (retainForLifecycle) {
            lifecycle.fire(afterPhase, entity.entity(), offset);
        }
    }

    void abort() {
        clear();
    }

    private void applyGeneratedKey(long offset, DynamicRow generatedKey) {
        RetainedEntity<T> entity = retained.get(offset);
        if (entity == null) {
            throw new MappingException("generated key does not match a retained batch entity at offset " + offset);
        }
        entity.apply(ids, generatedKey, offset);
    }

    private RetainedEntity<T> remove(long offset) {
        RetainedEntity<T> entity = retained.remove(offset);
        if (entity != null) {
            retainedBytes.addAndGet(-entity.bytes());
        }
        return entity;
    }

    private long estimate(T entity) {
        Map<String, Object> snapshot = entityValues.read(entity);
        long valueBytes = BatchMemoryBudget.estimateRowBytes(snapshot.values().toArray());
        return valueBytes > Long.MAX_VALUE - 64L ? Long.MAX_VALUE : valueBytes + 64L;
    }

    private long addBytes(long bytes) {
        while (true) {
            long current = retainedBytes.get();
            long next = Long.MAX_VALUE - current < bytes ? Long.MAX_VALUE : current + bytes;
            if (retainedBytes.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    private void clear() {
        retained.clear();
        retainedBytes.set(0L);
    }

    private static final class RetainedEntity<T> extends RepositoryEntityIdSupport.BatchState<T> {

        private final long bytes;

        private RetainedEntity(T entity, long bytes) {
            super(entity);
            this.bytes = bytes;
        }

        private long bytes() { return bytes; }
    }

}
