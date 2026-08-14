package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 同步批量写入中“输入偏移 -> 实体”的最小保留表。
 *
 * <p>只有 POST 生命周期真的有工作时才保留实体。保留量和批量参数共用同一个上限，分片收到已提交回执后
 * 就立刻删除对应实体。ATOMIC 的 ENLISTED 结果不会猜测外部事务结局，实体会交给
 * {@link BatchWriteCompletion} 在上层事务最终完成时处理。</p>
 */
final class SyncRepositoryBatchLifecycle<T> {

    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final EntityLifecyclePhase afterPhase;
    private final Function<T, Map<String, Object>> values;
    private final long maxRetainedBytes;
    private final boolean retainForLifecycle;
    private final boolean retainForGeneratedKeys;
    private final RepositoryEntityIdSupport<T> ids;
    private final Map<Long, RetainedEntity<T>> retained = new ConcurrentHashMap<>();
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicBoolean completed = new AtomicBoolean();

    SyncRepositoryBatchLifecycle(SyncRepositoryLifecycleSupport<T> lifecycle,
                                 EntityLifecyclePhase afterPhase,
                                 Function<T, Map<String, Object>> values,
                                 RepositoryEntityIdSupport<T> ids,
                                 boolean retainForGeneratedKeys,
                                 long maxRetainedBytes) {
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.afterPhase = Objects.requireNonNull(afterPhase, "batch after lifecycle phase must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
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

    BatchWriteCompletion completion() {
        return new BatchWriteCompletion() {
            @Override
            public Publisher<Void> afterCompletion(BatchWriteResult result) {
                return new CompletionPublisher(() -> finish(result));
            }

            @Override
            public void afterCompletionUnavailable(BatchWriteResult result) {
                finish(result);
            }
        };
    }

    void finish(BatchWriteResult result) {
        BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
        if (safeResult.status() == BatchWriteResult.Status.ENLISTED || !completed.compareAndSet(false, true)) {
            return;
        }
        try {
            if (safeResult.mode() == BatchWriteOptions.Mode.ATOMIC
                    && safeResult.status() != BatchWriteResult.Status.COMMITTED) {
                restoreAllGeneratedKeys();
                return;
            }
            for (BatchChunkResult chunk : safeResult.chunks()) {
                finishChunk(chunk);
            }
        } finally {
            clear();
        }
    }
    void abort() {
        completed.set(true);
        try {
            restoreAllGeneratedKeys();
        } finally {
            clear();
        }
    }
    private void finishChunk(BatchChunkResult chunk) {
        if (chunk.status() != BatchChunkResult.Status.COMMITTED) {
            restoreAndRelease(chunk);
            return;
        }
        for (int index = 0; index < chunk.inputCount(); index++) {
            long offset = chunk.startOffset() + index;
            RetainedEntity<T> entity = remove(offset);
            if (entity != null && retainForGeneratedKeys && !entity.generatedKeyApplied()) {
                throw new MappingException(
                        "batch executor committed without returning a generated key at offset " + offset);
            }
            if (entity != null && retainForLifecycle) {
                lifecycle.fire(afterPhase, entity.entity(), chunk);
            }
        }
    }
    private void applyGeneratedKey(long offset, DynamicRow generatedKey) {
        RetainedEntity<T> entity = retained.get(offset);
        if (entity == null) {
            throw new MappingException("generated key does not match a retained batch entity at offset " + offset);
        }
        Object originalValue = ids.currentGeneratedKey(entity.entity());
        if (!entity.markGeneratedKeyApplied(originalValue)) {
            throw new MappingException("generated key was delivered more than once at batch offset " + offset);
        }
        try {
            ids.applyGeneratedKey(entity.entity(), generatedKey);
        } catch (Throwable error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> restoreGeneratedKey(entity)));
        }
    }

    private void restoreAndRelease(BatchChunkResult chunk) {
        Throwable failure = null;
        for (int index = 0; index < chunk.inputCount(); index++) {
            RetainedEntity<T> entity = remove(chunk.startOffset() + index);
            if (entity != null) {
                try {
                    restoreGeneratedKey(entity);
                } catch (Throwable error) {
                    failure = failure == null ? error : RepositoryFailureSupport.merge(failure, error);
                }
            }
        }
        if (failure != null) {
            throw RepositoryFailureSupport.propagate(failure);
        }
    }

    private void restoreAllGeneratedKeys() {
        Throwable failure = null;
        for (RetainedEntity<T> entity : retained.values()) {
            try {
                restoreGeneratedKey(entity);
            } catch (Throwable error) {
                failure = failure == null ? error : RepositoryFailureSupport.merge(failure, error);
            }
        }
        if (failure != null) {
            throw RepositoryFailureSupport.propagate(failure);
        }
    }

    private void restoreGeneratedKey(RetainedEntity<T> entity) {
        if (retainForGeneratedKeys && entity.generatedKeyApplied()) {
            ids.restoreGeneratedKey(entity.entity(), entity.originalGeneratedKey());
        }
    }

    private RetainedEntity<T> remove(long offset) {
        RetainedEntity<T> entity = retained.remove(offset);
        if (entity != null) {
            retainedBytes.addAndGet(-entity.bytes());
        }
        return entity;
    }

    private long estimate(T entity) {
        Map<String, Object> snapshot = Objects.requireNonNull(values.apply(entity),
                                                               "repository entity values must not be null");
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

    private static final class RetainedEntity<T> {

        private final T entity;
        private final long bytes;
        private final AtomicBoolean generatedKeyApplied = new AtomicBoolean();
        private volatile Object originalGeneratedKey;

        private RetainedEntity(T entity, long bytes) {
            this.entity = entity;
            this.bytes = bytes;
        }

        private T entity() { return entity; }
        private long bytes() { return bytes; }
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

    /** 把同步完成动作暴露成 Reactive Streams 信号，不强制 JDBC 调用方引入 Reactor。 */
    private static final class CompletionPublisher implements Publisher<Void> {
        private final Runnable action;

        private CompletionPublisher(Runnable action) {
            this.action = Objects.requireNonNull(action, "batch completion action must not be null");
        }

        @Override
        public void subscribe(Subscriber<? super Void> subscriber) {
            Subscriber<? super Void> safeSubscriber = Objects.requireNonNull(
                    subscriber, "batch completion subscriber must not be null");
            safeSubscriber.onSubscribe(new CompletionSubscription(safeSubscriber, action));
        }
    }

    private static final class CompletionSubscription implements Subscription {
        private final Subscriber<? super Void> subscriber;
        private final Runnable action;
        private final AtomicBoolean terminated = new AtomicBoolean();

        private CompletionSubscription(Subscriber<? super Void> subscriber, Runnable action) {
            this.subscriber = subscriber;
            this.action = action;
        }

        @Override
        public void request(long count) {
            if (!terminated.compareAndSet(false, true)) {
                return;
            }
            if (count <= 0L) {
                subscriber.onError(new IllegalArgumentException("batch completion demand must be positive"));
                return;
            }
            try {
                action.run();
                subscriber.onComplete();
            } catch (RuntimeException error) {
                subscriber.onError(error);
            }
        }

        @Override
        public void cancel() {
            terminated.set(true);
        }
    }

}
