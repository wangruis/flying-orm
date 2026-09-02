package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
import com.flying.orm.rdb.internal.mapping.EntityValues;
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
    private final EntityValues<T> entityValues;
    private final long maxRetainedBytes;
    private final boolean retainForLifecycle;
    private final boolean retainForGeneratedKeys;
    private final RepositoryEntityIdSupport<T> ids;
    private final Map<Long, RetainedEntity<T>> retained = new ConcurrentHashMap<>();
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicBoolean completed = new AtomicBoolean();
    /** 普通 POST 失败不终止后续独立片，在最终完成或失败清理时一起报告。 */
    private Throwable completionFailure;

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

    BatchWriteCompletion completion() {
        return new BatchChunkCompletion() {
            @Override
            public void afterChunk(BatchChunkResult result) {
                finishChunk(result);
            }

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
            Throwable failure = finishEntries(safeResult);
            if (failure != null) {
                throw RepositoryFailureSupport.propagate(failure);
            }
        } finally {
            clear();
        }
    }

    /**
     * INDEPENDENT 执行异常仍可能携带已确认提交的分片；只撤销未确认实体，不能抹掉这些数据库事实。
     */
    void finishFailure(BatchWriteResult result) {
        BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
        if (safeResult.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            throw new IllegalArgumentException("batch failure lifecycle requires independent mode");
        }
        if (!completed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        try {
            failure = finishEntries(safeResult);
            try {
                restoreAllGeneratedKeys();
            } catch (RuntimeException error) {
                failure = failure == null ? error : RepositoryFailureSupport.merge(failure, error);
            }
        } finally {
            clear();
        }
        if (failure != null) {
            throw RepositoryFailureSupport.propagate(failure);
        }
    }

    void abort() {
        if (!completed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = completionFailure;
        completionFailure = null;
        try {
            if (failure != null) {
                throw RepositoryFailureSupport.propagate(
                        RepositoryFailureSupport.afterCleanup(failure, this::restoreAllGeneratedKeys));
            }
            restoreAllGeneratedKeys();
        } finally {
            clear();
        }
    }
    private Throwable finishEntries(BatchWriteResult result) {
        for (BatchChunkResult chunk : result.chunks()) {
            if (retained.isEmpty()) {
                break;
            }
            finishChunk(chunk);
        }
        Throwable failure = completionFailure;
        completionFailure = null;
        return failure;
    }

    private void finishChunk(BatchChunkResult chunk) {
        for (int index = 0; index < chunk.inputCount(); index++) {
            try {
                finishEntry(chunk, index);
            } catch (RuntimeException error) {
                completionFailure = completionFailure == null ? error
                        : RepositoryFailureSupport.merge(completionFailure, error);
            } catch (Error error) {
                if (chunk.status() == BatchChunkResult.Status.COMMITTED) {
                    // 直接 Error 停止 POST，但同片已提交实体不能再交给 abort 恢复主键。
                    for (int pending = index; pending < chunk.inputCount(); pending++) {
                        remove(chunk.startOffset() + pending);
                    }
                }
                throw error;
            }
        }
    }

    private void finishEntry(BatchChunkResult chunk, int index) {
        long offset = chunk.startOffset() + index;
        RetainedEntity<T> entity = remove(offset);
        if (entity == null) {
            return;
        }
        if (chunk.status() != BatchChunkResult.Status.COMMITTED) {
            restoreGeneratedKey(entity);
            return;
        }
        if (retainForGeneratedKeys && !entity.generatedKeyApplied()) {
            throw new MappingException(
                    "batch executor committed without returning a generated key at offset " + offset);
        }
        if (retainForLifecycle) {
            lifecycle.fire(afterPhase, entity.entity(), chunk);
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
        } catch (RuntimeException error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> restoreGeneratedKey(entity)));
        }
    }

    private void restoreAllGeneratedKeys() {
        Throwable failure = null;
        for (RetainedEntity<T> entity : retained.values()) {
            try {
                restoreGeneratedKey(entity);
            } catch (RuntimeException error) {
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
        completionFailure = null;
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
