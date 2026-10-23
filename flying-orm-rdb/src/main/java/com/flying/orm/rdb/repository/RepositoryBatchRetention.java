package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongBiFunction;
import java.util.function.Function;

/** 同步与响应式批量共用的在途实体、生成键状态及显式保留预算；不执行生命周期或 SQL。 */
final class RepositoryBatchRetention<T> {
    private final RepositoryEntityIdSupport<T> ids;
    private final boolean returnGeneratedKeys;
    private final long maxRetainedBytes;
    private final String limitName;
    private final ToLongBiFunction<T, Object> retainedMemory;
    private final Map<Long, RetainedEntity<T>> entities = new ConcurrentHashMap<>();
    private final AtomicLong retainedBytes = new AtomicLong();

    RepositoryBatchRetention(RepositoryEntityIdSupport<T> ids, boolean returnGeneratedKeys,
                             long maxRetainedBytes, String limitName,
                             ToLongBiFunction<T, Object> retainedMemory) {
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
        this.returnGeneratedKeys = returnGeneratedKeys;
        if (maxRetainedBytes <= 0L) throw new IllegalArgumentException("batch lifecycle retained byte limit must be positive");
        this.maxRetainedBytes = maxRetainedBytes;
        this.limitName = limitName;
        this.retainedMemory = Objects.requireNonNull(retainedMemory, "batch retained byte estimator must not be null");
    }

    BatchGeneratedKeys generatedKeys() {
        return returnGeneratedKeys ? BatchGeneratedKeys.required(ids.generatedKeyColumn(), this::applyGeneratedKey)
                : BatchGeneratedKeys.none();
    }

    /** 两端共用逐行映射与保留；序号属于本次订阅，不预取或提前收集输入。 */
    <R> Flux<R> map(Publisher<T> source, Function<T, R> mapper) {
        return Flux.defer(() -> {
            long[] offset = {0L};
            return Flux.from(source).map(entity -> {
                R row = Objects.requireNonNull(mapper.apply(entity), "mapped batch row must not be null");
                remember(offset[0]++, entity, row);
                return row;
            });
        });
    }

    void remember(long offset, T entity, Object row) {
        long bytes = 0L;
        if (maxRetainedBytes != Long.MAX_VALUE) {
            bytes = Math.max(1L, retainedMemory.applyAsLong(entity, row));
            long total = addBytes(bytes);
            if (total == Long.MAX_VALUE || total > maxRetainedBytes) {
                retainedBytes.addAndGet(-bytes);
                throw new BatchMemoryLimitExceededException(limitName, maxRetainedBytes, total);
            }
        }
        entities.put(offset, new RetainedEntity<>(entity, bytes,
                returnGeneratedKeys && ids.requiresGeneratedKey(entity)));
    }

    /** 释放引用和预算后返回实体，由各自执行方式完成 POST；缺失生成键不能冒充成功。 */
    T complete(long offset) {
        RetainedEntity<T> retained = entities.remove(offset);
        if (retained == null) return null;
        if (maxRetainedBytes != Long.MAX_VALUE) retainedBytes.addAndGet(-retained.bytes);
        if (retained.generatedKeyRequired && !retained.generatedKeyApplied()) {
            throw new MappingException("batch executor completed without returning a generated key at offset " + offset);
        }
        return retained.entity;
    }

    private void applyGeneratedKey(long offset, DynamicRow key) {
        RetainedEntity<T> retained = entities.get(offset);
        if (retained == null) {
            throw new MappingException("generated key does not match a retained batch entity at offset " + offset);
        }
        retained.apply(ids, key, offset);
    }

    void clear() {
        entities.clear();
        retainedBytes.set(0L);
    }

    private long addBytes(long bytes) {
        while (true) {
            long current = retainedBytes.get();
            long next = Long.MAX_VALUE - current < bytes ? Long.MAX_VALUE : current + bytes;
            if (retainedBytes.compareAndSet(current, next)) return next;
        }
    }

    private static final class RetainedEntity<T> {
        private final T entity;
        private final long bytes;
        private final boolean generatedKeyRequired;
        private boolean generatedKeyApplied;

        private RetainedEntity(T entity, long bytes, boolean generatedKeyRequired) {
            this.entity = entity;
            this.bytes = bytes;
            this.generatedKeyRequired = generatedKeyRequired;
        }

        private synchronized boolean generatedKeyApplied() {
            return generatedKeyApplied;
        }

        private void apply(RepositoryEntityIdSupport<T> ids, DynamicRow key, long offset) {
            if (!generatedKeyRequired) throw new MappingException("generated key returned for an assigned identity at offset " + offset);
            if (!markGeneratedKeyApplied()) {
                throw new MappingException("generated key was delivered more than once at batch offset " + offset);
            }
            ids.applyGeneratedKey(entity, key);
        }

        private synchronized boolean markGeneratedKeyApplied() {
            if (generatedKeyApplied) return false;
            generatedKeyApplied = true;
            return true;
        }
    }
}
