package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证同步 Repository 批量行映射在 callback 失败后仍会终止唯一上游订阅。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class SyncRepositoryBatchRowsTest {

    /** Repository 行映射器只接受第一个上游订阅，并立即取消协议违规的重复订阅。 */
    @Test
    void cancelsDuplicateSourceSubscription() {
        AtomicInteger downstreamSubscriptions = new AtomicInteger();
        AtomicInteger duplicateCancellations = new AtomicInteger();
        Subscription first = new Subscription() {
            @Override public void request(long count) { }
            @Override public void cancel() { }
        };
        Subscription duplicate = new Subscription() {
            @Override public void request(long count) { }
            @Override public void cancel() { duplicateCancellations.incrementAndGet(); }
        };
        Publisher<BatchEntity> source = subscriber -> {
            subscriber.onSubscribe(first);
            subscriber.onSubscribe(duplicate);
        };
        SyncRepositoryBatchRows<BatchEntity, Map<String, Object>> rows = new SyncRepositoryBatchRows<>(
                source,
                entity -> Map.of("id", entity.getId()),
                EntityLifecyclePhase.PRE_PERSIST,
                lifecycle(),
                retention());

        rows.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) {
                downstreamSubscriptions.incrementAndGet();
            }
            @Override public void onNext(Map<String, Object> value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        assertEquals(1, downstreamSubscriptions.get());
        assertEquals(1, duplicateCancellations.get());
    }

    /** 普通 Error 必须原样出站、取消上游且拒绝之后的 demand，不能让同一实体再次进入 mapper。 */
    @Test
    void cancelsSourceAndStopsFurtherDemandWhenMapperThrowsError() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger mappings = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AssertionError failure = new AssertionError("repository row mapper error");
        Publisher<BatchEntity> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L && !cancelled.get()) {
                    subscriber.onNext(new BatchEntity(1L));
                }
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });

        SyncRepositoryBatchRows<BatchEntity, Map<String, Object>> rows = new SyncRepositoryBatchRows<>(
                source,
                entity -> {
                    mappings.incrementAndGet();
                    throw failure;
                },
                EntityLifecyclePhase.PRE_PERSIST,
                lifecycle(),
                retention());
        rows.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) { downstream.set(subscription); }
            @Override public void onNext(Map<String, Object> value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        Throwable observed = null;
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed = error;
        }

        assertSame(failure, observed);
        assertTrue(cancelled.get());
        assertEquals(1, mappings.get());
        assertDoesNotThrow(() -> downstream.get().request(1));
        assertEquals(1, mappings.get());
    }

    /**
     * 普通 mapper 失败后，取消仅是清理动作；取消回调的普通异常不得从 request 外逸或覆盖下游应见的 mapper 失败。
     */
    @Test
    void retainsMapperRuntimeWhenSourceCancellationAlsoFailsNormally() {
        AtomicInteger cancellationCalls = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AtomicReference<Throwable> observed = new AtomicReference<>();
        IllegalStateException mapperFailure = new IllegalStateException("repository row mapper failure");
        IllegalStateException cancellationFailure = new IllegalStateException("repository source cancellation failure");
        Publisher<BatchEntity> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onNext(new BatchEntity(1L));
                }
            }

            @Override
            public void cancel() {
                cancellationCalls.incrementAndGet();
                throw cancellationFailure;
            }
        });

        SyncRepositoryBatchRows<BatchEntity, Map<String, Object>> rows = new SyncRepositoryBatchRows<>(
                source,
                entity -> {
                    throw mapperFailure;
                },
                EntityLifecyclePhase.PRE_PERSIST,
                lifecycle(),
                retention());
        rows.subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) { downstream.set(subscription); }
            @Override public void onNext(Map<String, Object> value) { }
            @Override public void onError(Throwable error) { observed.set(error); }
            @Override public void onComplete() { }
        });

        assertDoesNotThrow(() -> downstream.get().request(1));
        assertSame(mapperFailure, observed.get());
        assertEquals(1, cancellationCalls.get());
        assertEquals(1, mapperFailure.getSuppressed().length);
        assertSame(cancellationFailure, mapperFailure.getSuppressed()[0]);
        assertNull(cancellationFailure.getCause());
    }

    private static SyncRepositoryLifecycleSupport<BatchEntity> lifecycle() {
        return new SyncRepositoryLifecycleSupport<>(metadata(), null, new SyncRepositoryAwaiter(Duration.ofSeconds(1)));
    }

    private static SyncRepositoryBatchLifecycle<BatchEntity> retention() {
        EntityMetadata<BatchEntity> metadata = metadata();
        return new SyncRepositoryBatchLifecycle<>(
                lifecycle(),
                EntityLifecyclePhase.POST_PERSIST,
                ignored -> Map.of(),
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()),
                false,
                1024L);
    }

    private static EntityMetadata<BatchEntity> metadata() {
        return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()).metadata(BatchEntity.class);
    }

    @TableName("batch_entity")
    private static final class BatchEntity {

        @TableId(value = "id", type = IdType.INPUT)
        private Long id;

        private BatchEntity(Long id) {
            this.id = id;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }
    }
}
