package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveRepositoryRetentionSnapshotTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Test
    void aggregateUpdateEstimatesOwnedValuesWithoutCopyingThem() {
        verifyNoEstimateCopy(false);
    }

    @Test
    void chunkUpdateEstimatesOwnedValuesWithoutCopyingThem() {
        verifyNoEstimateCopy(true);
    }

    private static void verifyNoEstimateCopy(boolean chunks) {
        for (BatchWriteOptions.Mode mode : BatchWriteOptions.Mode.values()) {
            for (boolean tracked : new boolean[]{false, true}) {
                for (int size : new int[]{4096, 1_048_576}) {
                    try (EntityModelRegistry models = models()) {
                        AtomicInteger postUpdates = new AtomicInteger();
                        var coordinator = coordinator(models, PayloadEntity.class, tracked, postUpdates);
                        PayloadEntity entity = new PayloadEntity(size);
                        AtomicInteger mapperClones = new AtomicInteger();
                        AtomicInteger writerClones = new AtomicInteger();
                        CountedTimestamp.COPIES.set(0);
                        var options = options(mode, 4_194_304L);
                        java.util.function.Function<PayloadEntity, BatchOptimisticUpdate> mapper = item -> {
                            BatchOptimisticUpdate row = coordinator.optimisticUpdate(item);
                            mapperClones.set(CountedTimestamp.COPIES.get());
                            return row;
                        };
                        java.util.function.Consumer<BatchOptimisticUpdate> received = row -> {
                            writerClones.set(CountedTimestamp.COPIES.get());
                            byte[] exported = (byte[]) row.values().get("payload");
                            exported[0] = 99;
                            assertEquals(41, ((byte[]) row.values().get("payload"))[0]);
                            assertEquals(41, entity.payload[0]);
                        };
                        if (chunks) {
                            List<BatchChunkResult> result = coordinator.chunks(Flux.just(entity), mapper,
                                    EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                                    options, false, (rows, completion, keys) -> Flux.from(rows)
                                            .doOnNext(received).thenMany(Flux.just(committed())))
                                    .collectList().block(TIMEOUT);
                            assertNotNull(result);
                            assertEquals(1, result.size());
                        } else {
                            BatchWriteResult result = coordinator.write(Flux.just(entity), mapper,
                                    EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                                    options, false, (rows, completion, keys) -> Flux.from(rows)
                                            .doOnNext(received).then(Mono.just(BatchWriteResult.from(mode, List.of(committed())))))
                                    .block(TIMEOUT);
                            assertNotNull(result);
                            assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        }
                        assertEquals(1, mapperClones.get());
                        assertEquals(mapperClones.get(), writerClones.get(), "retention estimation must not clone values");
                        assertEquals(tracked ? 1 : 0, postUpdates.get());
                    }
                }
            }
        }
    }

    @Test
    void combinedBudgetStillCountsTheEntityAndItsOwnedMutableCopy() {
        try (EntityModelRegistry models = models()) {
            AtomicInteger postUpdates = new AtomicInteger();
            var coordinator = coordinator(models, PayloadEntity.class, true, postUpdates);
            BatchMemoryLimitExceededException error = assertThrows(BatchMemoryLimitExceededException.class,
                    () -> coordinator.write(Flux.just(new PayloadEntity(4096)), coordinator::optimisticUpdate,
                            EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                            options(BatchWriteOptions.Mode.ATOMIC, 6000), false,
                            (rows, completion, keys) -> Flux.from(rows)
                                    .then(Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed())))))
                            .block(TIMEOUT));
            assertEquals("combinedRetainedBytes", error.limitName());
            assertEquals(0, postUpdates.get());
        }
    }

    @Test
    void rowBudgetUsesTheMappedSnapshotWhenALaterGetterReturnsASmallerValue() {
        try (EntityModelRegistry models = models()) {
            var coordinator = coordinator(models, ChangingPayloadEntity.class, true, new AtomicInteger());
            BatchMemoryLimitExceededException error = assertThrows(BatchMemoryLimitExceededException.class,
                    () -> coordinator.write(Flux.just(new ChangingPayloadEntity()), coordinator::optimisticUpdate,
                            EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                            options(BatchWriteOptions.Mode.ATOMIC, 4096), false,
                            (rows, completion, keys) -> Flux.from(rows)
                                    .then(Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed())))))
                            .block(TIMEOUT));
            assertEquals("combinedRetainedBytes", error.limitName());
        }
    }

    @Test
    void combinedBudgetCountsSharedCollectionsOnce() {
        try (EntityModelRegistry models = models()) {
            AtomicInteger postUpdates = new AtomicInteger();
            var coordinator = coordinator(models, SharedPayloadEntity.class, true, postUpdates);
            BatchWriteResult result = coordinator.write(Flux.just(new SharedPayloadEntity()), coordinator::optimisticUpdate,
                    EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                    options(BatchWriteOptions.Mode.ATOMIC, 1600), false,
                    (rows, completion, keys) -> Flux.from(rows)
                            .then(Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed())))))
                    .block(TIMEOUT);
            assertNotNull(result);
            assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
            assertEquals(1, postUpdates.get());
        }
    }

    @Test
    void entityBudgetStillCountsBufferCapacityBeyondItsReadableWindow() {
        try (EntityModelRegistry models = models()) {
            var coordinator = coordinator(models, BufferEntity.class, true, new AtomicInteger());
            BatchMemoryLimitExceededException error = assertThrows(BatchMemoryLimitExceededException.class,
                    () -> coordinator.write(Flux.just(new BufferEntity()), coordinator::optimisticUpdate,
                            EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                            options(BatchWriteOptions.Mode.ATOMIC, 6000), false,
                            (rows, completion, keys) -> Flux.from(rows)
                                    .then(Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed())))))
                            .block(TIMEOUT));
            assertEquals("lifecycleRetainedBytes", error.limitName());
        }
    }

    private static <T> ReactiveRepositoryBatchCoordinator<T> coordinator(
            EntityModelRegistry models, Class<T> type, boolean tracked, AtomicInteger postUpdates) {
        var metadata = models.metadata(type);
        ReactiveEntityListener<T> listener = tracked ? event -> {
            if (event.phase() == EntityLifecyclePhase.POST_UPDATE) postUpdates.incrementAndGet();
            return Mono.empty();
        } : null;
        return new ReactiveRepositoryBatchCoordinator<>(metadata, models.entityValues(type),
                new ReactiveRepositoryLifecycleSupport<>(metadata, listener),
                RepositoryEntityIdSupport.create(metadata, models.idGenerator()));
    }

    private static EntityModelRegistry models() {
        return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
    }

    private static BatchWriteOptions options(BatchWriteOptions.Mode mode, long bytes) {
        return (mode == BatchWriteOptions.Mode.ATOMIC
                ? BatchWriteOptions.atomic(1) : BatchWriteOptions.independent(1, 1))
                .withMemoryLimits(1, bytes, 1);
    }

    private static BatchChunkResult committed() {
        return BatchChunkResult.committed(0, 0, 1, 1);
    }

    public static class PayloadEntity {
        @TableId(type = IdType.INPUT) public Long id = 1L;
        @Version public Long version = 1L;
        public Timestamp changedAt = new CountedTimestamp(123L);
        public byte[] payload;

        public PayloadEntity(int size) {
            payload = new byte[size];
            payload[0] = 41;
        }
    }

    public static final class ChangingPayloadEntity {
        @TableId(type = IdType.INPUT) public Long id = 1L;
        @Version public Long version = 1L;
        private byte[] payload = new byte[0];
        @TableField(exist = false)
        private int reads;

        public byte[] getPayload() {
            return ++reads == 2 ? new byte[8192] : payload;
        }
    }

    public static final class SharedPayloadEntity {
        @TableId(type = IdType.INPUT) public Long id = 1L;
        @Version public Long version = 1L;
        public List<String> payload = List.of("x".repeat(1024));
    }

    public static final class BufferEntity {
        @TableId(type = IdType.INPUT) public Long id = 1L;
        @Version public Long version = 1L;
        public ByteBuffer payload = ByteBuffer.allocate(8192).position(8191);
    }

    public static final class CountedTimestamp extends Timestamp {
        static final AtomicInteger COPIES = new AtomicInteger();

        public CountedTimestamp(long value) {
            super(value);
        }

        @Override
        public Object clone() {
            COPIES.incrementAndGet();
            return super.clone();
        }
    }
}
