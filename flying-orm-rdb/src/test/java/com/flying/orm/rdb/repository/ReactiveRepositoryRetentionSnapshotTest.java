package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.core.scope.DataScope;
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
import java.util.function.Consumer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveRepositoryRetentionSnapshotTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    @Test
    void updateEstimatesOwnedValuesWithoutAdditionalCopies() {
        for (int size : new int[]{4096, 1_048_576}) {
            int untrackedCopies = -1;
            for (boolean tracked : new boolean[]{false, true}) {
                try (EntityModelRegistry models = models()) {
                    AtomicInteger postUpdates = new AtomicInteger();
                    PayloadEntity entity = new PayloadEntity(size);
                    AtomicInteger copiesAtExecution = new AtomicInteger();
                    ReactiveRepositoryBatchCoordinator<PayloadEntity> coordinator = coordinator(
                            models, PayloadEntity.class, tracked, postUpdates, row -> {
                                copiesAtExecution.set(CountedTimestamp.COPIES.get());
                                byte[] payload = (byte[]) java.util.Arrays.stream(row)
                                        .filter(byte[].class::isInstance).findFirst().orElseThrow();
                                assertEquals(41, payload[0]);
                                payload[0] = 99;
                                assertEquals(41, entity.payload[0], "bound row must not alias the entity");
                            });
                    CountedTimestamp.COPIES.set(0);
                    BatchExecutionEvidence result = coordinator.write(Flux.just(entity), options(4_194_304L),
                            DataScope.none(), RepositoryBatchKind.UPDATE, null).block(TIMEOUT);
                    assertNotNull(result);
                    assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
                    org.junit.jupiter.api.Assertions.assertTrue(copiesAtExecution.get() > 0,
                            "mutable values must be snapshotted before execution");
                    if (!tracked) untrackedCopies = copiesAtExecution.get();
                    else assertEquals(untrackedCopies, copiesAtExecution.get(),
                            "retaining and estimating the entity must not add snapshot copies");
                    assertEquals(tracked ? 1 : 0, postUpdates.get());
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
                    () -> coordinator.write(Flux.just(new PayloadEntity(4096)), options(6000),
                            DataScope.none(), RepositoryBatchKind.UPDATE, null).block(TIMEOUT));
            assertEquals("combinedRetainedBytes", error.limitName());
            assertEquals(0, postUpdates.get());
        }
    }

    @Test
    void rowBudgetUsesTheMappedSnapshotWhenALaterGetterReturnsASmallerValue() {
        try (EntityModelRegistry models = models()) {
            var coordinator = coordinator(models, ChangingPayloadEntity.class, true, new AtomicInteger());
            BatchMemoryLimitExceededException error = assertThrows(BatchMemoryLimitExceededException.class,
                    () -> coordinator.write(Flux.just(new ChangingPayloadEntity()), options(4096),
                            DataScope.none(), RepositoryBatchKind.UPDATE, null).block(TIMEOUT));
            assertEquals("combinedRetainedBytes", error.limitName());
        }
    }

    @Test
    void combinedBudgetCountsSharedCollectionsOnce() {
        try (EntityModelRegistry models = models()) {
            AtomicInteger postUpdates = new AtomicInteger();
            var coordinator = coordinator(models, SharedPayloadEntity.class, true, postUpdates);
            BatchExecutionEvidence result = coordinator.write(Flux.just(new SharedPayloadEntity()), options(1600),
                    DataScope.none(), RepositoryBatchKind.UPDATE, null).block(TIMEOUT);
            assertNotNull(result);
            assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
            assertEquals(1, postUpdates.get());
        }
    }

    @Test
    void entityBudgetStillCountsBufferCapacityBeyondItsReadableWindow() {
        try (EntityModelRegistry models = models()) {
            var coordinator = coordinator(models, BufferEntity.class, true, new AtomicInteger());
            BatchMemoryLimitExceededException error = assertThrows(BatchMemoryLimitExceededException.class,
                    () -> coordinator.write(Flux.just(new BufferEntity()), options(6000),
                            DataScope.none(), RepositoryBatchKind.UPDATE, null).block(TIMEOUT));
            assertEquals("combinedRetainedBytes", error.limitName());
        }
    }

    private static <T> ReactiveRepositoryBatchCoordinator<T> coordinator(
            EntityModelRegistry models, Class<T> type, boolean tracked, AtomicInteger postUpdates) {
        return coordinator(models, type, tracked, postUpdates, row -> { });
    }

    private static <T> ReactiveRepositoryBatchCoordinator<T> coordinator(
            EntityModelRegistry models, Class<T> type, boolean tracked, AtomicInteger postUpdates,
            Consumer<Object[]> received) {
        com.flying.orm.rdb.mapping.EntityMetadata<T> metadata = models.metadata(type);
        ReactiveEntityListener<T> listener = tracked ? event -> {
            if (event.phase() == EntityLifecyclePhase.POST_UPDATE) postUpdates.incrementAndGet();
            return Mono.empty();
        } : null;
        ReactiveFormClient client = BatchEvidenceFixtures.batchFormClient((request, completed) ->
                Flux.from(request.rows()).index().concatMap(indexed -> {
                    received.accept(indexed.getT2());
                    return completed == null ? Mono.empty() : Mono.from(completed.apply(indexed.getT1()));
                }).then(Mono.fromSupplier(() -> BatchEvidenceFixtures.successful(1))),
                com.flying.orm.core.codec.ValueCodecRegistry.standard().withFirst(new com.flying.orm.core.codec.ValueCodec() {
                    public boolean supports(Class<?> type) { return type == CountedTimestamp.class; }
                    public Object write(Object value) { return ((Timestamp) value).toLocalDateTime(); }
                    public Object read(Object value, Class<?> type) { throw new UnsupportedOperationException(); }
                }));
        return new ReactiveRepositoryBatchCoordinator<>(client, metadata.toDynamicForm(),
                metadata, models.entityValues(type), new EntityLifecycleDispatcher<>(metadata, listener),
                RepositoryEntityIdSupport.create(metadata, models));
    }

    private static EntityModelRegistry models() {
        return EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
    }

    private static BatchWriteOptions options(long bytes) {
        return BatchWriteOptions.of(1).withMemoryLimits(1, bytes);
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
            return ++reads == 1 ? new byte[8192] : payload;
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
