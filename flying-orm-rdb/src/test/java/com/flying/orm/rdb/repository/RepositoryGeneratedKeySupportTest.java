package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryGeneratedKeySupportTest {

    @Test
    void bothWritersUseTheExistingSharedRuleOwners() throws Exception {
        assertDoesNotThrow(() -> RepositoryFailureSupport.class.getDeclaredMethod(
                "generatedKeyReadFailure", GeneratedKeyReadException.class));
        assertDoesNotThrow(() -> RepositoryEntityIdSupport.class.getDeclaredMethod(
                "assignGeneratedKey", Object.class, SqlWriteResult.class));
        for (String writer : new String[]{"ReactiveRepositoryEntityWriter", "SyncRepositoryEntityWriter"}) {
            String source = Files.readString(Path.of(
                    "src/main/java/com/flying/orm/rdb/repository/" + writer + ".java"));
            assertTrue(source.contains("RepositoryFailureSupport.generatedKeyReadFailure")
                    || source.contains("RepositoryFailureSupport::generatedKeyReadFailure"));
            assertTrue(source.contains("ids.assignGeneratedKey(entity, result)"));
        }
    }

    @Test
    void retainedRowsInheritAssignmentStateWithoutAnotherDelegate() {
        assertEquals(2, RepositoryEntityIdSupport.BatchState.class.getDeclaredFields().length,
                "batch state keeps only the entity and applied flag; failure recovery is invocation-local");
        for (Class<?> owner : new Class<?>[]{BatchLifecycleTracker.class, SyncRepositoryBatchLifecycle.class}) {
            Class<?> retained = Arrays.stream(owner.getDeclaredClasses())
                    .filter(type -> type.getSimpleName().equals("RetainedEntity")).findFirst().orElseThrow();
            assertEquals("BatchState", retained.getSuperclass().getSimpleName());
            assertSame(RepositoryEntityIdSupport.class, retained.getSuperclass().getEnclosingClass());
            assertTrue(Arrays.stream(retained.getDeclaredFields()).allMatch(field -> field.getType() == long.class),
                    "retained row owns only its distinct memory counters");
        }
    }

    @Test
    void bothBatchPathsRejectDuplicateDeliveryAndUnknownOffsets() {
        for (boolean reactive : new boolean[]{false, true}) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                Entity entity = new Entity();
                Batch batch = batch(models, entity, reactive);
                assertThrows(MappingException.class, () -> batch.keys().accept(1L, key(10L)));
                batch.keys().accept(0L, key(10L));
                assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(20L)));
                assertEquals(10L, entity.id);
                assertEquals(1, entity.writes);
                batch.complete().accept(0L);
                batch.abort().run();
                assertEquals(10L, entity.id);
            }
        }
    }

    @Test
    void bothBatchPathsRestoreOnlyTheAssignmentThatFailed() {
        for (boolean reactive : new boolean[]{false, true}) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                Entity entity = new Entity();
                entity.id = 3L;
                entity.reject = 10L;
                Batch batch = batch(models, entity, reactive);
                assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(10L)));
                assertEquals(3L, entity.id);
                assertEquals(2, entity.writes);
                assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(20L)));
                batch.abort().run();
                assertEquals(3L, entity.id);
            }
        }
    }

    @Test
    void bothBatchPathsPreserveSuccessfulAssignmentOnAbortBeforeCompletion() {
        for (boolean reactive : new boolean[]{false, true}) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
                Entity entity = new Entity();
                Batch batch = batch(models, entity, reactive);
                batch.keys().accept(0L, key(10L));
                batch.abort().run();
                assertEquals(10L, entity.id);
                assertEquals(1, entity.writes);
            }
        }
    }

    private static Batch batch(EntityModelRegistry models, Entity entity, boolean reactive) {
        var metadata = models.metadata(Entity.class);
        RepositoryEntityIdSupport<Entity> ids = RepositoryEntityIdSupport.create(metadata, models);
        if (reactive) {
            BatchLifecycleTracker<Entity> tracker = new BatchLifecycleTracker<>(
                    new EntityLifecycleDispatcher<>(metadata, null), EntityLifecyclePhase.POST_PERSIST,
                    ids, true, 1024L, (value, row) -> new BatchLifecycleTracker.RetainedMemory(1L, 1L));
            Flux.from(tracker.rows(Mono.just(entity), value -> value, EntityLifecyclePhase.PRE_PERSIST))
                    .blockLast();
            return new Batch(tracker.generatedKeys(),
                    offset -> tracker.rowCompleted(offset, EntityLifecyclePhase.POST_PERSIST).block(), tracker::abort);
        }
        SyncRepositoryBatchLifecycle<Entity> tracker = new SyncRepositoryBatchLifecycle<>(
                new SyncRepositoryLifecycleSupport<>(metadata, null, new SyncRepositoryAwaiter()),
                EntityLifecyclePhase.POST_PERSIST, models.entityValues(Entity.class), ids, true, 1024L);
        tracker.remember(0L, entity);
        return new Batch(tracker.generatedKeys(), tracker::rowCompleted, tracker::abort);
    }

    private static DynamicRow key(long value) {
        return DynamicRow.copyOf(Map.of("id", value));
    }

    private record Batch(BatchGeneratedKeys keys, LongConsumer complete, Runnable abort) {
    }

    @TableName("generated_key_entities")
    private static final class Entity {
        @TableId(type = IdType.AUTO)
        private Long id;
        private transient Long reject;
        private transient int writes;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
            writes++;
            if (id != null && id.equals(reject)) {
                throw new IllegalArgumentException("assignment failed after mutation");
            }
        }
    }
}
