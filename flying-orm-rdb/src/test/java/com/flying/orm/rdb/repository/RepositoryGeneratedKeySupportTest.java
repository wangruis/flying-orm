package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
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
        assertThrows(NoSuchMethodException.class, () -> RepositoryEntityIdSupport.class.getDeclaredMethod(
                "currentGeneratedKey", Object.class));
        assertThrows(NoSuchMethodException.class, () -> RepositoryEntityIdSupport.class.getDeclaredMethod(
                "restoreGeneratedKey", Object.class, Object.class));
        assertDoesNotThrow(() -> RepositoryFailureSupport.class.getDeclaredMethod(
                "generatedKeyReadFailure", GeneratedKeyReadException.class));
        assertDoesNotThrow(() -> RepositoryEntityIdSupport.class.getDeclaredMethod(
                "assignGeneratedKey", Object.class, SqlWriteResult.class));
        for (String writer : new String[]{"ReactiveFormRepository", "SyncFormRepository"}) {
            String source = Files.readString(Path.of(
                    "src/main/java/com/flying/orm/rdb/repository/" + writer + ".java"));
            assertTrue(source.contains("RepositoryFailureSupport.generatedKeyReadFailure")
                    || source.contains("RepositoryFailureSupport::generatedKeyReadFailure"));
            assertTrue(source.contains("ids.assignGeneratedKey(entity, result)"));
        }
    }


    @Test
    void sharedBatchStateRejectsDuplicateDeliveryAndUnknownOffsets() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            Entity entity = new Entity();
            Batch batch = batch(models, entity);
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

    @Test
    void failedSetterIsNotRetriedOrCompensated() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            Entity entity = new Entity();
            entity.reject = 10L;
            Batch batch = batch(models, entity);
            entity.id = 3L;
            assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(10L)));
            assertEquals(10L, entity.id);
            assertEquals(1, entity.writes);
            assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(20L)));
            batch.abort().run();
            assertEquals(10L, entity.id);
            assertEquals(1, entity.writes);
        }
    }

    @Test
    void sharedBatchStatePreservesKnownIdentityWithoutAcceptingGeneratedKeys() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            Entity entity = new Entity();
            entity.id = 3L;
            Batch batch = batch(models, entity);
            assertThrows(MappingException.class, () -> batch.keys().accept(0L, key(10L)));
            batch.complete().accept(0L);
            assertEquals(3L, entity.id);
            assertEquals(0, entity.writes);
        }
    }

    @Test
    void sharedBatchStatePreservesSuccessfulAssignmentOnAbortBeforeCompletion() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            Entity entity = new Entity();
            Batch batch = batch(models, entity);
            batch.keys().accept(0L, key(10L));
            batch.abort().run();
            assertEquals(10L, entity.id);
            assertEquals(1, entity.writes);
        }
    }

    private static Batch batch(EntityModelRegistry models, Entity entity) {
        var metadata = models.metadata(Entity.class);
        RepositoryEntityIdSupport<Entity> ids = RepositoryEntityIdSupport.create(metadata, models);
        RepositoryBatchRetention<Entity> retained = new RepositoryBatchRetention<>(
                ids, true, 1024L, "retainedBytes", (value, row) -> 1L);
        retained.remember(0L, entity, entity);
        return new Batch(retained.generatedKeys(), retained::complete, retained::clear);
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
