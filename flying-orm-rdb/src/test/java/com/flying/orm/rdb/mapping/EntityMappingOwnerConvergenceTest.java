package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EntityMappingOwnerConvergenceTest {

    @Test
    void registeredModelIsTheCompilationInputForAllConsumersWithoutRecompilingItsMetadata() {
        EntitySchemaDescriptor<AnnotatedKeys> declared = EntitySchemaDescriptor.builder(AnnotatedKeys.class).build();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults(),
                com.flying.orm.rdb.id.IdGenerator.none(), EntityFieldFiller.none(),
                Map.of(AnnotatedKeys.class, declared))) {
            for (int iteration = 0; iteration < 2; iteration++) {
                long loads = models.stats().loadSuccessCount();
                assertSame(declared.metadata(), models.metadata(AnnotatedKeys.class));
                assertSame(declared, models.schemaDescriptor(AnnotatedKeys.class));
                assertSame(declared.form(), models.metadata(AnnotatedKeys.class).toDynamicForm());
                assertEquals(loads, models.stats().loadSuccessCount());
                models.rowMapper(AnnotatedKeys.class, ValueCodecRegistry.standard());
                models.entityValues(AnnotatedKeys.class);
                assertEquals(loads + 2, models.stats().loadSuccessCount(), "only read/write plans need compilation");
                assertSame(declared, models.schemaDescriptor(AnnotatedKeys.class));
                models.invalidateAll();
            }
        }
    }

    @Test
    void annotatedKeysHaveOneMeaningBeforeAndAfterSchemaAccess() {
        for (boolean schemaFirst : List.of(false, true)) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                if (schemaFirst) models.schemaDescriptor(AnnotatedKeys.class);
                EntityMetadata<AnnotatedKeys> metadata = models.metadata(AnnotatedKeys.class);
                assertTrue(metadata.field("email").unique(), "inherited single-column constraint applies to CRUD");
                assertTrue(metadata.field("login").unique(), "single-column unique index applies to CRUD");
                assertFalse(metadata.field("tenant").unique(), "a composite key does not make each column unique");
                assertFalse(metadata.field("alias").unique());
                EntitySchemaDescriptor<AnnotatedKeys> schema = models.schemaDescriptor(AnnotatedKeys.class);
                assertSame(metadata, schema.metadata());
                assertEquals(List.of("email_address"), schema.table().uniqueConstraints().getFirst().columns());
                assertEquals(List.of(IndexKeyPart.desc("login")), schema.table().indexes().getFirst().keys());
                assertSame(metadata.toDynamicForm(), schema.form());
            }
        }
    }

    @TableUnique(id = "uq_email", properties = "email")
    private static class AnnotatedKeyBase {
        @TableField("email_address")
        private String email;
    }

    @TableUnique(id = "uq_tenant_alias", properties = {"tenant", "alias"})
    @TableIndex(id = "uq_login", unique = true,
            columns = @TableIndexColumn(property = "login", direction = TableIndexColumn.Direction.DESC))
    private static final class AnnotatedKeys extends AnnotatedKeyBase {
        @TableId
        private Long id;
        private String login;
        private String tenant;
        private String alias;
    }

    @Test
    void crudAndSchemaReuseTheSameCompiledColumnsInEitherAccessOrder() {
        for (boolean schemaFirst : List.of(false, true)) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                if (schemaFirst) models.schemaDescriptor(NativeUuidEntity.class);
                EntityMetadata<NativeUuidEntity> metadata = models.metadata(NativeUuidEntity.class);
                EntitySchemaDescriptor<NativeUuidEntity> schema = models.schemaDescriptor(NativeUuidEntity.class);
                assertSame(metadata, schema.metadata());
                for (EntityFieldMetadata field : metadata.fields()) {
                    assertSame(field.field().column(), schema.table().column(field.columnName()));
                }
                assertSame(metadata, models.metadata(NativeUuidEntity.class));
                assertSame(schema, models.schemaDescriptor(NativeUuidEntity.class));
            }
        }
    }

    @Test
    void strictSchemaFailureDoesNotReplaceTheUsableCrudModel() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            EntityMetadata<CustomValueEntity> metadata = models.metadata(CustomValueEntity.class);
            assertEquals("VARCHAR", metadata.field("value").databaseType().baseName());
            assertThrows(MappingException.class, () -> models.schemaDescriptor(CustomValueEntity.class));
            assertSame(metadata, models.metadata(CustomValueEntity.class));
        }
    }

    private static final class CustomValueEntity {
        private Thread value;
    }

    @Test
    void cachedReadAndWritePlansDoNotLookUpTheirMetadataAgain() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        List<Function<EntityModelRegistry, Object>> plans = List.of(
                models -> models.rowMapper(NativeUuidEntity.class, codecs),
                models -> models.decodedRowMapper(NativeUuidEntity.class, codecs),
                models -> models.rawRowMapper(NativeUuidEntity.class, codecs),
                models -> models.entityValues(NativeUuidEntity.class));
        for (Function<EntityModelRegistry, Object> plan : plans) {
            try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                Object first = plan.apply(models);
                long requests = models.stats().requestCount();
                long loads = models.stats().loadSuccessCount();
                assertSame(first, plan.apply(models));
                assertEquals(1, models.stats().requestCount() - requests,
                        "a cached plan must not revisit its metadata dependency");
                assertEquals(loads, models.stats().loadSuccessCount());
                models.invalidateAll();
                assertNotSame(first, plan.apply(models));
                assertEquals(2, models.estimatedMappings(), "metadata and plan share the same cache region");
            }
        }
    }

    @Test
    void planReuseStillHonorsCodecIdentityAndCacheLifecycle() {
        ValueCodecRegistry firstCodecs = ValueCodecRegistry.standard();
        ValueCodecRegistry secondCodecs = new ValueCodecRegistry(List.of());
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            RowMapper<NativeUuidEntity> mapper = models.rowMapper(NativeUuidEntity.class, firstCodecs);
            assertNotSame(mapper, models.rowMapper(NativeUuidEntity.class, secondCodecs));
            UUID id = UUID.randomUUID();
            assertEquals(id, mapper.map(Map.of("id", id)).id);
            models.close();
            assertNotSame(mapper, models.rowMapper(NativeUuidEntity.class, firstCodecs));
            assertNotSame(models.entityValues(NativeUuidEntity.class), models.entityValues(NativeUuidEntity.class));
            assertEquals(0, models.estimatedMappings());
        }
        for (CacheRegionPolicy policy : List.of(CacheRegionPolicy.disabled(),
                new CacheRegionPolicy(true, 64, 1, Duration.ofMinutes(1), true))) {
            try (EntityModelRegistry models = EntityModelRegistry.create(policy)) {
                assertNotSame(models.rowMapper(NativeUuidEntity.class, firstCodecs),
                        models.rowMapper(NativeUuidEntity.class, firstCodecs));
                assertNotSame(models.entityValues(NativeUuidEntity.class), models.entityValues(NativeUuidEntity.class));
                assertEquals(0, models.estimatedMappings());
            }
        }
    }

    @Test
    void concurrentColdRequestsStillShareAtomicPlanLoads() throws Exception {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults());
             java.util.concurrent.ExecutorService workers = Executors.newFixedThreadPool(4)) {
            Callable<List<Object>> request = () -> List.of(
                    models.rowMapper(NativeUuidEntity.class, ValueCodecRegistry.standard()),
                    models.entityValues(NativeUuidEntity.class));
            List<Future<List<Object>>> results = workers.invokeAll(java.util.Collections.nCopies(32, request));
            List<Object> first = results.getFirst().get();
            for (Future<List<Object>> result : results) {
                assertSame(first.get(0), result.get().get(0));
                assertSame(first.get(1), result.get().get(1));
            }
            assertEquals(3, models.stats().loadSuccessCount(), "one metadata, one read plan and one write plan");
        }
    }

    @Test
    void fieldGenerationHasNoSecondResolver() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.internal.mapping.EntityValueGenerationResolver"));
    }

    @Test
    void standardRegistryHasNoSeparateInitializationTable() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.mapping.EntityStandardTypeMappings"));
    }

    @Test
    void compilesSequenceBeforeIdentityAndLeavesApplicationIdsToTheCaller() {
        assertEquals(ValueGeneration.sequence("entity_seq"),
                EntityMetadataResolver.createUncached(SequenceEntity.class).field("id").generation());
        assertEquals(ValueGeneration.none(),
                EntityMetadataResolver.createUncached(SequenceEntity.class).field("value").generation());
        assertEquals(ValueGeneration.identity(),
                EntityMetadataResolver.createUncached(IdentityEntity.class).field("id").generation());
        EntityFieldMetadata assigned = EntityMetadataResolver.createUncached(AssignedEntity.class).field("id");
        assertEquals(ValueGeneration.none(), assigned.generation());
        assertEquals(IdType.ASSIGN_UUID, assigned.idType());
    }

    @Test
    void standardMappingsKeepSharedCodecsAndTheBuilderFingerprint() {
        EntityTypeMappingRegistry standard = EntityTypeMappingRegistry.standard();

        assertSame(standard, EntityTypeMappingRegistry.standard());
        assertEquals(standard.fingerprint(), EntityTypeMappingRegistry.builder().build().fingerprint());
        assertEquals("BIGINT", standard.resolve(long.class).id());
        assertSame(standard.resolve(Long.class).codec(), standard.resolve(String.class).codec());
        assertEquals("JSON", standard.resolve(Map.class).id());
        assertSame(standard.resolve(Map.class).codec(), standard.resolve(Collection.class).codec());
    }

    @Test
    void standardRegistryCompilesUuidFieldsWithoutCustomRegistration() {
        EntitySchemaDescriptor<NativeUuidEntity> descriptor =
                EntitySchemaDescriptor.builder(NativeUuidEntity.class).build();

        assertEquals(2L, descriptor.table().columns().stream()
                .filter(column -> "UUID".equals(column.databaseType().baseName()))
                .count());
        assertEquals("UUID", descriptor.typeMappings().resolve("UUID", UUID.class).id());
    }

    @KeySequence(" entity_seq ")
    private static final class SequenceEntity {
        @TableId(type = IdType.AUTO)
        private Long id;
        private String value;
    }

    @KeySequence(" ")
    private static final class IdentityEntity {
        @TableId(type = IdType.AUTO)
        private Long id;
    }

    private static final class AssignedEntity {
        @TableId(type = IdType.ASSIGN_UUID)
        private String id;
    }

    @TableName("native_uuid_entities")
    private static final class NativeUuidEntity {
        @TableId
        private UUID id;
        private UUID ownerId;
    }
}
