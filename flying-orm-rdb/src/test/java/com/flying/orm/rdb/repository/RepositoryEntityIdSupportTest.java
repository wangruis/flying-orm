package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryEntityIdSupportTest {

    @Test
    void inheritedGenericGeneratedKeysUseTheConcreteJavaType() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            GenericGeneratedLong fieldEntity = new GenericGeneratedLong();
            RepositoryEntityIdSupport.create(models.metadata(GenericGeneratedLong.class), models)
                    .applyGeneratedKey(fieldEntity, DynamicRow.copyOf(Map.of("id", 42)));
            assertEquals(42L, (Object) fieldEntity.id);
            GenericGeneratedBeanLong beanEntity = new GenericGeneratedBeanLong();
            RepositoryEntityIdSupport.create(models.metadata(GenericGeneratedBeanLong.class), models)
                    .applyGeneratedKey(beanEntity, DynamicRow.copyOf(Map.of("id", 43)));
            assertEquals(43L, (Object) beanEntity.getId());
            assertEquals(1, beanEntity.assignments);
        }
    }

    static class GenericGeneratedBase<T> {
        @TableId(type = IdType.AUTO) public T id;
    }
    static class GenericGeneratedLong extends GenericGeneratedBase<Long> { }
    static class GenericGeneratedBean<T> {
        @TableId(type = IdType.AUTO) private T id;
        transient int assignments;
        public T getId() { return id; }
        public void setId(T id) { this.id = id; assignments++; }
    }
    static class GenericGeneratedBeanLong extends GenericGeneratedBean<Long> { }

    @Test
    void generatedKeySelectionPreservesSingleColumnFallbackAndMultiColumnIdentity() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<AcronymGeneratedId> ids = RepositoryEntityIdSupport.create(
                    models.metadata(AcronymGeneratedId.class), models);
            for (Map<String, Long> row : List.of(Map.of("GENERATED_KEY", 42L), Map.of("ID", 42L),
                    Map.of("unrelated", 7L, "I_D", 42L))) {
                AcronymGeneratedId entity = new AcronymGeneratedId();
                ids.applyGeneratedKey(entity, DynamicRow.copyOf(row));
                assertEquals(42L, entity.getID());
                assertEquals(1, entity.setterCalls);
            }
            for (Map<String, ?> row : List.<Map<String, ?>>of(Map.of("id", 41L, "I_D", 42L),
                    Map.of("first", 41L, "second", 42L), Collections.singletonMap("GENERATED_KEY", null), Map.of())) {
                AcronymGeneratedId entity = new AcronymGeneratedId();
                assertThrows(MappingException.class, () -> ids.applyGeneratedKey(entity, DynamicRow.copyOf(row)));
                assertNull(entity.getID());
                assertEquals(0, entity.setterCalls);
            }
        }
    }

    @Test
    void inputKeyUsesCanonicalBeanPropertyLikeOrdinaryEntityValues() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<AcronymInputId> ids = RepositoryEntityIdSupport.create(
                    models.metadata(AcronymInputId.class), models);
            AcronymInputId supplied = new AcronymInputId();
            assertEquals("normalized", models.entityValues(AcronymInputId.class)
                    .readForInsert(supplied).get("id"));
            assertDoesNotThrow(() -> ids.prepare(supplied));
            supplied.id = " ";
            assertNull(models.entityValues(AcronymInputId.class).readForInsert(supplied).get("id"));
            assertThrows(MappingException.class, () -> ids.prepare(supplied));
        }
    }

    @Test
    void generatedKeyUsesCanonicalBeanSetterAndReader() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<AcronymGeneratedId> ids = RepositoryEntityIdSupport.create(
                    models.metadata(AcronymGeneratedId.class), models);
            AcronymGeneratedId entity = new AcronymGeneratedId();
            ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 42L)));
            assertEquals(1, entity.setterCalls);
            assertEquals(42L, entity.getID());
            assertThrows(MappingException.class,
                    () -> ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 43L))));
            assertEquals(2, entity.setterCalls, "a failed user setter must not be called again for compensation");
            assertEquals(43L, entity.getID());
        }
    }

    @Test
    void canonicalBeanAliasesUseTheSamePrecedenceAsOrdinaryMapping() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<AliasedBeanId> ids = RepositoryEntityIdSupport.create(
                    models.metadata(AliasedBeanId.class), models);
            AliasedBeanId entity = new AliasedBeanId();
            ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 42L)));
            assertEquals("id", entity.setter);
            assertEquals(42L, models.entityValues(AliasedBeanId.class).readIdentity(entity).get("id"));
            assertEquals(42L, entity.getId());
        }
    }

    @Test
    void recordInputKeyValidationUsesTheSameAccessorAsEntityWrites() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<NormalizedInputId> ids = RepositoryEntityIdSupport.create(
                    models.metadata(NormalizedInputId.class), models);
            NormalizedInputId supplied = new NormalizedInputId(null);
            assertEquals("fallback", models.entityValues(NormalizedInputId.class)
                    .readForInsert(supplied).get("id"));
            assertDoesNotThrow(() -> ids.prepare(supplied));

            NormalizedInputId missing = new NormalizedInputId(" ");
            assertNull(models.entityValues(NormalizedInputId.class).readForInsert(missing).get("id"));
            assertThrows(MappingException.class, () -> ids.prepare(missing));
        }
    }

    @Test
    void generatedKeyUsesPersistentParentInsteadOfTransientChildField() {
        ShadowedGeneratedId entity = new ShadowedGeneratedId();

        assertInheritedGeneratedKey(ShadowedGeneratedId.class, entity, () -> entity.id);
    }

    @Test
    void generatedKeyIgnoresAccessorsOfAnExcludedChildProperty() {
        ShadowedGeneratedPropertyId entity = new ShadowedGeneratedPropertyId();

        assertInheritedGeneratedKey(ShadowedGeneratedPropertyId.class, entity, () -> entity.id);
    }

    private static <T extends GeneratedParent> void assertInheritedGeneratedKey(
            Class<T> type, T entity, Supplier<Long> excludedId) {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<T> ids = RepositoryEntityIdSupport.create(models.metadata(type), models);
            assertAll(
                    () -> assertDoesNotThrow(() -> ids.prepare(entity)),
                    () -> assertNull(((GeneratedParent) entity).id),
                    () -> {
                        ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 42L)));
                        assertAll(
                                () -> assertEquals(42L, ((GeneratedParent) entity).id),
                                () -> assertEquals(99L, excludedId.get()));
                    });
        }
    }

    @Test
    void usesDescriptorCodecWhenWritingBackDatabaseGeneratedKey() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("generated-id", GeneratedId.class, DatabaseType.of("VARCHAR"), new GeneratedIdCodec())
                .build();
        EntitySchemaDescriptor<CustomGeneratedId> descriptor =
                EntitySchemaDescriptor.builder(CustomGeneratedId.class)
                        .typeMappings(mappings)
                        .build();
        RepositoryEntityIdSupport<CustomGeneratedId> ids = RepositoryEntityIdSupport.create(
                descriptor.metadata(),
                IdGenerator.none(),
                descriptor.metadata().field("id").customMapping());
        CustomGeneratedId entity = new CustomGeneratedId();

        ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 42L)));

        assertEquals(new GeneratedId("db-42"), entity.id);
    }

    @Test
    void registeredModelFactoryKeepsCustomGeneratedKeyCodecForRenamedColumns() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("generated-id", GeneratedId.class, DatabaseType.of("VARCHAR"), new GeneratedIdCodec())
                .build();
        EntitySchemaDescriptor<RenamedCustomGeneratedId> descriptor =
                EntitySchemaDescriptor.builder(RenamedCustomGeneratedId.class)
                        .typeMappings(mappings)
                        .build();
        try (EntityModelRegistry models = EntityModelRegistry.create(
                CacheRegionPolicy.disabled(), IdGenerator.none(),
                com.flying.orm.rdb.mapping.EntityFieldFiller.none(),
                Map.of(RenamedCustomGeneratedId.class, descriptor))) {
            RepositoryEntityIdSupport<RenamedCustomGeneratedId> ids =
                    RepositoryEntityIdSupport.create(descriptor.metadata(), models);
            RenamedCustomGeneratedId entity = new RenamedCustomGeneratedId();

            ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("generated_id", 42L)));

            assertEquals(new GeneratedId("db-42"), entity.id);
        }
    }

    @Test
    void recognizesDatabaseGeneratedFieldAfterAnInputCompositeKeyField() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<CompositeGeneratedId> metadata = models.metadata(CompositeGeneratedId.class);
            RepositoryEntityIdSupport<CompositeGeneratedId> ids = RepositoryEntityIdSupport.create(
                    metadata, models.idGenerator());
            CompositeGeneratedId entity = new CompositeGeneratedId();
            entity.tenantId = "tenant-a";

            ids.prepare(entity);
            assertTrue(ids.databaseGenerated());
            assertEquals("id", ids.generatedKeyColumn());

            ids.applyGeneratedKey(entity, DynamicRow.copyOf(Map.of("id", 42L)));
            assertEquals(42L, entity.id);
        }
    }

    @Test
    void doesNotPartiallyAssignACompositeKeyWhenAnotherPartIsInvalid() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<CompositeAssignedId> metadata = models.metadata(CompositeAssignedId.class);
            RepositoryEntityIdSupport<CompositeAssignedId> ids = RepositoryEntityIdSupport.create(
                    metadata, models.idGenerator());
            CompositeAssignedId entity = new CompositeAssignedId();

            assertThrows(MappingException.class, () -> ids.prepare(entity));
            assertNull(entity.generatedId);
            entity.ownerId = "owner";
            ids.prepare(entity);
            String assigned = entity.generatedId;
            assertEquals(32, assigned.length());
            ids.prepare(entity);
            assertEquals(assigned, entity.generatedId, "already assigned keys must not be generated again");
        }
    }

    @Test
    void doesNotMineGeneratorRuntimeWrappersButStillPropagatesDirectFatalErrors() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<AssignedId> metadata = models.metadata(AssignedId.class);
            SyntheticVirtualMachineError nestedFatal = new SyntheticVirtualMachineError();
            CompletionException wrapped = new CompletionException(nestedFatal);
            IdGenerator wrappedFailure = (entityType, propertyName, targetType) -> {
                throw wrapped;
            };
            RepositoryEntityIdSupport<AssignedId> wrappedIds = RepositoryEntityIdSupport.create(
                    metadata, wrappedFailure);

            MappingException failure = assertThrows(MappingException.class,
                    () -> wrappedIds.prepare(new AssignedId()));
            assertSame(wrapped, failure.getCause());

            SyntheticVirtualMachineError directFatal = new SyntheticVirtualMachineError();
            IdGenerator directFailure = (entityType, propertyName, targetType) -> {
                throw directFatal;
            };
            RepositoryEntityIdSupport<AssignedId> directIds = RepositoryEntityIdSupport.create(
                    metadata, directFailure);

            assertSame(directFatal, assertThrows(
                    SyntheticVirtualMachineError.class, () -> directIds.prepare(new AssignedId())));
        }
    }

    @TableName("composite_generated_id")
    private static final class CompositeGeneratedId {
        @TableId(type = IdType.INPUT)
        private String tenantId;
        @TableId(type = IdType.AUTO)
        private Long id;
    }

    private static final class AcronymInputId {
        @TableId(type = IdType.INPUT)
        private String id;

        public String getID() { return id == null ? "normalized" : id.isBlank() ? null : id.trim(); }
        public void setId(String value) { id = value; }
    }

    private static final class AcronymGeneratedId {
        @TableId(type = IdType.AUTO)
        private Long id;
        private transient int setterCalls;

        public Long getID() { return id; }
        public void setID(Long value) {
            setterCalls++;
            id = value;
            if (Long.valueOf(43L).equals(value)) throw new IllegalStateException("setter failed after assigning");
        }
    }

    private static final class AliasedBeanId {
        @TableId(type = IdType.AUTO)
        private Long id;
        private transient String setter;

        public Long getID() { return -1L; }
        public void setID(Long value) { setter = "ID"; id = value; }
        public Long getId() { return id; }
        public void setId(Long value) { setter = "id"; id = value; }
    }

    private static class GeneratedParent {

        @TableId(type = IdType.AUTO)
        private Long id;
    }

    private static final class ShadowedGeneratedId extends GeneratedParent {

        private transient Long id = 99L;
    }

    private static final class ShadowedGeneratedPropertyId extends GeneratedParent {

        @TableField(exist = false)
        private Long id = 99L;

        public Long getId() {
            return id;
        }

        public void setId(Long value) {
            id = value;
        }
    }

    @TableName("composite_assigned_id")
    private static final class CompositeAssignedId {
        @TableId(type = IdType.ASSIGN_UUID)
        private String generatedId;
        @TableId(type = IdType.INPUT)
        private String ownerId;
    }

    @TableName("assigned_id")
    private static final class AssignedId {
        @TableId(type = IdType.ASSIGN_ID)
        private Long id;
    }

    @TableName("custom_generated_id")
    private static final class CustomGeneratedId {

        @TableId(type = IdType.AUTO)
        @TableColumn(databaseTypeId = "generated-id")
        private GeneratedId id;
    }

    @TableName("renamed_custom_generated_id")
    private static final class RenamedCustomGeneratedId {

        @TableId(type = IdType.AUTO)
        @TableField("generated_id")
        @TableColumn(databaseTypeId = "generated-id")
        private GeneratedId id;
    }

    @TableName("normalized_input_id")
    private record NormalizedInputId(@TableId(type = IdType.INPUT) String id) {
        public String id() {
            return id == null ? "fallback" : id.isBlank() ? null : id.trim();
        }
    }

    private record GeneratedId(String value) {
    }

    private static final class GeneratedIdCodec implements ValueCodec {

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == GeneratedId.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((GeneratedId) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new GeneratedId("db-" + value);
        }
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
        private static final long serialVersionUID = 1L;
    }
}
