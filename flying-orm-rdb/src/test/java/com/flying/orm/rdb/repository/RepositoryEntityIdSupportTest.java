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
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryEntityIdSupportTest {

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
                descriptor.customFieldCodecs().get(descriptor.form().field("id")));
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
