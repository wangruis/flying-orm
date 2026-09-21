package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityMappingOwnerConvergenceTest {

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
