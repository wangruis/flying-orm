package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityUnknownTypeStrictSchemaTest {

    @Test
    void keepsLegacyCrudFallbackButRejectsTheSameUnknownTypeFromSchemaCompilation() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<UnknownEntity> legacy = models.metadata(UnknownEntity.class);
            assertEquals(DatabaseType.of("VARCHAR"), legacy.field("payload").databaseType());

            MappingException failure = assertThrows(
                    MappingException.class,
                    () -> models.schemaDescriptor(UnknownEntity.class));
            assertUnknownTypeMessage(failure);
        }
    }

    @Test
    void directDescriptorBuilderUsesTheSameStrictBoundary() {
        MappingException failure = assertThrows(
                MappingException.class,
                () -> EntitySchemaDescriptor.builder(UnknownEntity.class).build());

        assertUnknownTypeMessage(failure);
    }

    private static void assertUnknownTypeMessage(MappingException failure) {
        assertTrue(failure.getMessage().contains("payload"));
        assertTrue(failure.getMessage().contains(UnknownValue.class.getTypeName()));
    }

    @TableName("unknown_entity")
    private static final class UnknownEntity {
        private UnknownValue payload;
    }

    private record UnknownValue(String value) {
    }
}
