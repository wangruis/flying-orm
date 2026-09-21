package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LegacyCrudOnlyUnknownTypeBoundaryTest {

    private static final ValueCodec REFERENCE_CODEC = new ValueCodec() {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == ExternalReference.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((ExternalReference) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new ExternalReference(value.toString());
        }
    };

    @Test
    void requiresAnExplicitTypeAndCodecBeforeLegacyUnknownFieldsEnterSchemaApis() {
        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                .register("external-reference", ExternalReference.class,
                          DatabaseType.of("UUID"), REFERENCE_CODEC)
                .build();

        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            EntityMetadata<LegacyEntity> legacy = models.metadata(LegacyEntity.class);
            assertEquals(DatabaseType.of("VARCHAR"), legacy.field("reference").databaseType());
            assertThrows(MappingException.class, () -> models.schemaDescriptor(LegacyEntity.class));

            var descriptor = EntitySchemaDescriptor.builder(LegacyEntity.class)
                    .typeMappings(mappings)
                    .build();
            var column = descriptor.table().columns().getFirst();
            assertEquals(DatabaseType.of("UUID"), column.databaseType());
            assertEquals("external-reference", column.codecId());
            assertEquals(mappings.fingerprint(), descriptor.typeMappingsFingerprint());
        }
    }

    @TableName("legacy_entity")
    private static final class LegacyEntity {
        private ExternalReference reference;
    }

    private record ExternalReference(String value) {
    }
}
