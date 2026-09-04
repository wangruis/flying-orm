package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class EntityTypeMappingRegistryCacheIdentityTest {

    private static final ValueCodec MONEY_CODEC = new ValueCodec() {
        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == Money.class;
        }

        @Override
        public Object write(Object value) {
            return value == null ? null : ((Money) value).minorUnits();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value == null ? null : new Money(((Number) value).longValue());
        }
    };

    @Test
    void keysSchemaDescriptorsByRegistryIdentityWithoutChangingStableFingerprints() {
        EntityTypeMappingRegistry firstMappings = mappings();
        EntityTypeMappingRegistry equalButDistinctMappings = mappings();

        assertNotSame(firstMappings, equalButDistinctMappings);
        assertEquals(firstMappings.fingerprint(), equalButDistinctMappings.fingerprint());

        try (EntityModelRegistry models = EntityModelRegistry.create(
                CacheRegionPolicy.entityMappingDefaults())) {
            var first = models.schemaDescriptor(InvoiceEntity.class, firstMappings);
            var firstAgain = models.schemaDescriptor(InvoiceEntity.class, firstMappings);
            var second = models.schemaDescriptor(InvoiceEntity.class, equalButDistinctMappings);

            assertSame(first, firstAgain);
            assertNotSame(first, second);
            assertEquals(first.typeMappingsFingerprint(), second.typeMappingsFingerprint());
            assertEquals(RelationalMetadataFingerprint.of(first.table()),
                         RelationalMetadataFingerprint.of(second.table()));
        }
    }

    private static EntityTypeMappingRegistry mappings() {
        return EntityTypeMappingRegistry.builder()
                .register("money-minor-units", Money.class, DatabaseType.of("BIGINT"), MONEY_CODEC)
                .build();
    }

    @TableName("invoice")
    private static final class InvoiceEntity {
        private Money total;
    }

    private record Money(long minorUnits) {
    }
}
