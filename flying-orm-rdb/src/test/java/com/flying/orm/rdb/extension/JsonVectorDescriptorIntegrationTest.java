package com.flying.orm.rdb.extension;

import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.json.JsonTermHandlers;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.vector.VectorTermHandlers;
import com.flying.orm.rdb.vector.VectorValueCodec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonVectorDescriptorIntegrationTest {

    @Test
    void jsonAndVectorPublishNarrowDescriptorsOnTheirExistingSeams() {
        JsonTermHandlers.mysql().handlers().forEach(handler -> {
            var descriptor = handler.descriptor().orElseThrow();
            assertEquals(handler.id(), descriptor.id());
            assertEquals(FieldUse.FILTER, descriptor.fieldUse());
            assertTrue(descriptor.requiredCapabilities().contains(
                    DialectCapabilityId.JSON_FUNCTIONS.value()));
        });
        VectorTermHandlers.postgresql().handlers().forEach(handler -> {
            var descriptor = handler.descriptor().orElseThrow();
            assertEquals(handler.id(), descriptor.id());
            assertTrue(descriptor.requiredCapabilities().contains(
                    DialectCapabilityId.POSTGRESQL_VECTOR.value()));
        });

        assertTrue(JsonValueCodec.descriptor().supportsLogicalType(LogicalType.JSON));
        assertTrue(JsonValueCodec.descriptor().supportsJavaType(java.util.LinkedHashMap.class));
        assertTrue(VectorValueCodec.descriptor().supportsLogicalType(LogicalType.VECTOR));
        assertTrue(VectorValueCodec.descriptor().supportsJavaType(float[].class));
    }
}
