package com.flying.orm.core.codec;

import com.flying.orm.core.type.LogicalType;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValueCodecDescriptorTest {

    @Test
    void freezesJavaLogicalTypeAndCapabilityContract() {
        ValueCodecDescriptor descriptor = ValueCodecDescriptor.of(
                "json-value", Set.of(java.util.Map.class, java.util.List.class),
                Set.of(LogicalType.JSON), Set.of("native-json"));

        assertEquals("json-value", descriptor.id());
        assertTrue(descriptor.supportsJavaType(java.util.LinkedHashMap.class));
        assertTrue(descriptor.supportsLogicalType(LogicalType.JSON));
        assertTrue(descriptor.requiresCapability("NATIVE-JSON"));
        assertFalse(descriptor.supportsLogicalType(LogicalType.TEXT));
        assertEquals(descriptor.fingerprint(), ValueCodecDescriptor.of(
                "JSON-VALUE", Set.of(java.util.List.class, java.util.Map.class),
                Set.of(LogicalType.JSON), Set.of("native-json")).fingerprint());
    }
}
