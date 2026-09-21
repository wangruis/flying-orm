package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecDescriptor;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.LogicalType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ExistingRegistryFingerprintInvalidationTest {

    @Test
    void termAndCodecDescriptorChangesInvalidateTheirFrozenFingerprint() {
        TermRegistry oneParameter = TermRegistry.builder().add(TermHandler.described(
                TermExtensionDescriptor.filter("custom", Set.of(), 1, 1),
                ConditionValueShape.SCALAR)).build();
        TermRegistry twoParameters = TermRegistry.builder().add(TermHandler.described(
                TermExtensionDescriptor.filter("custom", Set.of(), 2, 1),
                ConditionValueShape.SCALAR)).build();

        assertNotEquals(oneParameter.descriptorFingerprint(), twoParameters.descriptorFingerprint());

        ValueCodecRegistry text = new ValueCodecRegistry(List.of(codec(LogicalType.TEXT)));
        ValueCodecRegistry json = new ValueCodecRegistry(List.of(codec(LogicalType.JSON)));
        assertNotEquals(text.descriptorFingerprint(), json.descriptorFingerprint());
    }

    private static ValueCodec codec(LogicalType type) {
        ValueCodecDescriptor descriptor = ValueCodecDescriptor.of(
                "custom-value", Set.of(String.class), Set.of(type), Set.of());
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value.toString();
            }

            @Override
            public Optional<ValueCodecDescriptor> descriptor() {
                return Optional.of(descriptor);
            }
        };
    }
}
