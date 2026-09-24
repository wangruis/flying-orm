package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionInputSnapshotTest {

    @Test
    void preservesReferenceArrayTypeWhileIsolatingItsElements() {
        Byte[] source = {1, 2};

        StructuredConditionInput input = StructuredConditionInput.term("payload", "eq", source);
        Byte[] snapshot = assertInstanceOf(Byte[].class, input.value());
        source[0] = 9;

        assertArrayEquals(new Byte[]{1, 2}, snapshot);
        assertNotSame(snapshot, input.value());
        assertArrayEquals(new Byte[]{1, 2}, assertInstanceOf(Byte[].class, input.value()));
    }

    @Test
    void acceptsConcreteMapArraysWithoutLeakingTheirMutableRuntimeType() {
        @SuppressWarnings("unchecked")
        Map<String, Object>[] source = new LinkedHashMap[]{new LinkedHashMap<>(Map.of("id", 1))};

        StructuredConditionInput input = StructuredConditionInput.term("payload", "eq", source);
        Object[] snapshot = assertInstanceOf(Object[].class, input.value());
        Map<?, ?> item = assertInstanceOf(Map.class, snapshot[0]);

        source[0].put("id", 2);
        assertEquals(1, item.get("id"));
        assertNotSame(snapshot, input.value());
        assertThrows(UnsupportedOperationException.class, item::clear);
    }

    @Test
    void snapshotsValuesBeyondTheFormerDepthBoundary() {
        Object value = "leaf";
        for (int depth = 0; depth < 66; depth++) {
            value = java.util.List.of(value);
        }
        Object deepValue = value;

        assertInstanceOf(java.util.List.class,
                         StructuredConditionInput.term("payload", "eq", deepValue).value());
    }

    @Test
    void isolatesOwnedInputBeforeEveryCodecRead() {
        ValueCodec consumingCodec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == Integer.class;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                byte[] bytes = (byte[]) value;
                int decoded = Byte.toUnsignedInt(bytes[0]);
                bytes[0]++;
                return decoded;
            }
        };
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("sequence", "INTEGER"))
                .build();
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create(
                ValueCodecRegistry.standard().withFirst(consumingCodec));
        StructuredConditionInput input = StructuredConditionInput.term(
                "sequence", "eq", new byte[]{1});

        TermCondition first = firstTerm(compiler.compile(form, input));
        TermCondition second = firstTerm(compiler.compile(form, input));

        assertEquals(1, first.value());
        assertEquals(1, second.value());
        assertEquals(1, Byte.toUnsignedInt(assertInstanceOf(byte[].class, input.value())[0]));
    }

    private static TermCondition firstTerm(ConditionGroup group) {
        return assertInstanceOf(TermCondition.class, group.children().getFirst());
    }
}
