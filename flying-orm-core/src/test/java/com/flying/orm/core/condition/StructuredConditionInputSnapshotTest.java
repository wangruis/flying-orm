package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StructuredConditionInputSnapshotTest {

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
    void rejectsValuesBeyondTheDepthBoundary() {
        Object value = "leaf";
        for (int depth = 0; depth < 66; depth++) {
            value = java.util.List.of(value);
        }
        Object deepValue = value;

        assertThrows(StructuredConditionException.class,
                     () -> StructuredConditionInput.term("payload", "eq", deepValue));
    }
}
