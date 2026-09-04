package com.flying.orm.core.codec;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueCodecRegistryPriorityTest {

    @Test
    void stopsAtTheFirstMatchingCodecForWritesAndReads() {
        List<String> calls = new ArrayList<>();
        ValueCodecRegistry registry = new ValueCodecRegistry(List.of(
                codec("skip", false, calls), codec("first", true, calls), codec("last", true, calls)));

        assertEquals("first", registry.write(7));
        assertEquals(List.of("skip", "first"), calls);
        calls.clear();
        assertEquals("first", registry.read(7, String.class));
        assertEquals(List.of("skip", "first"), calls);
    }

    @Test
    void withFirstChangesOnlyTheNewRegistryAndKeepsReadFastPaths() {
        List<String> calls = new ArrayList<>();
        ValueCodecRegistry original = new ValueCodecRegistry(List.of(codec("original", true, calls)));
        ValueCodecRegistry extended = original.withFirst(codec("custom", true, calls));

        assertEquals("custom", extended.write(7));
        assertEquals("custom", extended.read(7, String.class));
        assertEquals("original", original.write(7));
        assertEquals(List.of("custom", "custom", "original"), calls);
        calls.clear();
        Integer sameValue = 7;
        assertSame(sameValue, extended.read(sameValue, int.class));
        assertNull(extended.write(null));
        assertNull(extended.read(null, String.class));
        assertEquals(List.of(), calls);
    }

    @Test
    void keepsUnsupportedTypeMessagesAndSupportFailures() {
        ValueCodecRegistry empty = new ValueCodecRegistry(List.of());
        assertEquals("no value codec for java.lang.Integer",
                     assertThrows(IllegalArgumentException.class, () -> empty.write(7)).getMessage());
        assertEquals("no value codec for java.lang.String",
                     assertThrows(IllegalArgumentException.class, () -> empty.read(7, String.class)).getMessage());
        IllegalStateException failure = new IllegalStateException("custom support failure");
        ValueCodecRegistry failing = empty.withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                throw failure;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new AssertionError("unreachable");
            }
        });
        assertSame(failure, assertThrows(IllegalStateException.class, () -> failing.write(7)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> failing.read(7, String.class)));
        assertThrows(NullPointerException.class, () -> empty.read(null, null));
    }

    private static ValueCodec codec(String name, boolean matches, List<String> calls) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                calls.add(name);
                return matches;
            }

            @Override
            public Object write(Object value) {
                return name;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return name;
            }
        };
    }
}
