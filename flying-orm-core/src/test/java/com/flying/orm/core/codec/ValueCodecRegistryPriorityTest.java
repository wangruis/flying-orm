package com.flying.orm.core.codec;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueCodecRegistryPriorityTest {

    @Test
    void adaptersKeepPriorityAndReevaluateSupportOnEveryRead() {
        AtomicBoolean enabled = new AtomicBoolean();
        List<String> calls = new ArrayList<>();
        DriverValueAdapter first = adapter("first", enabled, calls);
        DriverValueAdapter last = adapter("last", new AtomicBoolean(true), calls);
        ValueCodecRegistry original = ValueCodecRegistry.standard().withDriverAdapter(last);
        ValueCodecRegistry extended = original.withDriverAdapter(first);
        assertEquals("last", extended.read(new Object(), String.class));
        enabled.set(true);
        assertEquals("first", extended.read(new Object(), String.class));
        assertEquals("last", original.read(new Object(), String.class));
        assertEquals(List.of("first", "last", "first", "last"), calls);
        calls.clear();
        assertNull(extended.read(null, String.class));
        assertEquals(List.of(), calls);
    }

    private static DriverValueAdapter adapter(String name, AtomicBoolean enabled, List<String> calls) {
        return new DriverValueAdapter() {
            public boolean supports(Object value) {
                calls.add(name);
                return enabled.get();
            }

            public Object unwrap(Object value) {
                return name;
            }
        };
    }

    @Test
    void textAndObjectFallbackKeepTheirExistingConversionRules() {
        ValueCodecRegistry registry = ValueCodecRegistry.standard();
        Object value = new Object();
        assertSame(value, registry.write(value));
        assertSame(value, registry.read(value, Object.class));
        assertEquals("7", registry.read(7, String.class));
        assertEquals(' ', registry.read(" ", Character.class));
        assertEquals("a", registry.write('a'));
        assertThrows(IllegalArgumentException.class, () -> registry.read(" a ", Character.class));
    }

    @Test
    void standardExtensionsReevaluateApplicationSupportBeforeBuiltins() {
        AtomicBoolean enabled = new AtomicBoolean();
        List<Class<?>> calls = new ArrayList<>();
        ValueCodecRegistry registry = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                calls.add(targetType);
                return enabled.get();
            }

            @Override
            public Object write(Object value) {
                return "custom";
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return 99;
            }
        });
        assertEquals(7, registry.write(7));
        assertEquals(7, registry.read("7", Integer.class));
        enabled.set(true);
        assertEquals("custom", registry.write(7));
        assertEquals(99, registry.read("7", Integer.class));
        enabled.set(false);
        assertEquals(7, registry.write(7));
        assertEquals(7, registry.read("7", Integer.class));
        assertEquals(List.of(Integer.class, Integer.class, Integer.class,
                             Integer.class, Integer.class, Integer.class), calls);
        assertEquals(7, ValueCodecRegistry.standard().write(7));
    }

    @Test
    void driverAdaptersKeepStandardFallbackAndExplicitRegistriesStayExplicit() {
        DriverValueAdapter adapter = new DriverValueAdapter() {
            @Override
            public boolean supports(Object value) {
                return value instanceof StringBuilder;
            }

            @Override
            public Object unwrap(Object value) {
                return value.toString();
            }
        };
        ValueCodecRegistry registry = ValueCodecRegistry.standard().withDriverAdapter(adapter);
        for (int index = 0; index < 2; index++) {
            assertEquals(7, registry.read(new StringBuilder("7"), Integer.class));
            assertEquals("text", registry.write(new StringBuilder("text")));
            assertEquals("ACTIVE", registry.write(State.ACTIVE));
            assertEquals(State.ACTIVE, registry.read("ACTIVE", State.class));
            assertThrows(IllegalArgumentException.class, () -> registry.write(new UnsupportedValue()));
        }
        ValueCodecRegistry explicit = new ValueCodecRegistry(List.of()).withDriverAdapter(adapter);
        assertThrows(IllegalArgumentException.class, () -> explicit.read("7", Integer.class));
        assertThrows(IllegalArgumentException.class, () -> explicit.write(7));
    }

    private enum State {
        ACTIVE
    }

    private record UnsupportedValue() {
    }

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

    @Test
    void bulkPrependKeepsOrderSnapshotsAndOriginalRegistry() {
        List<String> calls = new ArrayList<>();
        ValueCodecRegistry original = new ValueCodecRegistry(List.of(codec("original", true, calls)));
        ValueCodec[] additions = {codec("skip", false, calls), codec("first", true, calls),
                                  codec("last", true, calls)};
        ValueCodecRegistry extended = original.withFirst(additions);
        additions[1] = codec("replacement", true, calls);

        assertEquals("first", extended.write(7));
        assertEquals("first", extended.read(7, String.class));
        assertEquals("original", original.write(7));
        assertEquals(List.of("skip", "first", "skip", "first", "original"), calls);
        assertSame(original, original.withFirst());
        assertThrows(NullPointerException.class, () -> original.withFirst((ValueCodec) null));
    }

    @Test
    void missingCodecFallbackDoesNotChangeExistingReadsOrSelectedCodecs() {
        ValueCodecRegistry empty = new ValueCodecRegistry(List.of());
        assertEquals(List.of("decoded"), empty.read("raw", List.class, (value, type) -> List.of("decoded")));
        assertThrows(IllegalArgumentException.class, () -> empty.read("raw", List.class));
        assertThrows(IllegalArgumentException.class, () -> empty.read("raw", List.class, (value, type) -> value));
        assertThrows(NullPointerException.class, () -> empty.read("raw", List.class, (value, type) -> null));
        List<String> calls = new ArrayList<>();
        ValueCodecRegistry custom = empty.withFirst(codec("selected", true, calls));
        assertEquals("selected", custom.read(7, String.class, (value, type) -> {
            throw new AssertionError("fallback must not replace a selected application codec");
        }));
        assertEquals(List.of("selected"), calls);
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
