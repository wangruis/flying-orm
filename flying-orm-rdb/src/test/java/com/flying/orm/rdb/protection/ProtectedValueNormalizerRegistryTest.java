package com.flying.orm.rdb.protection;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedValueNormalizerRegistryTest {

    @Test
    void invokesCustomNormalizerExactlyOnce() {
        AtomicInteger invocations = new AtomicInteger();
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("counting", value -> {
                    invocations.incrementAndGet();
                    return value.strip();
                });

        assertEquals("value", registry.normalize("counting", " value ", 16));
        assertEquals(1, invocations.get());
    }

    @Test
    void retainsNullEmptyAndLengthValidation() {
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("empty", value -> "")
                .with("long", value -> "abc");

        assertThrows(NullPointerException.class, () -> registry.normalize("identity", null, 8));
        assertThrows(IllegalArgumentException.class, () -> registry.normalize("empty", "value", 8));
        assertThrows(IllegalArgumentException.class, () -> registry.normalize("long", "value", 2));
    }

    @Test
    void sanitizesExtensionFailuresWithoutRetainingPlaintext() {
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("failure", value -> {
                    throw new IllegalStateException("leaked:" + value);
                });

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> registry.normalize("failure", "secret-value", 32));

        assertEquals("protected value normalizer failed", failure.getMessage());
        assertFalse(failure.getMessage().contains("secret-value"));
        assertEquals(null, failure.getCause());
    }
}
