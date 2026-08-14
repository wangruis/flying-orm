package com.flying.orm.rdb.protection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证自定义保护值规范化器的异常边界不会泄露待加密明文。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedValueNormalizerRegistryTest {

    @Test
    void sanitizesCustomNormalizerFailureWithoutExposingPlaintext() {
        String plaintext = "sensitive-value-that-must-not-escape";
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("failing", value -> {
                    throw new IllegalStateException("normalizer leaked " + value);
                });

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> registry.normalize("failing", plaintext, 128));

        assertEquals("protected value normalizer failed", error.getMessage());
        assertNull(error.getCause());
    }

    /** 自定义规范化器抛出的普通 Error 不能把待加密明文带出公共错误边界。 */
    @Test
    void sanitizesOrdinaryErrorFromCustomNormalizer() {
        String plaintext = "ordinary-error-secret";
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("error", value -> {
                    throw new AssertionError("normalizer leaked " + value);
                });

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> registry.normalize("error", plaintext, 128));

        assertEquals("protected value normalizer failed", error.getMessage());
        assertNull(error.getCause());
    }

    @Test
    void preservesVirtualMachineErrorFromCustomNormalizer() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("fatal", value -> {
                    throw fatal;
                });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> registry.normalize("fatal", "secret", 128));

        assertSame(fatal, observed);
    }

    /** 扩展实现包装的 VM 错误也必须提升原对象，不能伪装成普通规范化失败。 */
    @Test
    void preservesNestedVirtualMachineErrorFromCustomNormalizer() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested normalizer fatal");
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("fatal-wrapper", value -> {
                    throw new IllegalStateException("normalizer wrapper", fatal);
                });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> registry.normalize("fatal-wrapper", "secret", 128));

        assertSame(fatal, observed);
    }

    /** 自定义规范化器深层包装的 VME 仍须按原对象传播。 */
    @Test
    void preservesDeeplyNestedVirtualMachineErrorFromCustomNormalizer() {
        OutOfMemoryError fatal = new OutOfMemoryError("deep normalizer fatal");
        RuntimeException wrapper = new IllegalStateException("wrapper-0", fatal);
        for (int depth = 1; depth < 70; depth++) {
            wrapper = new IllegalStateException("wrapper-" + depth, wrapper);
        }
        RuntimeException failure = wrapper;
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard()
                .with("deep-fatal-wrapper", value -> { throw failure; });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> registry.normalize("deep-fatal-wrapper", "secret", 128));

        assertSame(fatal, observed);
    }
}
