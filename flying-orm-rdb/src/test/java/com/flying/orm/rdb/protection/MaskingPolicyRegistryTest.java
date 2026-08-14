package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证通用 masking policy 和查询级展示优先级。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class MaskingPolicyRegistryTest {

    @Test
    void masksByUnicodeCodePointWithoutAssumingPhoneOrIdentityFields() {
        MaskingPolicyRegistry registry = MaskingPolicyRegistry.standard();
        MaskedFieldDefinition definition = MaskedFieldDefinition.builder("partial")
                .prefix(1)
                .suffix(2)
                .build();

        assertEquals("张***世界", registry.mask("张敏感世界", definition));
        assertEquals("*****", registry.mask("short", MaskedFieldDefinition.builder("full").build()));
        assertNull(registry.mask(null, definition));
    }

    @Test
    void appliesQueryOverrideThenDeclarationOnlyToExplicitlyMaskedFields() {
        DynamicForm form = DynamicForm.builder("customer", "customer")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("ordinary", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("partial")
                        .prefix(1)
                        .suffix(1)
                        .display(SensitiveDisplayMode.FULL)
                        .build())
                .build();
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        values.put("secret", "abcdef");
        values.put("ordinary", "unchanged");
        DynamicRow row = DynamicRow.copyOf(values);
        MaskedFieldResultTransformer transformer = new MaskedFieldResultTransformer(MaskingPolicyRegistry.standard());

        assertEquals("abcdef", transformer.transform(form, row, SensitiveDisplayMode.DECLARED).get("secret"));
        assertEquals("a****f", transformer.transform(form, row, SensitiveDisplayMode.MASKED).get("secret"));
        assertEquals("abcdef", transformer.transform(form, row, SensitiveDisplayMode.FULL).get("secret"));
        assertEquals("unchanged", transformer.transform(form, row, SensitiveDisplayMode.MASKED).get("ordinary"));
        assertEquals(Map.of("secret", "a****f", "ordinary", "unchanged"),
                     transformer.transform(form, row, SensitiveDisplayMode.MASKED));
    }

    @Test
    void sanitizesCustomMaskingFailureWithoutExposingPlaintext() {
        String plaintext = "sensitive-value-that-must-not-escape";
        MaskingPolicyRegistry registry = MaskingPolicyRegistry.standard()
                .with("failing", (value, definition) -> {
                    throw new IllegalStateException("masking leaked " + value);
                });
        MaskedFieldDefinition definition = MaskedFieldDefinition.builder("failing").build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> registry.mask(plaintext, definition));

        assertEquals("masking policy failed", error.getMessage());
        assertNull(error.getCause());
    }

    /** 普通 Error 也可能携带完整明文；只有 JVM 致命错误允许越过脱敏边界。 */
    @Test
    void sanitizesOrdinaryErrorFromCustomMaskingPolicy() {
        String plaintext = "ordinary-error-secret";
        MaskingPolicyRegistry registry = MaskingPolicyRegistry.standard()
                .with("error", (value, definition) -> {
                    throw new AssertionError("masking leaked " + value);
                });

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> registry.mask(plaintext, MaskedFieldDefinition.builder("error").build()));

        assertEquals("masking policy failed", error.getMessage());
        assertNull(error.getCause());
    }

    @Test
    void preservesVirtualMachineErrorFromCustomMaskingPolicy() {
        OutOfMemoryError fatal = new OutOfMemoryError("fatal");
        MaskingPolicyRegistry registry = MaskingPolicyRegistry.standard()
                .with("fatal", (value, definition) -> {
                    throw fatal;
                });
        MaskedFieldDefinition definition = MaskedFieldDefinition.builder("fatal").build();

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> registry.mask("secret", definition));

        assertSame(fatal, observed);
    }

    /** 扩展策略包装的 VM 错误仍按 JVM 致命错误传播，不能被脱敏分类吞掉。 */
    @Test
    void preservesNestedVirtualMachineErrorFromCustomMaskingPolicy() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested masking fatal");
        MaskingPolicyRegistry registry = MaskingPolicyRegistry.standard()
                .with("fatal-wrapper", (value, definition) -> {
                    throw new IllegalStateException("masking wrapper", fatal);
                });

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> registry.mask("secret", MaskedFieldDefinition.builder("fatal-wrapper").build()));

        assertSame(fatal, observed);
    }
}
