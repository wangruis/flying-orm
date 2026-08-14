package com.flying.orm.core.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证字段保护只有显式声明时才启用，并在发布后保持不可变。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class FieldProtectionRegistryTest {

    @Test
    void keepsExplicitDynamicFormProtectionWithoutAffectingRegularFields() {
        EncryptedFieldDefinition encrypted = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX)
                .normalizer("digits")
                .suffixLengths(4, 8)
                .maxNormalizedLength(64)
                .build();
        MaskedFieldDefinition masked = MaskedFieldDefinition.builder("partial")
                .prefix(3)
                .suffix(4)
                .display(SensitiveDisplayMode.FULL)
                .build();

        DynamicForm form = DynamicForm.builder("customer", "customer")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("contactValue", "VARCHAR"))
                .addField(DynamicField.of("nickname", "VARCHAR"))
                .encrypted("contactValue", encrypted)
                .masked("contactValue", masked)
                .build();

        assertEquals(encrypted, form.protections().encrypted(" contactValue ").orElseThrow());
        assertEquals(masked, form.protections().masked("CONTACTVALUE").orElseThrow());
        assertFalse(form.protections().protectedField("nickname"));
        assertTrue(form.protections().protectedField("contactValue"));
    }

    @Test
    void validatesSearchModesAndTakesImmutableSnapshots() {
        int[] suffixes = {8, 4, 4};
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.SUFFIX, EncryptedSearchMode.EXACT)
                .suffixLengths(suffixes)
                .build();
        suffixes[0] = 2;

        assertEquals(Set.of(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX), definition.searchModes());
        assertEquals(List.of(4, 8), definition.suffixLengths());
        assertThrows(UnsupportedOperationException.class, () -> definition.suffixLengths().add(12));
        assertThrows(IllegalArgumentException.class,
                     () -> EncryptedFieldDefinition.builder()
                             .searchModes(EncryptedSearchMode.SUFFIX)
                             .build());
        assertThrows(IllegalArgumentException.class,
                     () -> EncryptedFieldDefinition.builder()
                             .searchModes(EncryptedSearchMode.CONTAINS)
                             .containsMinLength(2)
                             .build());
        assertThrows(IllegalArgumentException.class,
                     () -> EncryptedFieldDefinition.builder()
                             .searchModes(EncryptedSearchMode.SUFFIX)
                             .suffixLengths(65)
                             .maxNormalizedLength(64)
                             .build());
        EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT)
                .containsMinLength(5_000)
                .maxNormalizedLength(64)
                .build();
    }

    /** 后缀声明必须有固定容量，避免一个配置生成无界隐藏列、索引和写入令牌。 */
    @Test
    void rejectsAnUnboundedNumberOfSuffixSearchLengths() {
        int[] lengths = IntStream.rangeClosed(1, 33).toArray();

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.SUFFIX)
                        .suffixLengths(lengths)
                        .build());

        assertEquals("encrypted suffix length count exceeds the safe limit", failure.getMessage());
    }

    /** 扩展 ID 必须在元数据发布前满足与运行时 registry 相同的有界格式。 */
    @Test
    void rejectsInvalidProtectionExtensionIdsAtDefinitionBoundary() {
        String invalidId = "caller supplied policy ".repeat(256);

        IllegalArgumentException normalizerFailure = assertThrows(
                IllegalArgumentException.class,
                () -> EncryptedFieldDefinition.builder().normalizer(invalidId).build());
        IllegalArgumentException policyFailure = assertThrows(
                IllegalArgumentException.class,
                () -> MaskedFieldDefinition.builder(invalidId).build());

        assertEquals("protected value normalizer id is invalid", normalizerFailure.getMessage());
        assertEquals("masking policy id is invalid", policyFailure.getMessage());
        assertFalse(normalizerFailure.getMessage().contains(invalidId));
        assertFalse(policyFailure.getMessage().contains(invalidId));
    }

    @Test
    void rejectsProtectionForUnknownOrDuplicateFieldsWithoutEchoingCallerName() {
        String secretName = "secret-caller-field";
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder().build();

        IllegalArgumentException missing = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("customer", "customer")
                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                        .encrypted(secretName, definition)
                        .build());
        IllegalArgumentException duplicate = assertThrows(
                IllegalArgumentException.class,
                () -> FieldProtectionRegistry.builder()
                        .encrypted(secretName, definition)
                        .encrypted(" " + secretName + " ", definition));

        assertEquals("protected field does not exist in form", missing.getMessage());
        assertEquals("duplicate encrypted field declaration", duplicate.getMessage());
        assertFalse(missing.getMessage().contains(secretName));
        assertFalse(duplicate.getMessage().contains(secretName));
    }
}
