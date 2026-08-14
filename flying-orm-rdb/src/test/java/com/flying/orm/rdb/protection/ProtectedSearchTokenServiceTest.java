package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证保护搜索的规范化、版本轮换和字段/租户用途隔离。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class ProtectedSearchTokenServiceTest {

    @Test
    void normalizesUnicodeValuesWithStableBuiltInPolicies() {
        ProtectedValueNormalizerRegistry registry = ProtectedValueNormalizerRegistry.standard();

        assertEquals("Ａlice", registry.normalize("identity", "Ａlice", 32));
        assertEquals("alice", registry.normalize("case-fold", "ALICE", 32));
        assertEquals("13800138000", registry.normalize("digits", "１３８-0013-8000", 32));
        IllegalArgumentException empty = assertThrows(
                IllegalArgumentException.class,
                () -> registry.normalize("digits", "not-a-number", 32));
        assertEquals("protected normalized value must not be empty", empty.getMessage());
        assertThrows(IllegalArgumentException.class, () -> registry.normalize("missing", "value", 32));
    }

    @Test
    void createsCurrentWriteTokenAndAllReadableQueryTokensInStableOrder() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT)
                .normalizer("digits")
                .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .build()) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());
            ProtectedFieldContext context = new ProtectedFieldContext("customer", "contact", "tenant-a");

            byte[] writeToken = tokens.currentExactToken("138-0013-8000", definition, context);
            List<byte[]> queryTokens = tokens.exactQueryTokens("１３８00138000", definition, context);

            assertEquals(2, queryTokens.size());
            assertArrayEquals(writeToken, queryTokens.getFirst());
            assertFalse(Arrays.equals(queryTokens.get(0), queryTokens.get(1)));
        }
    }

    /** 唯一字段的盲索引必须使用独立稳定密钥，轮换加密密钥后仍由数据库唯一索引识别同一业务值。 */
    @Test
    void keepsUniqueExactTokenStableAcrossEncryptionKeyRotation() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT)
                .normalizer("digits")
                .build();
        ProtectedFieldContext context = new ProtectedFieldContext("customer", "contact", "tenant-a");
        byte[] initial;
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            initial = new ProtectedSearchTokenService(keys, ProtectedValueNormalizerRegistry.standard())
                    .currentExactToken("13800138000", definition, context, true);
        }

        try (ProtectedFieldKeyRing rotating = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .uniqueSearchKey(key(1))
                .build()) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    rotating, ProtectedValueNormalizerRegistry.standard());

            byte[] current = tokens.currentExactToken("138-0013-8000", definition, context, true);
            List<byte[]> query = tokens.exactQueryTokens("１３８00138000", definition, context, true);

            assertArrayEquals(initial, current);
            assertEquals(1, query.size());
            assertArrayEquals(initial, query.getFirst());
        }
    }

    /** 受保护批量回执与唯一搜索共用稳定身份密钥，但通过 HKDF purpose 保持用途隔离。 */
    @Test
    void keepsReceiptPayloadIdentityStableAcrossEncryptionKeyRotation() {
        ProtectedFieldContext context = new ProtectedFieldContext("customer", "contact", "tenant-a");
        byte[] initial;
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            initial = new ProtectedSearchTokenService(keys, ProtectedValueNormalizerRegistry.standard())
                    .stableReceiptToken("13800138000", context);
        }

        try (ProtectedFieldKeyRing rotating = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .uniqueSearchKey(key(1))
                .build()) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    rotating, ProtectedValueNormalizerRegistry.standard());
            byte[] receipt = tokens.stableReceiptToken("13800138000", context);
            EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                    .searchModes(EncryptedSearchMode.EXACT)
                    .build();
            byte[] exact = tokens.currentExactToken("13800138000", definition, context, true);

            assertArrayEquals(initial, receipt);
            assertFalse(Arrays.equals(receipt, exact));
        }
    }

    /** 轮换期没有显式稳定唯一搜索密钥时必须拒绝唯一字段写入，不能静默改变数据库唯一语义。 */
    @Test
    void rejectsUniqueExactTokenDuringRotationWithoutStableSearchKey() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder().build();
        try (ProtectedFieldKeyRing rotating = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .build()) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    rotating, ProtectedValueNormalizerRegistry.standard());

            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> tokens.currentExactToken("value", definition,
                            new ProtectedFieldContext("customer", "contact", "tenant-a"), true));

            assertEquals("stable unique search key is required during key rotation", error.getMessage());
        }
    }

    @Test
    void isolatesTokensByFieldTenantAndSearchPurpose() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX)
                .normalizer("digits")
                .suffixLengths(4)
                .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());
            ProtectedFieldContext contact = new ProtectedFieldContext("customer", "contact", "tenant-a");

            byte[] exact = tokens.currentExactToken("13800138000", definition, contact);
            byte[] suffix = tokens.currentSuffixToken("8000", definition, contact);
            byte[] otherField = tokens.currentExactToken("13800138000", definition,
                    new ProtectedFieldContext("customer", "identity", "tenant-a"));
            byte[] otherTenant = tokens.currentExactToken("13800138000", definition,
                    new ProtectedFieldContext("customer", "contact", "tenant-b"));

            assertFalse(Arrays.equals(exact, suffix));
            assertFalse(Arrays.equals(exact, otherField));
            assertFalse(Arrays.equals(exact, otherTenant));
        }
    }

    /** 租户身份必须按完整编码值隔离，不能因尾部空白规范化而共享密码学上下文。 */
    @Test
    void preservesExactTenantIdentityWhenDerivingSearchTokens() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder().build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());

            byte[] plain = tokens.currentExactToken("value", definition,
                    new ProtectedFieldContext("customer", "contact", "text:tenant"));
            byte[] trailingSpace = tokens.currentExactToken("value", definition,
                    new ProtectedFieldContext("customer", "contact", "text:tenant "));

            assertFalse(Arrays.equals(plain, trailingSpace));
        }
    }

    @Test
    void rejectsUndeclaredOrWrongLengthSuffixBeforeCreatingSqlParameters() {
        String secret = "secret-search-input";
        EncryptedFieldDefinition exactOnly = EncryptedFieldDefinition.builder().build();
        EncryptedFieldDefinition suffix = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.SUFFIX)
                .suffixLengths(4)
                .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());
            ProtectedFieldContext context = new ProtectedFieldContext("customer", "contact", "tenant-a");

            IllegalArgumentException undeclared = assertThrows(
                    IllegalArgumentException.class,
                    () -> tokens.currentSuffixToken(secret, exactOnly, context));
            IllegalArgumentException wrongLength = assertThrows(
                    IllegalArgumentException.class,
                    () -> tokens.currentSuffixToken(secret, suffix, context));

            assertFalse(undeclared.getMessage().contains(secret));
            assertFalse(wrongLength.getMessage().contains(secret));
        }
    }

    /** 后缀声明只描述可搜索长度，较短的合法业务值不能被误当作字段最小长度校验。 */
    @Test
    void storesNullForDeclaredSuffixesLongerThanTheProtectedValue() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.SUFFIX)
                .suffixLengths(4, 8)
                .build();
        DynamicForm form = DynamicForm.builder("customer", "customer")
                .addField(DynamicField.of("contact", "VARCHAR").withNullable(false))
                .encrypted("contact", definition)
                .build();
        String shortSuffixColumn = ProtectedFormLayout.suffixColumn(form, "contact", 8);

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldRuntime.PreparedWrite write = ProtectedFieldRuntime.create(keys).prepareWrite(
                    form, Map.of("contact", "8000"), DataScope.none(), ValueCodecRegistry.standard());

            assertTrue(write.values().get(ProtectedFormLayout.suffixColumn(form, "contact", 4)) instanceof byte[]);
            assertTrue(write.values().containsKey(shortSuffixColumn));
            assertNull(write.values().get(shortSuffixColumn));
            assertTrue(write.physicalForm().field(shortSuffixColumn).nullable());
        }
    }

    /** CONTAINS 使用 Unicode code point trigram，重复片段去重并按密钥版本独立分组。 */
    @Test
    void createsDeduplicatedContainsTokensGroupedByReadableKeyVersion() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.CONTAINS)
                .normalizer("case-fold")
                .containsMinLength(3)
                .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .build()) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());
            ProtectedFieldContext context = new ProtectedFieldContext("customer", "alias", "tenant-a");

            List<byte[]> writeTokens = tokens.currentContainsTokens("ABABA", definition, context);
            ProtectedSearchTokenService.ContainsQuery query = tokens.containsQuery("abaBA", definition, context);

            assertEquals(2, writeTokens.size());
            assertEquals(2, query.groups().size());
            assertEquals(2, query.groups().getFirst().tokens().size());
            assertArrayEquals(writeTokens.getFirst(), query.groups().getFirst().tokens().getFirst());
            assertFalse(Arrays.equals(query.groups().getFirst().tokens().getFirst(),
                                      query.groups().getLast().tokens().getFirst()));
        }
    }

    /** 任意短字符串和超出固定令牌预算的 CONTAINS 都必须在生成 SQL 前稳定拒绝。 */
    @Test
    void rejectsUnsafeContainsSearchBeforeCreatingSqlParameters() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.CONTAINS)
                .containsMinLength(4)
                .maxNormalizedLength(5000)
                .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                    keys, ProtectedValueNormalizerRegistry.standard());
            ProtectedFieldContext context = new ProtectedFieldContext("customer", "alias", "tenant-a");

            IllegalArgumentException shortValue = assertThrows(
                    IllegalArgumentException.class,
                    () -> tokens.containsQuery("abc", definition, context));
            String manyUniqueTrigrams = uniqueTrigramInput(4100);
            IllegalArgumentException tooMany = assertThrows(
                    IllegalArgumentException.class,
                    () -> tokens.containsQuery(manyUniqueTrigrams, definition, context));

            assertEquals("protected contains search value is too short", shortValue.getMessage());
            assertEquals("protected contains token limit exceeded", tooMany.getMessage());
            assertTrue(manyUniqueTrigrams.length() <= definition.maxNormalizedLength());
        }
    }

    private static String uniqueTrigramInput(int length) {
        StringBuilder value = new StringBuilder(length);
        for (int index = 0; index < length; index++) {
            value.appendCodePoint(0x1000 + index);
        }
        return value.toString();
    }

    private static byte[] key(int seed) {
        byte[] value = new byte[32];
        Arrays.fill(value, (byte) seed);
        return value;
    }
}
