package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证显式密钥轮换任务只重写旧版本密文，并能幂等跳过 current 版本。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedFieldReprotectionTest {

    /** 旧版本恢复为逻辑值，current 版本重复扫描时必须直接跳过。 */
    @Test
    void extractsOnlyValuesThatNeedReprotection() {
        DynamicForm form = DynamicForm.builder("customer", "customer")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("contact", "VARCHAR"))
                                      .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                     .searchModes(
                                                                                             EncryptedSearchMode.EXACT,
                                                                                             EncryptedSearchMode.CONTAINS)
                                                                                     .build())
                                      .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        byte[] oldCiphertext;
        try (ProtectedFieldKeyRing oldKeys = ProtectedFieldKeyRing.single("v1", key(1))) {
            oldCiphertext = (byte[]) ProtectedFieldRuntime.create(oldKeys)
                    .prepareWrite(form, Map.of("contact", "AlphaBeta"), DataScope.none(), codecs)
                    .values().get("contact");
        }

        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .build()) {
            ProtectedFieldReprotection reprotection = ProtectedFieldReprotection.create(keys);
            assertEquals("v1", reprotection.ciphertextVersion(oldCiphertext));
            assertEquals(Map.of("contact", "AlphaBeta"), reprotection.valuesNeedingReprotection(
                    form, Map.of("contact", oldCiphertext), DataScope.none(), codecs));

            byte[] current = (byte[]) ProtectedFieldRuntime.create(keys)
                    .prepareWrite(form, Map.of("contact", "AlphaBeta"), DataScope.none(), codecs)
                    .values().get("contact");
            assertTrue(reprotection.valuesNeedingReprotection(
                    form, Map.of("contact", current), DataScope.none(), codecs).isEmpty());
        }
    }

    /** 历史明文只在目标密文缺失时进入迁移；已有密文的行必须可重复扫描并幂等跳过。 */
    @Test
    void extractsOnlyPlaintextValuesWhoseTargetCiphertextIsMissing() {
        DynamicForm form = DynamicForm.builder("customer", "customer")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("contact", "VARCHAR"))
                                      .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                      .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(3))) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);
            assertEquals(Map.of("contact", "13800138000"), migration.valuesNeedingPlaintextMigration(
                    form, Map.of("contact", "13800138000"), Map.of()));

            byte[] ciphertext = (byte[]) ProtectedFieldRuntime.create(keys)
                    .prepareWrite(form, Map.of("contact", "13800138000"),
                                  DataScope.none(), ValueCodecRegistry.standard())
                    .values().get("contact");
            assertTrue(migration.valuesNeedingPlaintextMigration(
                    form,
                    Map.of("contact", "13800138000"),
                    Map.of("contact", ciphertext)).isEmpty());
        }
    }

    /** 大小写回退只接受唯一列，避免轮换任务按 Map 迭代顺序选择错误明文。 */
    @Test
    void rejectsAmbiguousCaseFoldedMigrationColumns() {
        DynamicForm form = DynamicForm.builder("customer", "customer")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("contact", "VARCHAR"))
                                      .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                      .build();
        Map<String, Object> ambiguous = new LinkedHashMap<>();
        ambiguous.put("CONTACT", "first");
        ambiguous.put("Contact", "second");
        Map<String, Object> exactAndFolded = new LinkedHashMap<>();
        exactAndFolded.put("contact", "exact");
        exactAndFolded.put("CONTACT", "folded");
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(4))) {
            ProtectedFieldReprotection migration = ProtectedFieldReprotection.create(keys);

            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> migration.valuesNeedingPlaintextMigration(form, ambiguous, Map.of()));
            IllegalArgumentException exactFailure = assertThrows(IllegalArgumentException.class,
                    () -> migration.valuesNeedingPlaintextMigration(form, exactAndFolded, Map.of()));

            assertEquals("protected migration column is ambiguous", failure.getMessage());
            assertEquals("protected migration column is ambiguous", exactFailure.getMessage());
        }
    }

    private static byte[] key(int seed) {
        byte[] key = new byte[32];
        java.util.Arrays.fill(key, (byte) seed);
        return key;
    }
}
