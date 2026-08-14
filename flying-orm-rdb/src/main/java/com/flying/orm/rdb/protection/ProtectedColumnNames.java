package com.flying.orm.rdb.protection;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** 为受保护字段生成不超过 30 个 ASCII 字符的稳定内部列名。 */
final class ProtectedColumnNames {

    private static final int HASH_HEX_LENGTH = 16;

    private ProtectedColumnNames() {
    }

    static String exact(String formId, String fieldName) {
        return "__fop_e_" + hash(formId, fieldName, "exact");
    }

    static String suffix(String formId, String fieldName, int length) {
        return "__fop_s" + length + "_" + hash(formId, fieldName, "suffix/" + length);
    }

    static String containsTable(String formId, String tableName) {
        return "__fop_c_" + hash(formId, tableName, "contains-table");
    }

    static String containsFieldTag(String formId, String fieldName) {
        return "f_" + hash(formId, fieldName, "contains-field");
    }

    static String containsQueryIndex(String formId, String tableName) {
        return "__fop_q_" + hash(formId, tableName, "contains-query-index");
    }

    static String containsUniqueIndex(String formId, String tableName) {
        return "__fop_u_" + hash(formId, tableName, "contains-unique-index");
    }

    static String containsForeignKey(String formId, String tableName) {
        return "__fop_fk_" + hash(formId, tableName, "contains-foreign-key");
    }

    private static String hash(String formId, String fieldName, String purpose) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                                         .digest((formId + '\0' + fieldName + '\0' + purpose)
                                                         .getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(HASH_HEX_LENGTH);
            for (int index = 0; index < HASH_HEX_LENGTH / 2; index++) {
                result.append(Character.forDigit((digest[index] >>> 4) & 0x0f, 16));
                result.append(Character.forDigit(digest[index] & 0x0f, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is required by Java 21", error);
        }
    }
}
