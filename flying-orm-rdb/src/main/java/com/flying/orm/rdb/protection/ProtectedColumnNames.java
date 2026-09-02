package com.flying.orm.rdb.protection;

import com.flying.orm.core.internal.hash.StableDigest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/** 为受保护字段生成不超过 30 个 ASCII 字符的稳定内部列名。 */
final class ProtectedColumnNames {

    private static final int HASH_HEX_LENGTH = 16;
    private static final StableDigest.Domain NAME_DOMAIN = StableDigest.domain("protected-column-name/v1");

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
        if (formId.indexOf('\0') < 0 && fieldName.indexOf('\0') < 0) {
            return legacyHash(formId, fieldName, purpose);
        }
        return StableDigest.sha256(NAME_DOMAIN)
                           .text("FORM", formId)
                           .text("SOURCE", fieldName)
                           .text("PURPOSE", purpose)
                           .finishHex()
                           .substring(0, HASH_HEX_LENGTH);
    }

    /** 这些名称已经属于持久化数据库结构；普通标识符必须继续生成同一物理名称。 */
    private static String legacyHash(String formId, String fieldName, String purpose) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                                         .digest((formId + '\0' + fieldName + '\0' + purpose)
                                                         .getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, HASH_HEX_LENGTH / 2);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is required by Java 21", error);
        }
    }
}
