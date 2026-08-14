package com.flying.orm.core.form;

import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.protection.MaskedFieldDefinition;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 集中计算动态表单的稳定结构指纹和跨数据库自动索引名。
 *
 * <p>两个结果都只依赖结构，不读取业务值；从 {@link DynamicForm} 拆出后可让表单模型继续保持聚焦，同时避免
 * DDL 与缓存各自复制摘要算法。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class DynamicFormStructure {

    private static final int MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH = 30;

    private DynamicFormStructure() {
    }

    static String uniqueIndexName(String table, String column) {
        String tableSource = table.trim();
        String columnSource = column.trim();
        String tablePart = identifierPart(tableSource);
        String columnPart = identifierPart(columnSource);
        String base = "uk_" + tablePart + "_" + columnPart;
        boolean identityLost = !tablePart.equals(tableSource) || !columnPart.equals(columnSource);
        if (!identityLost && base.length() <= MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH) {
            return base;
        }
        String suffix = digest(tableSource + '\0' + columnSource).substring(0, 24);
        if (base.length() + suffix.length() + 1 <= MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH) {
            return base + "_" + suffix;
        }
        return base.substring(0, MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH - suffix.length() - 1) + "_" + suffix;
    }

    static String fingerprint(String table,
                              List<DynamicField> fields,
                              LogicDeleteDefinition logicDelete,
                              TenantDefinition tenant,
                              FieldProtectionRegistry protections) {
        StringBuilder shape = new StringBuilder(64 + fields.size() * 96);
        append(shape, table);
        for (DynamicField field : fields) {
            append(shape, field.normalizedName());
            append(shape, field.dataType().trim().toUpperCase(Locale.ROOT));
            append(shape, field.primaryKey());
            append(shape, field.nullable());
            append(shape, field.unique());
            append(shape, field.length());
            append(shape, field.precision());
            append(shape, field.scale());
            append(shape, field.generation().strategy());
            append(shape, field.generation().sequenceName());
            append(shape, field.generation().startWith());
            append(shape, field.generation().incrementBy());
            append(shape, field.generation().cacheSize());
        }
        append(shape, logicDelete == null ? null : logicDelete.fieldName().toLowerCase(Locale.ROOT));
        append(shape, tenant == null ? null : tenant.fieldName().toLowerCase(Locale.ROOT));
        append(shape, tenant == null ? null : tenant.strategy());
        appendEncryptedFields(shape, protections.encryptedFields());
        appendMaskedFields(shape, protections.maskedFields());
        return digest(shape.toString());
    }

    /**
     * Map 与 Set 的迭代顺序不属于结构契约，必须按字段名和枚举名规范化后再参与稳定指纹。
     */
    private static void appendEncryptedFields(
            StringBuilder target,
            Map<String, EncryptedFieldDefinition> definitions) {
        definitions.entrySet().stream()
                   .sorted(Map.Entry.comparingByKey())
                   .forEach(entry -> {
                       EncryptedFieldDefinition definition = entry.getValue();
                       append(target, entry.getKey());
                       definition.searchModes().stream()
                                 .map(Enum::name)
                                 .sorted()
                                 .forEach(mode -> append(target, mode));
                       append(target, definition.normalizer());
                       definition.suffixLengths().forEach(length -> append(target, length));
                       append(target, definition.maxNormalizedLength());
                       append(target, definition.containsMinLength());
                   });
    }

    /** 按字段名和声明属性写入脱敏结构，避免不可变 Map 的随机化迭代顺序污染指纹。 */
    private static void appendMaskedFields(
            StringBuilder target,
            Map<String, MaskedFieldDefinition> definitions) {
        definitions.entrySet().stream()
                   .sorted(Map.Entry.comparingByKey())
                   .forEach(entry -> {
                       MaskedFieldDefinition definition = entry.getValue();
                       append(target, entry.getKey());
                       append(target, definition.policy());
                       append(target, definition.prefix());
                       append(target, definition.suffix());
                       append(target, definition.display());
                   });
    }

    private static void append(StringBuilder target, Object value) {
        String text = value == null ? "<null>" : value.toString();
        target.append(text.length()).append(':').append(text).append(';');
    }

    private static String identifierPart(String value) {
        return value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                                        .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is required by Java 21", error);
        }
    }
}
