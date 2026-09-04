package com.flying.orm.core.form;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.protection.MaskedFieldDefinition;

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
    private static final StableDigest.Domain UNIQUE_INDEX_DOMAIN =
            StableDigest.domain("dynamic-form-unique-index/v1");
    private static final StableDigest.Domain STRUCTURE_DOMAIN =
            StableDigest.domain("dynamic-form-structure/v1");

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
        String suffix = StableDigest.sha256(UNIQUE_INDEX_DOMAIN)
                                    .text("TABLE", tableSource)
                                    .text("COLUMN", columnSource)
                                    .finishHex()
                                    .substring(0, 24);
        if (base.length() + suffix.length() + 1 <= MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH) {
            return base + "_" + suffix;
        }
        return base.substring(0, MAX_AUTO_UNIQUE_INDEX_NAME_LENGTH - suffix.length() - 1) + "_" + suffix;
    }

    static String fingerprint(RelationIdentity identity,
                              List<DynamicField> fields,
                              LogicDeleteDefinition logicDelete,
                              TenantDefinition tenant,
                              FieldProtectionRegistry protections) {
        StableEncoder shape = StableDigest.sha256(STRUCTURE_DOMAIN)
                                          .marker("SEGMENTED_RELATION");
        identity.catalog().ifPresent(value -> shape.text("CATALOG", value));
        identity.schema().ifPresent(value -> shape.text("SCHEMA", value));
        shape.text("TABLE", identity.table());
        return fingerprint(shape, fields, logicDelete, tenant, protections);
    }

    static String fingerprint(String table,
                              List<DynamicField> fields,
                              LogicDeleteDefinition logicDelete,
                              TenantDefinition tenant,
                              FieldProtectionRegistry protections) {
        StableEncoder shape = StableDigest.sha256(STRUCTURE_DOMAIN)
                                          .text("TABLE", table);
        return fingerprint(shape, fields, logicDelete, tenant, protections);
    }

    private static String fingerprint(StableEncoder shape,
                                      List<DynamicField> fields,
                                      LogicDeleteDefinition logicDelete,
                                      TenantDefinition tenant,
                                      FieldProtectionRegistry protections) {
        for (DynamicField field : fields) {
            shape.marker("FIELD")
                 .text("FIELD_NAME", field.name().trim())
                 .text("FIELD_TYPE", field.databaseType().canonical())
                 .bool("FIELD_PRIMARY_KEY", field.primaryKey())
                 .bool("FIELD_NULLABLE", field.nullable())
                 .bool("FIELD_UNIQUE", field.unique())
                 .nullableInteger("FIELD_LENGTH", field.length())
                 .nullableInteger("FIELD_PRECISION", field.precision())
                 .nullableInteger("FIELD_SCALE", field.scale())
                 .text("GENERATION_STRATEGY", field.generation().strategy().name())
                 .nullableText("GENERATION_SEQUENCE", field.generation().sequenceName())
                 .integer("GENERATION_START", field.generation().startWith())
                 .integer("GENERATION_INCREMENT", field.generation().incrementBy())
                 .integer("GENERATION_CACHE", field.generation().cacheSize());
        }
        shape.nullableText("LOGIC_DELETE_FIELD", logicDelete == null ? null : logicDelete.identity().key())
             .nullableText("TENANT_FIELD", tenant == null ? null : tenant.identity().key())
             .nullableText("TENANT_STRATEGY", tenant == null ? null : tenant.strategy().name());
        appendEncryptedFields(shape, protections.encryptedFields());
        appendMaskedFields(shape, protections.maskedFields());
        return shape.finishHex();
    }

    /**
     * Map 与 Set 的迭代顺序不属于结构契约，必须按字段名和枚举名规范化后再参与稳定指纹。
     */
    private static void appendEncryptedFields(
            StableEncoder target,
            Map<String, EncryptedFieldDefinition> definitions) {
        definitions.entrySet().stream()
                   .sorted(Map.Entry.comparingByKey())
                   .forEach(entry -> {
                       EncryptedFieldDefinition definition = entry.getValue();
                       target.marker("ENCRYPTED_FIELD")
                             .text("ENCRYPTED_FIELD_NAME", entry.getKey());
                       definition.searchModes().stream()
                                 .map(Enum::name)
                                 .sorted()
                                 .forEach(mode -> target.text("ENCRYPTED_SEARCH_MODE", mode));
                       target.text("ENCRYPTED_NORMALIZER", definition.normalizer());
                       definition.suffixLengths().forEach(length ->
                               target.integer("ENCRYPTED_SUFFIX_LENGTH", length));
                       target.integer("ENCRYPTED_MAX_LENGTH", definition.maxNormalizedLength())
                             .integer("ENCRYPTED_CONTAINS_MIN_LENGTH", definition.containsMinLength());
                   });
    }

    /** 按字段名和声明属性写入脱敏结构，避免不可变 Map 的随机化迭代顺序污染指纹。 */
    private static void appendMaskedFields(
            StableEncoder target,
            Map<String, MaskedFieldDefinition> definitions) {
        definitions.entrySet().stream()
                   .sorted(Map.Entry.comparingByKey())
                   .forEach(entry -> {
                       MaskedFieldDefinition definition = entry.getValue();
                       target.marker("MASKED_FIELD")
                             .text("MASKED_FIELD_NAME", entry.getKey())
                             .text("MASKED_POLICY", definition.policy())
                             .integer("MASKED_PREFIX", definition.prefix())
                             .integer("MASKED_SUFFIX", definition.suffix())
                             .text("MASKED_DISPLAY", definition.display().name());
                   });
    }

    private static String identifierPart(String value) {
        return value.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]+", "_");
    }
}
