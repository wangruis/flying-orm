package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * 生成覆盖 DDL 语义的稳定 SHA-256 指纹。
 *
 * <p>列和复合键内部的顺序属于关系合同，按声明顺序编码；命名约束、索引、外键和 CHECK 是集合，
 * 按名称排序后编码，因此仅改变构建器调用顺序不会制造无意义的迁移差异。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class RelationalMetadataFingerprint {

    private static final StableDigest.Domain DOMAIN = StableDigest.domain("relational-metadata/v2");
    private static final StableDigest.Domain SCHEMA_DOMAIN = StableDigest.domain("relational-schema/v2");

    private RelationalMetadataFingerprint() {
    }

    /** Schema 摘要包含命名空间和按名称排序的全部表；空集合也保留其命名空间。 */
    public static String of(RelationalSchemaDefinition schema) {
        RelationalSchemaDefinition source = Objects.requireNonNull(schema, "relational schema must not be null");
        StableEncoder encoder = StableDigest.sha256(SCHEMA_DOMAIN)
                .nullableText("CATALOG", source.catalog().orElse(null))
                .nullableText("SCHEMA", source.schema().orElse(null))
                .integer("TABLE_COUNT", source.tables().size());
        for (RelationalTableDefinition table : sorted(source.tables(), RelationalTableDefinition::name)) {
            encoder.text("TABLE_FINGERPRINT", of(table));
        }
        return encoder.finishHex();
    }

    /** @return 64 个小写十六进制字符组成的稳定关系指纹 */
    public static String of(RelationalTableDefinition table) {
        RelationalTableDefinition source = Objects.requireNonNull(
                table, "relational table definition must not be null");
        StableEncoder encoder = StableDigest.sha256(DOMAIN);
        encodeIdentity(encoder, "TABLE", source.identity());
        encoder.nullableText("TABLE_COMMENT", source.comment());

        encoder.integer("COLUMN_COUNT", source.columns().size());
        for (ColumnDefinition column : source.columns()) {
            encodeColumn(encoder, column);
        }

        encoder.bool("HAS_PRIMARY_KEY", source.primaryKey().isPresent());
        source.primaryKey().ifPresent(primaryKey -> encodeColumns(
                encoder, "PRIMARY_KEY", primaryKey.name(), primaryKey.columns()));

        encodeUniques(encoder, source.uniqueConstraints());

        List<IndexDefinition> indexes = sorted(source.indexes(), IndexDefinition::name);
        encoder.integer("INDEX_COUNT", indexes.size());
        for (IndexDefinition index : indexes) {
            encoder.marker("INDEX").text("INDEX_NAME", index.name()).bool("INDEX_UNIQUE", index.unique());
            encoder.integer("INDEX_KEY_COUNT", index.keys().size());
            for (IndexKeyPart key : index.keys()) {
                encoder.text("INDEX_COLUMN", key.column())
                       .text("INDEX_DIRECTION", key.direction().name());
            }
        }

        List<ForeignKeyDefinition> foreignKeys = sorted(source.foreignKeys(), ForeignKeyDefinition::name);
        encoder.integer("FOREIGN_KEY_COUNT", foreignKeys.size());
        for (ForeignKeyDefinition foreignKey : foreignKeys) {
            encoder.marker("FOREIGN_KEY").text("FOREIGN_KEY_NAME", foreignKey.name());
            encodeStringList(encoder, "FOREIGN_KEY_COLUMN", foreignKey.columns());
            encodeIdentity(encoder, "FOREIGN_KEY_REFERENCE", foreignKey.reference());
            encodeStringList(encoder, "FOREIGN_KEY_REFERENCE_COLUMN", foreignKey.referenceColumns());
            encoder.text("FOREIGN_KEY_ON_DELETE", foreignKey.onDelete().name())
                   .text("FOREIGN_KEY_ON_UPDATE", foreignKey.onUpdate().name());
        }

        List<CheckConstraintDefinition> checks = sorted(source.checks(), CheckConstraintDefinition::name);
        encoder.integer("CHECK_COUNT", checks.size());
        for (CheckConstraintDefinition check : checks) {
            encoder.marker("CHECK").text("CHECK_NAME", check.name());
            encodePredicate(encoder, check.predicate());
        }
        source.partition().ifPresent(partition -> encoder.marker("TABLE_PARTITION")
                .text("TABLE_PARTITION_STRATEGY", partition.strategy().name())
                .text("TABLE_PARTITION_COLUMN", partition.column()));
        return encoder.finishHex();
    }

    /** 将完整物理列声明写入结构摘要，供表和实体模型共用同一编码规则。 */
    public static void encodeColumn(StableEncoder encoder, ColumnDefinition column) {
        encoder.marker("COLUMN")
               .text("COLUMN_NAME", column.name())
               .text("COLUMN_TYPE", column.databaseType().canonical())
               .bool("COLUMN_NULLABLE", column.nullable())
               .nullableInteger("COLUMN_LENGTH", column.length())
               .nullableInteger("COLUMN_PRECISION", column.precision())
               .nullableInteger("COLUMN_SCALE", column.scale())
               .nullableInteger("COLUMN_TEMPORAL_PRECISION", column.temporalPrecision())
               .nullableText("COLUMN_COMMENT", column.comment())
               .text("COLUMN_GENERATION_STRATEGY", column.generation().strategy().name())
               .nullableText("COLUMN_SEQUENCE", column.generation().sequenceName())
               .integer("COLUMN_GENERATION_START", column.generation().startWith())
               .integer("COLUMN_GENERATION_INCREMENT", column.generation().incrementBy())
               .integer("COLUMN_GENERATION_CACHE", column.generation().cacheSize())
               .nullableText("COLUMN_CHARSET", column.charset())
               .nullableText("COLUMN_COLLATION", column.collation());
        encodeDefault(encoder, column.defaultValue());
        encoder.nullableText("COLUMN_DEFAULT_CONSTRAINT_NAME", column.defaultConstraintName());
    }

    private static void encodeDefault(StableEncoder encoder, ColumnDefault value) {
        encoder.text("COLUMN_DEFAULT_KIND", value.kind().name());
        if (value.value().isPresent()) {
            encoder.bool("COLUMN_DEFAULT_HAS_VALUE", true);
            encodeLiteral(encoder, "COLUMN_DEFAULT_VALUE", value.value().orElseThrow());
        } else {
            encoder.bool("COLUMN_DEFAULT_HAS_VALUE", false);
        }
    }

    private static void encodePredicate(StableEncoder encoder, CheckPredicate predicate) {
        switch (predicate) {
            case CheckPredicate.Comparison comparison -> {
                encoder.marker("CHECK_COMPARISON")
                       .text("CHECK_COLUMN", comparison.column())
                       .text("CHECK_OPERATOR", comparison.operator().name());
                encodeLiteral(encoder, "CHECK_VALUE", comparison.value());
            }
            case CheckPredicate.Range range -> {
                encoder.marker("CHECK_RANGE")
                       .text("CHECK_COLUMN", range.column())
                       .bool("CHECK_LOWER_INCLUSIVE", range.lowerInclusive())
                       .bool("CHECK_UPPER_INCLUSIVE", range.upperInclusive());
                encodeLiteral(encoder, "CHECK_LOWER", range.lower());
                encodeLiteral(encoder, "CHECK_UPPER", range.upper());
            }
            case CheckPredicate.In in -> {
                encoder.marker("CHECK_IN").text("CHECK_COLUMN", in.column())
                       .integer("CHECK_VALUE_COUNT", in.values().size());
                for (Object value : in.values()) {
                    encodeLiteral(encoder, "CHECK_VALUE", value);
                }
            }
            case CheckPredicate.NullCheck nullCheck -> encoder.marker("CHECK_NULL")
                    .text("CHECK_COLUMN", nullCheck.column())
                    .bool("CHECK_NEGATED", nullCheck.negated());
            case CheckPredicate.Logical logical -> {
                encoder.marker("CHECK_LOGICAL")
                       .text("CHECK_LOGICAL_OPERATOR", logical.operator().name())
                       .integer("CHECK_PREDICATE_COUNT", logical.predicates().size());
                for (CheckPredicate child : logical.predicates()) {
                    encodePredicate(encoder, child);
                }
            }
            case CheckPredicate.Negation negation -> {
                encoder.marker("CHECK_NEGATION");
                encodePredicate(encoder, negation.predicate());
            }
        }
    }

    private static void encodeLiteral(StableEncoder encoder, String tag, Object value) {
        // ColumnDefault 与 CheckPredicate 已规范化为非空、不可变标量，不再兼容未进入模型的旧值形状。
        encoder.text(tag + "_TYPE", value.getClass().getName()).text(tag, value.toString());
    }

    private static void encodeIdentity(StableEncoder encoder, String marker, RelationIdentity identity) {
        encoder.marker(marker)
               .nullableText(marker + "_CATALOG", identity.catalog().orElse(null))
               .nullableText(marker + "_SCHEMA", identity.schema().orElse(null))
               .text(marker + "_TABLE", identity.table());
    }

    private static void encodeUniques(StableEncoder encoder, List<UniqueConstraintDefinition> values) {
        List<UniqueConstraintDefinition> ordered = sorted(values, UniqueConstraintDefinition::name);
        encoder.integer("UNIQUE_COUNT", ordered.size());
        for (UniqueConstraintDefinition value : ordered) {
            encodeColumns(encoder, "UNIQUE", value.name(), value.columns());
            encoder.text("UNIQUE_NULL_POLICY", value.nullPolicy().name());
        }
    }

    private static void encodeColumns(StableEncoder encoder,
                                      String marker,
                                      String name,
                                      List<String> columns) {
        encoder.marker(marker).text(marker + "_NAME", name);
        encodeStringList(encoder, marker + "_COLUMN", columns);
    }

    private static void encodeStringList(StableEncoder encoder, String tag, List<String> values) {
        encoder.integer(tag + "_COUNT", values.size());
        for (String value : values) {
            encoder.text(tag, value);
        }
    }

    private static <T> List<T> sorted(List<T> values, java.util.function.Function<T, String> name) {
        return values.stream().sorted(Comparator.comparing(name)).toList();
    }
}
