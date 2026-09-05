package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.TablePartitionDefinition;

/**
 * 在任何结构 SQL 生成前校验目标表和约束的方言能力。
 *
 * <p>单个 {@link SchemaOperation} 不携带完整表上下文，因此 CREATE 和 ALTER 必须共用这里的表级
 * 校验。这样新增约束或索引时，不会绕过创建表时已经执行的分区规则。</p>
 */
final class RelationalTableDdlValidator {

    private RelationalTableDdlValidator() {
    }

    static void validate(RelationalTableDefinition table, SchemaDialect dialect) {
        TablePartitionDefinition partition = table.partition().orElse(null);
        if (partition == null) {
            return;
        }
        validatePartitionKeys(table);
        if (dialect.generatedValueStyle() != SchemaDialect.GeneratedValueStyle.POSTGRESQL) {
            throw new UnsupportedOperationException(
                    "table partitioning is supported only by the PostgreSQL schema dialect");
        }
    }

    static void validate(ForeignKeyDefinition foreignKey, SchemaDialect dialect) {
        SchemaDialect.GeneratedValueStyle style = dialect.generatedValueStyle();
        if (style == SchemaDialect.GeneratedValueStyle.MYSQL
                && (foreignKey.onDelete() == ReferentialAction.SET_DEFAULT
                || foreignKey.onUpdate() == ReferentialAction.SET_DEFAULT)) {
            throw new UnsupportedOperationException("mysql does not support SET DEFAULT actions");
        }
        if (style == SchemaDialect.GeneratedValueStyle.ORACLE
                && foreignKey.onUpdate() != ReferentialAction.NO_ACTION) {
            throw new UnsupportedOperationException("oracle does not support ON UPDATE actions");
        }
        if (style == SchemaDialect.GeneratedValueStyle.ORACLE
                && foreignKey.onDelete() == ReferentialAction.SET_DEFAULT) {
            throw new UnsupportedOperationException("oracle does not support ON DELETE SET DEFAULT");
        }
        if ((style == SchemaDialect.GeneratedValueStyle.ORACLE
                || style == SchemaDialect.GeneratedValueStyle.SQL_SERVER)
                && (foreignKey.onDelete() == ReferentialAction.RESTRICT
                || foreignKey.onUpdate() == ReferentialAction.RESTRICT)) {
            throw new UnsupportedOperationException("current dialect does not support RESTRICT actions");
        }
    }

    static void validatePartitionKeys(RelationalTableDefinition table) {
        TablePartitionDefinition partition = table.partition().orElse(null);
        if (partition == null) {
            return;
        }
        String key = partition.column();
        table.primaryKey().ifPresent(primaryKey -> requirePartitionKey(
                primaryKey.columns().contains(key), "primary key"));
        table.uniqueConstraints().forEach(unique -> requirePartitionKey(
                unique.columns().contains(key), "unique constraint"));
        table.indexes().stream().filter(IndexDefinition::unique).forEach(index -> requirePartitionKey(
                index.keys().stream().anyMatch(keyPart -> keyPart.column().equals(key)),
                "unique index"));
    }

    private static void requirePartitionKey(boolean present, String owner) {
        if (!present) {
            throw new UnsupportedOperationException(
                    owner + " on a partitioned table must include the partition column");
        }
    }
}
