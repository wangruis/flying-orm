package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.protection.ProtectedRelationalSchemaProjector;

import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把公开逻辑表单和索引声明收敛为 Schema 引擎唯一可见的物理目标。
 *
 * <p>加密列本身是随机密文，不能继续承载业务唯一索引或外键。单列唯一约束由物理表单自动迁移到 EXACT
 * 盲索引；其他直接引用加密列的结构声明在生成 DDL 前拒绝，避免创建无效且具有误导性的数据库约束。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
record ProtectedSchemaTarget(DynamicForm form,
                             List<IndexDefinition> indexes,
                             List<ForeignKeyDefinition> foreignKeys) {

    ProtectedSchemaTarget {
        form = Objects.requireNonNull(form, "protected schema form must not be null");
        indexes = List.copyOf(Objects.requireNonNull(indexes, "protected schema indexes must not be null"));
        foreignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "protected schema foreign keys must not be null"));
    }

    /** @return 物理列、自动盲索引和仍然安全的显式结构声明。 */
    static ProtectedSchemaTarget resolve(DynamicForm logical,
                                         List<IndexDefinition> indexes,
                                         List<ForeignKeyDefinition> foreignKeys) {
        DynamicForm safeLogical = Objects.requireNonNull(logical, "dynamic form must not be null");
        List<IndexDefinition> safeIndexes = Objects.requireNonNull(indexes, "target indexes must not be null");
        List<ForeignKeyDefinition> safeForeignKeys = Objects.requireNonNull(
                foreignKeys, "target foreign keys must not be null");
        if (safeLogical.protections().encryptedFields().isEmpty()
                && safeLogical.fields().stream().noneMatch(DynamicField::unique)) {
            return new ProtectedSchemaTarget(safeLogical, safeIndexes, safeForeignKeys);
        }

        DynamicForm physical = ProtectedFormLayout.physical(safeLogical);
        Map<String, IndexDefinition> merged = new LinkedHashMap<>();
        Set<String> automaticIndexes = new HashSet<>();
        physical.toTableDefinition().indexes().forEach(index -> {
            String name = key(index.name());
            automaticIndexes.add(name);
            merged.put(name, index);
        });
        for (IndexDefinition index : safeIndexes) {
            IndexDefinition projected = ProtectedRelationalSchemaProjector.projectIndex(safeLogical, index);
            if (index.unique()) {
                merged.entrySet().removeIf(entry -> automaticIndexes.contains(entry.getKey())
                        && sameUniqueColumns(entry.getValue(), projected));
            }
            IndexDefinition previous = merged.putIfAbsent(key(projected.name()), projected);
            if (previous != null && !sameIndex(previous, projected)) {
                // 自动索引与上层重复传入的同一声明可以合并；同名但不同定义必须在 DDL 前拒绝。
                throw new IllegalArgumentException("duplicate protected schema index name");
            }
        }
        for (ForeignKeyDefinition foreignKey : safeForeignKeys) {
            boolean encrypted = foreignKey.columns().stream()
                                          .map(safeLogical::field)
                                          .anyMatch(field -> safeLogical.protections()
                                                                        .encrypted(field.name()).isPresent());
            if (encrypted) {
                throw new IllegalArgumentException("foreign key must not reference an encrypted field");
            }
        }
        return new ProtectedSchemaTarget(physical, List.copyOf(merged.values()), safeForeignKeys);
    }

    /**
     * 已有同名业务列只有在元数据已经呈现为二进制存储时才能直接接入保护协议；文本列必须走显式数据迁移。
     */
    void validateExistingStorage(RelationalTableDefinition current, DynamicForm logical) {
        RelationalTableDefinition safeCurrent = Objects.requireNonNull(current, "current table metadata must not be null");
        DynamicForm safeLogical = Objects.requireNonNull(logical, "target dynamic form must not be null");
        safeLogical.protections().encryptedFields().keySet().forEach(fieldName ->
                safeCurrent.findColumn(fieldName).ifPresent(column -> {
                    if (!column.databaseType().isBinary() || column.length() != null) {
                        throw new IllegalArgumentException(
                                "encrypted field storage requires an explicit plaintext migration");
                    }
                }));
        if (safeLogical.protections().encryptedFields().isEmpty()) {
            return;
        }
        form.fields().stream()
                           .filter(field -> ProtectedFormLayout.isHashType(field.databaseType()))
                           .forEach(field -> safeCurrent.findColumn(field.name()).ifPresent(column -> {
                               if (!column.databaseType().isBinary()
                                       || column.length() != null && column.length() < 32) {
                                   throw new IllegalArgumentException(
                                           "protected search hash storage must hold at least 32 bytes");
                               }
                           }));
    }

    /**
     * Metadata reader 会把各数据库的 BLOB/BYTEA/VARBINARY/RAW 统一为 BLOB，不能再与内部伪类型逐字比较。
     */
    static boolean sameProtectedStorage(ColumnDefinition source, DynamicField target) {
        boolean compatible = ProtectedFormLayout.isCiphertextType(target.databaseType())
                ? source.databaseType().isBinary() && source.length() == null
                : ProtectedFormLayout.isHashType(target.databaseType())
                        && source.databaseType().isBinary()
                        && (source.length() == null || source.length() >= 32);
        return compatible
                && source.nullable() == target.nullable()
                && Objects.equals(source.generation(), target.generation());
    }

    private static boolean sameUniqueColumns(IndexDefinition left, IndexDefinition right) {
        return left.unique() && right.unique()
                && left.columns().stream().map(ProtectedSchemaTarget::key).toList()
                .equals(right.columns().stream().map(ProtectedSchemaTarget::key).toList());
    }

    private static boolean sameIndex(IndexDefinition left, IndexDefinition right) {
        return left.unique() == right.unique() && left.keys().equals(right.keys());
    }

    private static String key(String name) {
        return name.trim();
    }

}
