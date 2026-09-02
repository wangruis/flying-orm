package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.protection.ProtectedFormLayout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
                             List<IndexMetadata> indexes,
                             List<ForeignKeyMetadata> foreignKeys) {

    ProtectedSchemaTarget {
        form = Objects.requireNonNull(form, "protected schema form must not be null");
        indexes = List.copyOf(Objects.requireNonNull(indexes, "protected schema indexes must not be null"));
        foreignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "protected schema foreign keys must not be null"));
    }

    /** @return 物理列、自动盲索引和仍然安全的显式结构声明。 */
    static ProtectedSchemaTarget resolve(DynamicForm logical,
                                         List<IndexMetadata> indexes,
                                         List<ForeignKeyMetadata> foreignKeys) {
        DynamicForm safeLogical = Objects.requireNonNull(logical, "target dynamic form must not be null");
        List<IndexMetadata> safeIndexes = List.copyOf(Objects.requireNonNull(
                indexes, "target indexes must not be null"));
        List<ForeignKeyMetadata> safeForeignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "target foreign keys must not be null"));
        if (safeLogical.protections().encryptedFields().isEmpty()) {
            return new ProtectedSchemaTarget(safeLogical, safeIndexes, safeForeignKeys);
        }

        DynamicForm physical = ProtectedFormLayout.physical(safeLogical);
        Map<String, IndexMetadata> merged = new LinkedHashMap<>();
        physical.toTableMetadata().indexes().forEach(index -> merged.put(key(index.name()), index));
        for (IndexMetadata index : safeIndexes) {
            List<DynamicField> encrypted = index.columns().stream()
                                                .map(safeLogical::field)
                                                .filter(field -> safeLogical.protections()
                                                                            .encrypted(field.name()).isPresent())
                                                .toList();
            if (!encrypted.isEmpty()) {
                if (isAutomaticEncryptedUnique(index, encrypted.getFirst())) {
                    continue;
                }
                throw new IllegalArgumentException("database index must not reference an encrypted field");
            }
            IndexMetadata previous = merged.putIfAbsent(key(index.name()), index);
            if (previous != null && !sameIndex(previous, index)) {
                // 自动索引与上层重复传入的同一声明可以合并；同名但不同定义必须在 DDL 前拒绝。
                throw new IllegalArgumentException("duplicate protected schema index name");
            }
        }
        for (ForeignKeyMetadata foreignKey : safeForeignKeys) {
            boolean encrypted = foreignKey.columns().stream()
                                          .map(safeLogical::field)
                                          .anyMatch(field -> safeLogical.protections()
                                                                        .encrypted(field.name()).isPresent());
            if (encrypted) {
                throw new IllegalArgumentException("foreign key must not reference an encrypted field");
            }
        }
        return new ProtectedSchemaTarget(physical, new ArrayList<>(merged.values()), safeForeignKeys);
    }

    /**
     * 已有同名业务列只有在元数据已经呈现为二进制存储时才能直接接入保护协议；文本列必须走显式数据迁移。
     */
    static void validateExistingStorage(TableMetadata current, DynamicForm logical) {
        TableMetadata safeCurrent = Objects.requireNonNull(current, "current table metadata must not be null");
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
        ProtectedFormLayout.physical(safeLogical).fields().stream()
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
    static boolean sameProtectedStorage(ColumnMetadata source, DynamicField target) {
        boolean compatible = ProtectedFormLayout.isCiphertextType(target.databaseType())
                ? source.databaseType().isBinary() && source.length() == null
                : ProtectedFormLayout.isHashType(target.databaseType())
                        && source.databaseType().isBinary()
                        && (source.length() == null || source.length() >= 32);
        return compatible
                && source.primaryKey() == target.primaryKey()
                && source.nullable() == target.nullable()
                && Objects.equals(source.generation(), target.generation());
    }

    private static boolean isAutomaticEncryptedUnique(IndexMetadata index, DynamicField field) {
        return index.unique() && index.columns().size() == 1 && field.unique();
    }

    private static boolean sameIndex(IndexMetadata left, IndexMetadata right) {
        return left.unique() == right.unique()
                && left.columns().stream().map(ProtectedSchemaTarget::key).toList()
                       .equals(right.columns().stream().map(ProtectedSchemaTarget::key).toList());
    }

    private static String key(String name) {
        return name.trim();
    }

}
