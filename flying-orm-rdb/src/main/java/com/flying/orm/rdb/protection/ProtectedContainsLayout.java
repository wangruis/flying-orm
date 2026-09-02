package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 描述单张业务表共用的 CONTAINS 侧索引表。
 *
 * <p>侧表只保存受控字段标签、HMAC 令牌和业务表全部主键列，不保存字段名或明文片段。所有自动名称均为
 * 每个自动生成的本地标识符最多 30 个 ASCII 字符，以兼容项目声明支持的最低 Oracle 标识符边界；
 * 业务表显式声明 Schema 时，侧表必须留在同一 Schema。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
@InternalApi
public record ProtectedContainsLayout(DynamicForm table,
                                      List<IndexMetadata> indexes,
                                      List<ForeignKeyMetadata> foreignKeys) {

    public ProtectedContainsLayout {
        table = Objects.requireNonNull(table, "protected contains table must not be null");
        indexes = List.copyOf(Objects.requireNonNull(indexes, "protected contains indexes must not be null"));
        foreignKeys = List.copyOf(Objects.requireNonNull(
                foreignKeys, "protected contains foreign keys must not be null"));
    }

    /**
     * 为显式启用 CONTAINS 的表创建侧索引布局；未启用时返回空。
     *
     * @param form 逻辑业务表单
     * @return 侧索引布局
     */
    public static Optional<ProtectedContainsLayout> resolve(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        boolean enabled = safeForm.protections().encryptedFields().values().stream()
                                  .anyMatch(definition -> definition.searchModes()
                                                                    .contains(EncryptedSearchMode.CONTAINS));
        if (!enabled) {
            return Optional.empty();
        }
        List<DynamicField> primaryKeys = safeForm.fields().stream()
                                                 .filter(DynamicField::primaryKey)
                                                 .toList();
        if (primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("protected contains search requires a primary key");
        }
        String tableName = containsTable(safeForm);
        DynamicForm.Builder table = DynamicForm.builder(safeForm.id() + "Contains", tableName);
        primaryKeys.forEach(field -> table.addField(ownerColumn(field)));
        table.addField(DynamicField.of("field_tag", "VARCHAR").withLength(30).withNullable(false));
        table.addField(DynamicField.of("token_hash", ProtectedFormLayout.HASH_TYPE).withNullable(false));

        List<String> ownerColumns = primaryKeys.stream().map(DynamicField::name).toList();
        IndexMetadata.Builder query = IndexMetadata.builder(
                ProtectedColumnNames.containsQueryIndex(safeForm.id(), safeForm.table()))
                                                   .addColumn("field_tag")
                                                   .addColumn("token_hash");
        ownerColumns.forEach(query::addColumn);
        IndexMetadata.Builder unique = IndexMetadata.builder(
                ProtectedColumnNames.containsUniqueIndex(safeForm.id(), safeForm.table())).unique();
        ownerColumns.forEach(unique::addColumn);
        unique.addColumn("field_tag").addColumn("token_hash");

        ForeignKeyMetadata foreignKey = new ForeignKeyMetadata(
                ProtectedColumnNames.containsForeignKey(safeForm.id(), safeForm.table()),
                ownerColumns,
                safeForm.table(),
                ownerColumns);
        return Optional.of(new ProtectedContainsLayout(
                table.build(), List.of(query.build(), unique.build()), List.of(foreignKey)));
    }

    private static String containsTable(DynamicForm form) {
        String businessTable = form.table();
        String generatedName = ProtectedColumnNames.containsTable(form.id(), businessTable);
        int separator = businessTable.lastIndexOf('.');
        return separator < 0 ? generatedName : businessTable.substring(0, separator + 1) + generatedName;
    }

    private static DynamicField ownerColumn(DynamicField field) {
        return new DynamicField(field.name(), field.databaseType(), false, false, false,
                                field.length(), field.precision(), field.scale(), null, ValueGeneration.none());
    }
}
