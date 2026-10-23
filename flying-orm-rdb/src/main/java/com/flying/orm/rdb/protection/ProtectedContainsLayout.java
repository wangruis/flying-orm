package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.List;
import java.util.HashSet;
import java.util.Locale;
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
                                      List<IndexDefinition> indexes,
                                      List<ForeignKeyDefinition> foreignKeys) {

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
        if (safeForm.protections().encryptedFields().isEmpty()) {
            return Optional.empty();
        }
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
        DynamicForm.Builder table = DynamicForm.relationalBuilder(
                safeForm.id() + "Contains", safeForm.relationIdentity().withTable(tableName));
        primaryKeys.forEach(field -> table.addField(new DynamicField(ownerColumn(field.column()), false, false)));
        List<String> ownerColumns = primaryKeys.stream().map(DynamicField::name).toList();
        String fieldTag = fieldTagColumn(ownerColumns);
        String tokenHash = tokenHashColumn(ownerColumns);
        table.addField(DynamicField.of(fieldTag, "VARCHAR").withLength(30).withNullable(false));
        table.addField(DynamicField.of(tokenHash, ProtectedFormLayout.HASH_TYPE).withNullable(false));

        IndexDefinition.Builder query = IndexDefinition.builder(
                ProtectedColumnNames.containsQueryIndex(safeForm.id(), safeForm.namingSource()))
                                                   .addColumn(fieldTag)
                                                   .addColumn(tokenHash);
        ownerColumns.forEach(query::addColumn);
        IndexDefinition.Builder unique = IndexDefinition.builder(
                ProtectedColumnNames.containsUniqueIndex(safeForm.id(), safeForm.namingSource())).unique();
        ownerColumns.forEach(unique::addColumn);
        unique.addColumn(fieldTag).addColumn(tokenHash);

        ForeignKeyDefinition.Builder foreignKey = ForeignKeyDefinition.builder(
                ProtectedColumnNames.containsForeignKey(safeForm.id(), safeForm.namingSource()))
                .reference(safeForm.relationIdentity())
                .onDelete(ReferentialAction.CASCADE);
        ownerColumns.forEach(column -> foreignKey.addColumn(column).addReferenceColumn(column));
        return Optional.of(new ProtectedContainsLayout(
                table.build(), List.of(query.build(), unique.build()), List.of(foreignKey.build())));
    }

    /** 返回与 owner 主键不冲突的字段标签列；无冲突时保留既有物理名称。 */
    public static String fieldTagColumn(List<String> ownerColumns) {
        return protocolColumn(ownerColumns, "field_tag");
    }

    /** 返回与 owner 主键不冲突的令牌列；无冲突时保留既有物理名称。 */
    public static String tokenHashColumn(List<String> ownerColumns) {
        return protocolColumn(ownerColumns, "token_hash");
    }

    private static String protocolColumn(List<String> ownerColumns, String base) {
        var occupied = new HashSet<String>();
        ownerColumns.forEach(name -> occupied.add(name.toLowerCase(Locale.ROOT)));
        String candidate = base;
        for (int suffix = 1; occupied.contains(candidate); suffix++) {
            candidate = base + "_" + suffix;
        }
        return candidate;
    }

    private static String containsTable(DynamicForm form) {
        return ProtectedColumnNames.containsTable(form.id(), form.namingSource());
    }

    /** 侧表引用已有主键值，保留完整存储类型，不继承主表的生成与默认规则。 */
    static ColumnDefinition ownerColumn(ColumnDefinition source) {
        return source.toBuilder().nullable(false).comment(null)
                .defaultValue(ColumnDefault.none()).defaultConstraintName(null)
                .generation(ValueGeneration.none()).build();
    }
}
