package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicForm;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 规范关系模型与旧轻量元数据之间的显式兼容适配器。
 *
 * <p>这些方法只应由 Schema、DDL 或迁移等冷路径调用。普通 CRUD 不保存规范关系对象，也不会因本适配器
 * 多一次分配、扫描或摘要计算。旧模型无法表达的 catalog、排序方向、外键动作、默认值和 CHECK 等信息，
 * 转回旧视图时会自然收窄；规范模型本身不会因此丢失信息。</p>
 *
 * @author wangr
 * @date 2026-09-03
 * @version v3.2
 */
public final class RelationalMetadataAdapter {

    private RelationalMetadataAdapter() {
    }

    /** 把旧动态表单当前可表达的关系子集转成规范模型。 */
    public static RelationalTableDefinition from(DynamicForm form) {
        DynamicForm source = Objects.requireNonNull(form, "dynamic form must not be null");
        RelationIdentity identity = source.relationIdentity()
                .orElseGet(() -> RelationIdentity.table(source.table()));
        return from(source.toTableMetadata(), identity);
    }

    /** 把旧表元数据当前可表达的关系子集转成规范模型。 */
    public static RelationalTableDefinition from(TableMetadata metadata) {
        TableMetadata source = Objects.requireNonNull(metadata, "table metadata must not be null");
        return from(source, RelationIdentity.table(source.name()));
    }

    private static RelationalTableDefinition from(TableMetadata source, RelationIdentity identity) {
        RelationalTableDefinition.Builder target = RelationalTableDefinition.builder(identity);

        for (ColumnMetadata column : source.columns()) {
            target.addColumn(toColumnDefinition(column));
        }
        if (!source.primaryKeyColumns().isEmpty()) {
            target.primaryKey(PrimaryKeyDefinition.of(
                    legacyPrimaryKeyName(source.name()),
                    source.primaryKeyColumns().stream().map(ColumnMetadata::name).toArray(String[]::new)));
        }
        for (IndexMetadata index : source.indexes()) {
            // 旧模型明确说这是 IndexMetadata。唯一性只是索引属性，不能在往返时擅自改成唯一约束。
            IndexDefinition.Builder definition = IndexDefinition.builder(index.name()).unique(index.unique());
            index.columns().forEach(column -> definition.addKey(IndexKeyPart.asc(column)));
            target.addIndex(definition.build());
        }
        for (ForeignKeyMetadata foreignKey : source.foreignKeys()) {
            ForeignKeyDefinition.Builder definition = ForeignKeyDefinition.builder(foreignKey.name())
                    .reference(RelationIdentity.table(foreignKey.referenceTable()));
            foreignKey.columns().forEach(definition::addColumn);
            foreignKey.referenceColumns().forEach(definition::addReferenceColumn);
            target.addForeignKey(definition.build());
        }
        return target.build();
    }

    /**
     * 生成现有 CRUD 与旧 Schema 代码可直接消费的轻量视图。
     *
     * <p>这不是第二份事实来源；每次都从传入的规范快照投影，且不在任一对象上挂 side map。</p>
     */
    public static TableMetadata toTableMetadata(RelationalTableDefinition definition) {
        RelationalTableDefinition source = Objects.requireNonNull(
                definition, "relational table definition must not be null");
        TableMetadata.Builder target = TableMetadata.builder(source.identity().table());

        Set<String> primaryColumns = new HashSet<>();
        source.primaryKey().ifPresent(primaryKey -> {
            for (String name : primaryKey.columns()) {
                primaryColumns.add(source.column(name).name());
            }
        });
        for (ColumnDefinition column : source.columns()) {
            target.addColumn(toColumnMetadata(column, primaryColumns.contains(column.name())));
        }
        for (UniqueConstraintDefinition unique : source.uniqueConstraints()) {
            IndexMetadata.Builder index = IndexMetadata.builder(unique.name()).unique();
            unique.columns().forEach(index::addColumn);
            target.addIndex(index.build());
        }
        for (IndexDefinition index : source.indexes()) {
            IndexMetadata.Builder legacy = IndexMetadata.builder(index.name());
            if (index.unique()) {
                legacy.unique();
            }
            index.keys().forEach(key -> legacy.addColumn(key.column()));
            target.addIndex(legacy.build());
        }
        for (ForeignKeyDefinition foreignKey : source.foreignKeys()) {
            ForeignKeyMetadata.Builder legacy = ForeignKeyMetadata.builder(foreignKey.name())
                    .referenceTable(foreignKey.reference().table());
            foreignKey.columns().forEach(legacy::addColumn);
            foreignKey.referenceColumns().forEach(legacy::addReferenceColumn);
            target.addForeignKey(legacy.build());
        }
        return target.build();
    }

    private static ColumnDefinition toColumnDefinition(ColumnMetadata column) {
        ColumnDefinition.Builder builder = ColumnDefinition.builder(column.name(), column.dataType())
                .nullable(column.nullable())
                .length(column.length())
                .comment(column.comment())
                .generation(column.generation());
        if (column.databaseType().logicalType().temporal()) {
            builder.temporalPrecision(column.precision());
        } else {
            builder.precision(column.precision()).scale(column.scale());
        }
        return builder.build();
    }

    private static ColumnMetadata toColumnMetadata(ColumnDefinition column, boolean primaryKey) {
        Integer legacyPrecision = column.databaseType().logicalType().temporal()
                ? column.temporalPrecision()
                : column.precision();
        return new ColumnMetadata(column.name(),
                                  column.databaseType().declaration(),
                                  primaryKey,
                                  column.nullable(),
                                  column.length(),
                                  legacyPrecision,
                                  column.scale(),
                                  column.comment(),
                                  column.generation());
    }

    private static String legacyPrimaryKeyName(String tableName) {
        return "pk_" + tableName;
    }
}
