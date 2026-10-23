package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 一次 create-or-alter 调用共享的结构草稿。
 *
 * <p>同步和响应式入口只在怎样执行计划上不同，表、列、索引和外键的描述规则应当完全一致。
 * 本类集中保存这份纯结构状态，不持有连接或执行器，也不会进入普通 CRUD 热路径。</p>
 */
final class DdlStructureDraft {

    private final String table;
    private final List<DynamicField> fields = new ArrayList<>();
    private final List<IndexDefinition> indexes = new ArrayList<>();
    private final List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();
    private SchemaMigrationOptions options = SchemaMigrationOptions.safe();

    DdlStructureDraft(String table) {
        this.table = requireText(table, "table");
    }

    void addColumn(DynamicField field) {
        fields.add(Objects.requireNonNull(field, "dynamic field must not be null"));
    }

    void addIndex(IndexDefinition index) {
        indexes.add(Objects.requireNonNull(index, "index metadata must not be null"));
    }

    void addForeignKey(ForeignKeyDefinition foreignKey) {
        foreignKeys.add(Objects.requireNonNull(foreignKey, "foreign key metadata must not be null"));
    }

    void options(SchemaMigrationOptions value) {
        options = Objects.requireNonNull(value, "schema migration options must not be null");
    }

    DynamicForm form() {
        DynamicForm.Builder builder = DynamicForm.builder(table, table);
        fields.forEach(builder::addField);
        return builder.build();
    }

    List<IndexDefinition> indexes() {
        return indexes;
    }

    List<ForeignKeyDefinition> foreignKeys() {
        return foreignKeys;
    }

    SchemaMigrationOptions options() {
        return options;
    }

    static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

}
