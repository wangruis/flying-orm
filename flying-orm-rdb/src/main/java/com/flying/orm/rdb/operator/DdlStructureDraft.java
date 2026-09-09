package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
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
    private final List<IndexMetadata> indexes = new ArrayList<>();
    private final List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
    private SchemaMigrationOptions options = SchemaMigrationOptions.safe();

    DdlStructureDraft(String table) {
        this.table = requireText(table, "table");
    }

    ColumnDraft column() {
        return new ColumnDraft(this);
    }

    void addIndex(IndexMetadata index) {
        indexes.add(Objects.requireNonNull(index, "index metadata must not be null"));
    }

    void addForeignKey(ForeignKeyMetadata foreignKey) {
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

    List<IndexMetadata> indexes() {
        return indexes;
    }

    List<ForeignKeyMetadata> foreignKeys() {
        return foreignKeys;
    }

    boolean hasForeignKeys() {
        return !foreignKeys.isEmpty();
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

    /** 当前一列的临时描述；只有 commit 后才加入所属表草稿。 */
    static final class ColumnDraft {

        private final DdlStructureDraft owner;
        private String name;
        private String dataType;
        private boolean primaryKey;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private String comment;

        private ColumnDraft(DdlStructureDraft owner) {
            this.owner = owner;
        }

        void name(String value) {
            name = requireText(value, "column name");
        }

        void number(int value) {
            if (value < 1) {
                throw new IllegalArgumentException("number precision must be positive");
            }
            dataType = value <= 10 ? "INTEGER" : value <= 19 ? "BIGINT" : "DECIMAL";
            if ("DECIMAL".equals(dataType)) {
                precision = value;
                scale = 0;
            }
        }

        void varchar(int value) {
            if (value < 1) {
                throw new IllegalArgumentException("varchar length must be positive");
            }
            dataType = "VARCHAR";
            length = value;
        }

        void primaryKey() {
            primaryKey = true;
        }

        void comment(String value) {
            comment = requireText(value, "column comment");
        }

        void commit() {
            String safeName = requireText(name, "column name");
            String safeType = requireText(dataType, "column data type");
            DynamicField field = primaryKey
                    ? DynamicField.primaryKey(safeName, safeType)
                    : DynamicField.of(safeName, safeType);
            owner.fields.add(field.withLength(length).withPrecision(precision, scale).withComment(comment));
        }
    }
}
