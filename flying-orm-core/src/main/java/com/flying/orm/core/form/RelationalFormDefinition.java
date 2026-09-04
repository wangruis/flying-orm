package com.flying.orm.core.form;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * 同时发布动态表单和完整关系表定义，避免调用方分别维护两份容易漂移的元数据。
 *
 * <p>这是显式的 Schema 冷路径对象。普通 CRUD 仍然直接使用 {@link DynamicForm}，不会因为没有启用
 * 完整关系元数据而多分配对象或多做校验。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class RelationalFormDefinition {

    private final DynamicForm form;

    private final RelationalTableDefinition table;

    private RelationalFormDefinition(DynamicForm form, RelationalTableDefinition table) {
        this.form = form;
        this.table = table;
    }

    /**
     * 创建完整表单定义构建器。
     *
     * @param formId 表单业务 ID
     * @param identity 分段保存的物理表身份
     * @return 新构建器
     */
    public static Builder builder(String formId, RelationIdentity identity) {
        return new Builder(formId, identity);
    }

    /** @return 供既有 CRUD 链路直接使用的只读表单投影 */
    public DynamicForm form() {
        return form;
    }

    /** @return 供 Schema、迁移和能力协商使用的规范关系表定义 */
    public RelationalTableDefinition table() {
        return table;
    }

    /**
     * 构建器只在完整元数据入口工作。字段与列在加入时校验一次，build 不再重复解析类型或扫描名称。
     */
    public static final class Builder {

        private final String formId;

        private final RelationIdentity identity;

        private final List<FieldColumnPair> fields = new ArrayList<>();

        private PrimaryKeyDefinition primaryKey;

        private final List<UniqueConstraintDefinition> uniqueConstraints = new ArrayList<>();

        private final List<IndexDefinition> indexes = new ArrayList<>();

        private final List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();

        private final List<CheckConstraintDefinition> checks = new ArrayList<>();

        private Builder(String formId, RelationIdentity identity) {
            this.formId = Objects.requireNonNull(formId, "form id must not be null");
            this.identity = Objects.requireNonNull(identity, "relation identity must not be null");
        }

        /**
         * 同时加入一个 CRUD 字段和它的规范列定义。
         *
         * <p>两者共同表达的物理属性必须一致。codec、默认值、字符集和排序规则等只有关系模型才表达的
         * 属性不在这里伪造旧模型状态。</p>
         */
        public Builder addField(DynamicField field, ColumnDefinition column) {
            DynamicField safeField = Objects.requireNonNull(field, "dynamic field must not be null");
            ColumnDefinition safeColumn = Objects.requireNonNull(column, "column definition must not be null");
            requireAligned(safeField, safeColumn);
            fields.add(new FieldColumnPair(safeField, safeColumn));
            return this;
        }

        /** @return 当前构建器 */
        public Builder primaryKey(PrimaryKeyDefinition definition) {
            this.primaryKey = Objects.requireNonNull(definition, "primary key must not be null");
            return this;
        }

        /** @return 当前构建器 */
        public Builder unique(UniqueConstraintDefinition definition) {
            uniqueConstraints.add(Objects.requireNonNull(definition, "unique constraint must not be null"));
            return this;
        }

        /** @return 当前构建器 */
        public Builder index(IndexDefinition definition) {
            indexes.add(Objects.requireNonNull(definition, "index must not be null"));
            return this;
        }

        /** @return 当前构建器 */
        public Builder foreignKey(ForeignKeyDefinition definition) {
            foreignKeys.add(Objects.requireNonNull(definition, "foreign key must not be null"));
            return this;
        }

        /** @return 当前构建器 */
        public Builder check(CheckConstraintDefinition definition) {
            checks.add(Objects.requireNonNull(definition, "check constraint must not be null"));
            return this;
        }

        /**
         * 一次构建两份彼此对齐的只读投影。
         *
         * @return 完整表单定义
         */
        public RelationalFormDefinition build() {
            DynamicForm.Builder formBuilder = DynamicForm.relationalBuilder(formId, identity);
            RelationalTableDefinition.Builder tableBuilder = RelationalTableDefinition.builder(identity);
            for (FieldColumnPair pair : fields) {
                formBuilder.addField(pair.field());
                tableBuilder.addColumn(pair.column());
            }
            if (primaryKey != null) {
                tableBuilder.primaryKey(primaryKey);
            }
            uniqueConstraints.forEach(tableBuilder::addUnique);
            indexes.forEach(tableBuilder::addIndex);
            foreignKeys.forEach(tableBuilder::addForeignKey);
            checks.forEach(tableBuilder::addCheck);

            DynamicForm builtForm = formBuilder.build();
            RelationalTableDefinition builtTable = tableBuilder.build();
            requireProjectionAligned(builtForm, builtTable);
            return new RelationalFormDefinition(builtForm, builtTable);
        }

        private static void requireAligned(DynamicField field, ColumnDefinition column) {
            Integer columnPrecision = field.databaseType().isTemporal()
                    ? column.temporalPrecision()
                    : column.precision();
            boolean aligned = field.name().equals(column.name())
                    && field.databaseType().equals(column.databaseType())
                    && field.nullable() == column.nullable()
                    && Objects.equals(field.length(), column.length())
                    && Objects.equals(field.precision(), columnPrecision)
                    && Objects.equals(field.scale(), column.scale())
                    && Objects.equals(field.comment(), column.comment())
                    && field.generation().equals(column.generation());
            if (!aligned) {
                throw new IllegalArgumentException("dynamic field and relational column must describe the same column");
            }
        }

        private static void requireProjectionAligned(DynamicForm form, RelationalTableDefinition table) {
            List<String> formKeys = form.fields().stream()
                    .filter(DynamicField::primaryKey)
                    .map(DynamicField::name)
                    .toList();
            List<String> tableKeys = table.primaryKey()
                    .map(PrimaryKeyDefinition::columns)
                    .orElseGet(List::of);
            if (!samePhysicalColumns(formKeys, tableKeys)) {
                throw new IllegalArgumentException("dynamic form and relational table must declare the same primary key");
            }

            Set<String> formUniqueColumns = new HashSet<>();
            form.fields().stream()
                    .filter(DynamicField::unique)
                    .map(DynamicField::name)
                    .forEach(formUniqueColumns::add);
            Set<String> tableUniqueColumns = new HashSet<>();
            table.uniqueConstraints().stream()
                    .filter(unique -> unique.columns().size() == 1)
                    .map(unique -> unique.columns().getFirst())
                    .forEach(tableUniqueColumns::add);
            table.indexes().stream()
                    .filter(IndexDefinition::unique)
                    .filter(index -> index.keys().size() == 1)
                    .map(index -> index.keys().getFirst().column())
                    .forEach(tableUniqueColumns::add);
            if (!formUniqueColumns.equals(tableUniqueColumns)) {
                throw new IllegalArgumentException(
                        "dynamic form and relational table must declare the same single-column unique keys");
            }
        }

        private static boolean samePhysicalColumns(List<String> first, List<String> second) {
            return first.size() == second.size() && new HashSet<>(first).equals(new HashSet<>(second));
        }
    }

    /** 字段与列只在构建器里成对保存，发布后不再保留这层中间对象。 */
    private record FieldColumnPair(DynamicField field, ColumnDefinition column) {
    }
}
