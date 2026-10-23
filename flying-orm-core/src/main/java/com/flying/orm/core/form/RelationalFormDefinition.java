package com.flying.orm.core.form;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
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
     * 字段简写和命名约束在构建时合为一份事实，CRUD 与 Schema 从中发布一致视图。
     */
    public static final class Builder {

        private final String formId;

        private final RelationIdentity identity;

        private final List<DynamicField> fields = new ArrayList<>();
        private FeatureRegistry features = FeatureRegistry.empty();

        private PrimaryKeyDefinition primaryKey;

        private final List<UniqueConstraintDefinition> uniqueConstraints = new ArrayList<>();

        private final List<IndexDefinition> indexes = new ArrayList<>();

        private final List<ForeignKeyDefinition> foreignKeys = new ArrayList<>();

        private final List<CheckConstraintDefinition> checks = new ArrayList<>();

        private Builder(String formId, RelationIdentity identity) {
            this.formId = Objects.requireNonNull(formId, "form id must not be null");
            this.identity = Objects.requireNonNull(identity, "relation identity must not be null");
        }

        /** 逻辑表单的扩展配置，不写入数据库结构。 */
        public Builder features(FeatureRegistry features) {
            this.features = Objects.requireNonNull(features, "form features");
            return this;
        }

        /** 加入唯一字段声明；CRUD 和 Schema 使用其中同一份列定义。 */
        public Builder addField(DynamicField field) {
            fields.add(Objects.requireNonNull(field, "dynamic field must not be null"));
            return this;
        }

        /** 声明完整主键及列顺序；未声明时从字段的主键标记生成。 */
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
            DynamicForm.Builder formBuilder = DynamicForm.relationalBuilder(formId, identity).features(features);
            RelationalTableDefinition.Builder tableBuilder = RelationalTableDefinition.builder(identity);
            List<String> keyColumns = primaryKey == null
                    ? fields.stream().filter(DynamicField::primaryKey).map(DynamicField::name).toList()
                    : primaryKey.columns();
            Set<String> keys = new HashSet<>(keyColumns);
            Set<String> uniqueColumns = new HashSet<>();
            uniqueConstraints.stream().filter(unique -> unique.columns().size() == 1)
                    .forEach(unique -> uniqueColumns.add(unique.columns().getFirst()));
            indexes.stream().filter(IndexDefinition::unique).filter(index -> index.keys().size() == 1)
                    .forEach(index -> uniqueColumns.add(index.keys().getFirst().column()));
            for (DynamicField field : fields) {
                if (field.unique() && uniqueColumns.add(field.name())) {
                    tableBuilder.addIndex(IndexDefinition.builder(
                            DynamicFormStructure.uniqueIndexName(identity.table(), field.name()))
                            .unique().addKey(IndexKeyPart.asc(field.name())).build());
                }
                boolean primary = keys.contains(field.name());
                boolean unique = uniqueColumns.contains(field.name());
                DynamicField projected = primary == field.primaryKey() && unique == field.unique() ? field
                        : new DynamicField(field.column(), primary, unique, field.runtimeDefault(), field.features());
                formBuilder.addField(projected);
                tableBuilder.addColumn(projected.column());
            }
            if (!keyColumns.isEmpty()) {
                tableBuilder.primaryKey(primaryKey == null
                        ? PrimaryKeyDefinition.of("pk_" + identity.table(), keyColumns.toArray(String[]::new))
                        : primaryKey);
            }
            uniqueConstraints.forEach(tableBuilder::addUnique);
            indexes.forEach(tableBuilder::addIndex);
            foreignKeys.forEach(tableBuilder::addForeignKey);
            checks.forEach(tableBuilder::addCheck);

            RelationalTableDefinition builtTable = tableBuilder.build();
            return new RelationalFormDefinition(formBuilder.build(), builtTable);
        }
    }

}
