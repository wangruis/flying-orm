package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 同步 DDL 链式调用的一次性结构草稿。
 *
 * <p>它只保存调用方正在描述的目标表，不持有 JDBC Connection，也不缓存任何执行结果。因此每次
 * {@code createOrAlter(...)} 都会创建新实例，不能跨线程或跨请求复用。真正的读库、规划和 DDL
 * 执行仍交给 {@link JdbcSchemaClient}，这里的职责只是把四个同步 builder 收集到同一份稳定输入。</p>
 */
final class SyncDdlStructureState {

    private final JdbcSchemaClient schemaClient;
    private final JdbcFormMetadataReader metadataReader;
    private final String table;
    private final List<DynamicField> fields = new ArrayList<>();
    private final List<IndexMetadata> indexes = new ArrayList<>();
    private final List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();
    private SchemaMigrationOptions options = SchemaMigrationOptions.safe();

    SyncDdlStructureState(JdbcSchemaClient schemaClient, JdbcFormMetadataReader metadataReader, String table) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "jdbc schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "jdbc form metadata reader must not be null");
        this.table = CreateOrAlterTableBuilder.requireText(table, "table");
    }

    void addField(DynamicField field) {
        fields.add(Objects.requireNonNull(field, "dynamic field must not be null"));
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

    SchemaMigrationResult commitDetailed() {
        return schemaClient.createOrAlterDetailed(form(), indexes, foreignKeys, metadataReader, options);
    }

    SchemaMigrationPlan plan() {
        if (foreignKeys.isEmpty()) {
            return schemaClient.planCreateOrAlter(form(), indexes, metadataReader, options);
        }
        // JdbcSchemaClient 还没有带外键参数的 plan 重载。审核入口使用同一份 JDBC 规划器，
        // 只额外生成审核信息，不会执行 SQL；取回 migration 能避免同步路径悄悄丢掉外键。
        return review(SchemaMigrationReviewPolicy.allowBlocking()).migration();
    }

    ReviewedSchemaMigrationPlan review(SchemaMigrationReviewPolicy policy) {
        return schemaClient.reviewCreateOrAlter(form(), indexes, foreignKeys, metadataReader, options, policy);
    }

    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                          SchemaMigrationExecutionOptions executionOptions) {
        return schemaClient.executeReviewed(reviewedPlan, executionOptions);
    }

    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan) {
        return schemaClient.executeReviewed(reviewedPlan);
    }

    private DynamicForm form() {
        DynamicForm.Builder builder = DynamicForm.builder(table, table);
        fields.forEach(builder::addField);
        return builder.build();
    }
}
