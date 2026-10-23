package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationResult;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 同步 create-or-alter 链式 builder。
 *
 * <p>列、索引、外键和迁移选项先写入本次调用独享的结构状态，最终直接交给 JDBC Schema 客户端。
 * 它不持有 R2DBC 组件，也不会等待响应式 DDL。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncCreateOrAlterTableBuilder {

    private final JdbcSchemaClient schemaClient;
    private final JdbcFormMetadataReader metadataReader;
    private final DdlStructureDraft draft;

    SyncCreateOrAlterTableBuilder(JdbcSchemaClient schemaClient,
                                  JdbcFormMetadataReader metadataReader,
                                  String table) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "jdbc schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "jdbc form metadata reader must not be null");
        this.draft = new DdlStructureDraft(table);
    }

    /** 开始描述一列；必须调用列 builder 的 commit 才会加入目标表。 */
    public ColumnBuilder<SyncCreateOrAlterTableBuilder> addColumn() {
        return new ColumnBuilder<>(this, draft);
    }

    /** 开始描述具名列，等价于 addColumn().name(name)。 */
    public ColumnBuilder<SyncCreateOrAlterTableBuilder> addColumn(String name) {
        return addColumn().name(name);
    }

    /** 配置并添加一列，回调中无需 commit；返回时冻结列定义，不触发数据库操作。 */
    public SyncCreateOrAlterTableBuilder addColumn(
            String name, Consumer<? super ColumnBuilder<SyncCreateOrAlterTableBuilder>> definition) {
        ColumnBuilder<SyncCreateOrAlterTableBuilder> column = addColumn(name);
        Objects.requireNonNull(definition, "column configuration must not be null").accept(column);
        return column.commit();
    }

    /** 直接复用完整动态字段，包括类型、精度、可空性、唯一性和生成策略。 */
    public SyncCreateOrAlterTableBuilder addColumn(DynamicField field) {
        draft.addColumn(field);
        return this;
    }

    /** 开始描述一个索引；索引列顺序按调用顺序保留。 */
    public IndexBuilder<SyncCreateOrAlterTableBuilder> addIndex(String name) {
        return new IndexBuilder<>(this, draft, name);
    }

    /** 开始描述一个外键；结构变化按显式迁移选项规划。 */
    public ForeignKeyBuilder<SyncCreateOrAlterTableBuilder> addForeignKey(String name) {
        return new ForeignKeyBuilder<>(this, draft, name);
    }

    /** 设置本次迁移允许执行的变化范围，默认使用安全策略。 */
    public SyncCreateOrAlterTableBuilder options(SchemaMigrationOptions options) {
        draft.options(options);
        return this;
    }

    /** 执行迁移并返回累计影响行数。 */
    public long commit() {
        return commitDetailed().rowsUpdated();
    }

    /** 执行迁移并返回完整计划、跳过项和影响行数。 */
    public SchemaMigrationResult commitDetailed() {
        return schemaClient.createOrAlterDetailed(
                draft.form(), draft.indexes(), draft.foreignKeys(), metadataReader, draft.options());
    }

    /** 只生成结构化计划，不执行 DDL，也不会失效元数据缓存。 */
    public SchemaMigrationPlan plan() {
        return schemaClient.planCreateOrAlter(
                draft.form(), draft.indexes(), draft.foreignKeys(), metadataReader, draft.options());
    }

    /** 执行冻结的结构计划，并以显式 SQL 选项覆盖客户端默认值。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan,
                                                 SqlExecutionOptions executionOptions) {
        return schemaClient.execute(plan, metadataReader, executionOptions);
    }

    /** 使用 Schema 客户端配置的默认 SQL 选项执行计划。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan) {
        return schemaClient.execute(plan, metadataReader);
    }

    DdlStructureDraft draft() {
        return draft;
    }
}
