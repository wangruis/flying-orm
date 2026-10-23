package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * createOrAlter 的链式入口。它先读当前表结构，再按安全策略生成并执行差异计划。
 *
 * <p>构建器只描述一张表的一次目标状态，内部列表会随 add/commit 变化，不能跨线程共享。默认使用
 * {@link SchemaMigrationOptions#safe()}，危险删列、改类型或索引重建不会因为调用 commit 就被自动放开。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class CreateOrAlterTableBuilder {

    private final ReactiveSchemaClient schemaClient;

    private final ReactiveFormMetadataReader metadataReader;

    private final DdlStructureDraft draft;

    CreateOrAlterTableBuilder(ReactiveSchemaClient schemaClient, ReactiveFormMetadataReader metadataReader, String table) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "reactive form metadata reader must not be null");
        this.draft = new DdlStructureDraft(table);
    }

    /**
     * 开始描述一列。必须在列构建器上调用 commit，列才会进入目标表结构。
     *
     * @return 新的列构建器
     */
    public ColumnBuilder<CreateOrAlterTableBuilder> addColumn() {
        return new ColumnBuilder<>(this, draft);
    }

    /** 开始描述具名列，等价于 addColumn().name(name)。 */
    public ColumnBuilder<CreateOrAlterTableBuilder> addColumn(String name) {
        return addColumn().name(name);
    }

    /** 配置并添加一列，回调中无需 commit；返回时冻结列定义，不触发数据库操作。 */
    public CreateOrAlterTableBuilder addColumn(
            String name, Consumer<? super ColumnBuilder<CreateOrAlterTableBuilder>> definition) {
        ColumnBuilder<CreateOrAlterTableBuilder> column = addColumn(name);
        Objects.requireNonNull(definition, "column configuration must not be null").accept(column);
        return column.commit();
    }

    /** 直接复用完整动态字段，包括类型、精度、可空性、唯一性和生成策略。 */
    public CreateOrAlterTableBuilder addColumn(DynamicField field) {
        draft.addColumn(field);
        return this;
    }

    /**
     * 开始描述一个索引。索引名和列顺序会进入迁移差异比较。
     *
     * @param name 索引名
     * @return 新的索引构建器
     */
    public IndexBuilder<CreateOrAlterTableBuilder> addIndex(String name) {
        return new IndexBuilder<>(this, draft, name);
    }

    /**
     * 开始描述一个外键。不能生成正确 SQL 的关系变化会明确列入计划。
     *
     * @param name 外键名
     * @return 新的外键构建器
     */
    public ForeignKeyBuilder<CreateOrAlterTableBuilder> addForeignKey(String name) {
        return new ForeignKeyBuilder<>(this, draft, name);
    }

    /**
     * 设置本次迁移允许执行的变化范围。调用方必须显式放开危险操作，默认不会自动继承宽松配置。
     *
     * @param options 迁移安全选项
     * @return 当前表构建器
     */
    public CreateOrAlterTableBuilder options(SchemaMigrationOptions options) {
        draft.options(options);
        return this;
    }

    /**
     * 按显式迁移选项执行，只返回所有已执行 DDL 的累计影响行数。
     *
     * @return 惰性的迁移结果
     */
    public Mono<Long> commit() {
        return commitDetailed().map(SchemaMigrationResult::rowsUpdated);
    }

    /**
     * 按显式迁移选项执行并保留完整计划、跳过项和影响行数，执行后失效相关表元数据缓存。
     *
     * @return 惰性的详细迁移结果
     */
    public Mono<SchemaMigrationResult> commitDetailed() {
        return schemaClient.createOrAlterDetailed(
                draft.form(), draft.indexes(), draft.foreignKeys(), metadataReader, draft.options());
    }

    /**
     * 只读取元数据并生成计划，不执行 DDL，也不会让元数据缓存失效。
     *
     * @return 惰性的结构化迁移计划
     */
    public Mono<SchemaMigrationPlan> plan() {
        return schemaClient.planCreateOrAlter(
                draft.form(), draft.indexes(), draft.foreignKeys(), metadataReader, draft.options());
    }

    /** 执行冻结的结构计划，不重新读取当前可变 builder。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan,
                                                       SqlExecutionOptions executionOptions) {
        return schemaClient.execute(plan, metadataReader, executionOptions);
    }

    /** 使用 DatabaseOperator 或 SchemaClient 装配时统一设置的默认执行保护。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan) {
        return schemaClient.execute(plan, metadataReader);
    }

    DdlStructureDraft draft() {
        return draft;
    }
}
