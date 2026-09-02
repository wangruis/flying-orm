package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    private final String table;

    private final List<DynamicField> fields = new ArrayList<>();

    private final List<IndexMetadata> indexes = new ArrayList<>();

    private final List<ForeignKeyMetadata> foreignKeys = new ArrayList<>();

    private SchemaMigrationOptions options = SchemaMigrationOptions.safe();

    CreateOrAlterTableBuilder(ReactiveSchemaClient schemaClient, ReactiveFormMetadataReader metadataReader, String table) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "reactive form metadata reader must not be null");
        this.table = requireText(table, "table");
    }

    /**
     * 开始描述一列。必须在列构建器上调用 commit，列才会进入目标表结构。
     *
     * @return 新的列构建器
     */
    public ColumnBuilder addColumn() {
        return new ColumnBuilder(this);
    }

    /**
     * 开始描述一个索引。索引名和列顺序会进入迁移差异比较。
     *
     * @param name 索引名
     * @return 新的索引构建器
     */
    public IndexBuilder addIndex(String name) {
        return new IndexBuilder(this, name);
    }

    /**
     * 开始描述一个外键。安全计划会把高风险关系变化留给上层明确审核。
     *
     * @param name 外键名
     * @return 新的外键构建器
     */
    public ForeignKeyBuilder addForeignKey(String name) {
        return new ForeignKeyBuilder(this, name);
    }

    /**
     * 设置本次迁移允许执行的变化范围。调用方必须显式放开危险操作，默认不会自动继承宽松配置。
     *
     * @param options 迁移安全选项
     * @return 当前表构建器
     */
    public CreateOrAlterTableBuilder options(SchemaMigrationOptions options) {
        this.options = Objects.requireNonNull(options, "schema migration options must not be null");
        return this;
    }

    /**
     * 执行安全迁移，只返回所有已执行 DDL 的累计影响行数。
     *
     * @return 惰性的迁移结果
     */
    public Mono<Long> commit() {
        return commitDetailed().map(SchemaMigrationResult::rowsUpdated);
    }

    /**
     * 执行安全迁移并保留完整计划、跳过项和影响行数。只有执行成功才会精确清理本表元数据缓存。
     *
     * @return 惰性的详细迁移结果
     */
    public Mono<SchemaMigrationResult> commitDetailed() {
        return schemaClient.createOrAlterDetailed(buildForm(), indexes, foreignKeys, metadataReader, options);
    }

    /**
     * 只读取元数据并生成计划，不执行 DDL，也不会让元数据缓存失效。
     *
     * @return 惰性的结构化迁移计划
     */
    public Mono<SchemaMigrationPlan> plan() {
        return schemaClient.planCreateOrAlter(buildForm(), indexes, foreignKeys, metadataReader, options);
    }

    /**
     * 生成带风险等级、回滚缺口和在线 DDL 判断的审核结果，不执行 SQL。危险 options 应先走这里，
     * 把结果展示或记录后再生成精确批准。
     */
    public Mono<ReviewedSchemaMigrationPlan> review(SchemaMigrationReviewPolicy policy) {
        return schemaClient.reviewCreateOrAlter(
                buildForm(), indexes, foreignKeys, metadataReader, options, policy);
    }

    /**
     * 执行已经审核过的确定计划。执行内容来自 reviewedPlan，不会重新读取当前可变 builder，
     * 因此审批指纹和真正下发的 SQL 始终对应同一份计划。
     */
    public Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                       SchemaMigrationExecutionOptions executionOptions) {
        return schemaClient.executeReviewed(reviewedPlan, metadataReader, executionOptions);
    }

    /** 使用 DatabaseOperator 或 SchemaClient 装配时统一设置的默认执行保护。 */
    public Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan) {
        return schemaClient.executeReviewed(reviewedPlan, metadataReader);
    }

    private DynamicForm buildForm() {
        DynamicForm.Builder builder = DynamicForm.builder(table, table);
        fields.forEach(builder::addField);
        return builder.build();
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

    static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
