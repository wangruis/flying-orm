package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.ReviewedSchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;

import java.util.Objects;

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

    private final SyncDdlStructureState jdbcState;

    SyncCreateOrAlterTableBuilder(JdbcSchemaClient schemaClient,
                                  JdbcFormMetadataReader metadataReader,
                                  String table) {
        this.jdbcState = new SyncDdlStructureState(schemaClient, metadataReader, table);
    }

    /** 开始描述一列；必须调用列 builder 的 commit 才会加入目标表。 */
    public SyncColumnBuilder addColumn() {
        return new SyncColumnBuilder(jdbcState, this);
    }

    /** 开始描述一个索引；索引列顺序按调用顺序保留。 */
    public SyncIndexBuilder addIndex(String name) {
        return new SyncIndexBuilder(jdbcState, name, this);
    }

    /** 开始描述一个外键；危险关系变化仍由迁移计划和审核策略决定是否执行。 */
    public SyncForeignKeyBuilder addForeignKey(String name) {
        return new SyncForeignKeyBuilder(jdbcState, name, this);
    }

    /** 设置本次迁移允许执行的变化范围，默认使用安全策略。 */
    public SyncCreateOrAlterTableBuilder options(SchemaMigrationOptions options) {
        jdbcState.options(options);
        return this;
    }

    /** 执行迁移并返回累计影响行数。 */
    public long commit() {
        return commitDetailed().rowsUpdated();
    }

    /** 执行迁移并返回完整计划、跳过项和影响行数。 */
    public SchemaMigrationResult commitDetailed() {
        return jdbcState.commitDetailed();
    }

    /** 只生成结构化计划，不执行 DDL，也不会失效元数据缓存。 */
    public SchemaMigrationPlan plan() {
        return jdbcState.plan();
    }

    /** 生成可展示、可审批的审核计划，不会执行 DDL。 */
    public ReviewedSchemaMigrationPlan review(SchemaMigrationReviewPolicy policy) {
        return jdbcState.review(policy);
    }

    /** 执行已审核的确定计划，并以本次显式保护参数覆盖客户端默认值。 */
    public SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                 SchemaMigrationExecutionOptions executionOptions) {
        return jdbcState.executeReviewed(reviewedPlan, executionOptions);
    }

    /** 使用 Schema 客户端配置的默认执行保护执行已审核计划。 */
    public SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan) {
        return jdbcState.executeReviewed(reviewedPlan);
    }
}
