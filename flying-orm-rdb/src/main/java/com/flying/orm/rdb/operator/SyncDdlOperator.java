package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 同步 DDL 入口。
 *
 * <p>该入口直接使用 JDBC Schema 客户端和元数据读取器。元数据读取与 DDL 执行共用上层连接接入和
 * 显式方言，调用方不需要额外拼装执行器。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class SyncDdlOperator {

    private final JdbcSchemaClient jdbcSchemaClient;
    private final JdbcFormMetadataReader jdbcMetadataReader;

    private SyncDdlOperator(JdbcSchemaClient jdbcSchemaClient, JdbcFormMetadataReader jdbcMetadataReader) {
        this.jdbcSchemaClient = Objects.requireNonNull(jdbcSchemaClient, "jdbc schema client must not be null");
        this.jdbcMetadataReader = Objects.requireNonNull(
                jdbcMetadataReader, "jdbc form metadata reader must not be null");
    }

    /**
     * 创建不会创建响应式运行时的同步 DDL 入口。
     *
     * <p>两个依赖应使用同一上层 JDBC 连接接入和显式方言。连接来源及不同操作间的一致性由上层决定，
     * ORM 不探测或参与事务。</p>
     *
     * @param schemaClient 原生 JDBC Schema 客户端
     * @param metadataReader 原生 JDBC 表结构读取器
     * @return 可复用的同步 DDL 入口
     */
    public static SyncDdlOperator create(JdbcSchemaClient schemaClient, JdbcFormMetadataReader metadataReader) {
        return new SyncDdlOperator(schemaClient, metadataReader);
    }

    /**
     * 开始描述一张表的目标结构。真正读取元数据和执行 DDL 都在后续的 commit、plan 或 review 中发生。
     *
     * @param table 物理表名
     * @return 单次使用的同步结构构建器
     */
    public SyncCreateOrAlterTableBuilder createOrAlter(String table) {
        return new SyncCreateOrAlterTableBuilder(jdbcSchemaClient, jdbcMetadataReader, table);
    }

    /** 从同一份动态表单建表或增量调整；其他变更通过构建器选项显式选择，不提供审核流程。 */
    public long createOrAlter(DynamicForm form) {
        return jdbcSchemaClient.createOrAlter(form, List.of(), jdbcMetadataReader);
    }

    /**
     * 按关系声明增量调整，未声明的现有列和约束保留；结果仅记录实际执行的 SQL。
     */
    public SchemaMigrationResult createOrAlter(DatabaseDescriptor database, RelationalTableDefinition table) {
        return execute(plan(database, table));
    }

    /** 在一次回调中描述增量结构，冻结后复用同一规划和执行路径。 */
    public SchemaMigrationResult createOrAlter(DatabaseDescriptor database, String table,
                                                Consumer<RelationalTableDefinition.Builder> definition) {
        return createOrAlter(database, RelationIdentity.table(table), definition);
    }

    /** 按显式 catalog/schema/table 身份配置完整表结构，不拆分表名中的点号。 */
    public SchemaMigrationResult createOrAlter(DatabaseDescriptor database, RelationIdentity table,
                                                Consumer<RelationalTableDefinition.Builder> definition) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(table);
        Objects.requireNonNull(definition, "table configuration must not be null").accept(builder);
        return createOrAlter(database, builder.build());
    }

    /** 规划增量声明，不执行 SQL；未声明的现有对象保留。 */
    public SchemaMigrationPlan plan(DatabaseDescriptor database, RelationalTableDefinition table) {
        return jdbcSchemaClient.planCreateOrAlter(database, table, jdbcMetadataReader);
    }

    /** 显式规划完整目标替换；目标未包含的现有对象会进入删除计划。 */
    public SchemaMigrationPlan planReplacement(DatabaseDescriptor database, RelationalTableDefinition table) {
        return planReplacement(database, table, SchemaCompatibilityMode.EXACT);
    }

    /** 对完整目标求差异；兼容模式只分类差异，不改变替换意图或执行 SQL。 */
    public SchemaMigrationPlan planReplacement(DatabaseDescriptor database, RelationalTableDefinition table,
                                      SchemaCompatibilityMode mode) {
        return jdbcSchemaClient.reviewRelational(database, table, jdbcMetadataReader, mode);
    }

    /** 执行已冻结的关系计划，使用客户端默认执行选项。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan) {
        return jdbcSchemaClient.execute(plan, jdbcMetadataReader);
    }

    /** 使用本次明确指定的执行选项执行已冻结计划。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan, SqlExecutionOptions options) {
        return jdbcSchemaClient.execute(plan, jdbcMetadataReader, options);
    }
}
