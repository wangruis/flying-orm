package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 动态表结构的响应式入口。
 *
 * <p>本对象本身不保存建表状态，每次调用 {@link #createOrAlter(String)} 都会创建一个新的迁移构建器；
 * 它只共享已经组装好的 schema client 和元数据 reader，因此可以作为轻量入口使用。</p>
 *
 * @author wangr
 * @date 2026-07-27
 * @version v1.0
 */
public final class DdlOperator {

    private final ReactiveSchemaClient schemaClient;

    private final ReactiveFormMetadataReader metadataReader;

    DdlOperator(ReactiveSchemaClient schemaClient, ReactiveFormMetadataReader metadataReader) {
        this.schemaClient = Objects.requireNonNull(schemaClient, "schema client must not be null");
        this.metadataReader = Objects.requireNonNull(metadataReader, "reactive form metadata reader must not be null");
    }

    /**
     * 开始描述一张表的目标结构。真正读取元数据、生成迁移计划和执行 DDL 都在后续 commit/plan 时发生。
     *
     * @param table 物理表名，只接受安全标识符
     * @return 新的表结构构建器
     */
    public CreateOrAlterTableBuilder createOrAlter(String table) {
        return new CreateOrAlterTableBuilder(schemaClient, metadataReader, table);
    }

    /** 从同一份动态表单建表或增量调整；其他变更通过构建器选项显式选择，不提供审核流程。 */
    public Mono<Long> createOrAlter(DynamicForm form) {
        return schemaClient.createOrAlter(form, List.of(), metadataReader);
    }

    /**
     * 按关系声明增量调整，未声明的现有列和约束保留；订阅后读取、规划并执行。
     */
    public Mono<SchemaMigrationResult> createOrAlter(DatabaseDescriptor database, RelationalTableDefinition table) {
        return plan(database, table).flatMap(this::execute);
    }

    /**
     * 在一次回调中描述完整表结构，支持列默认值、CHECK 和命名约束。
     * 配置在调用时执行一次并冻结；订阅时才读取、规划和执行，重复订阅复用同一份声明。
     */
    public Mono<SchemaMigrationResult> createOrAlter(DatabaseDescriptor database, String table,
                                                      Consumer<RelationalTableDefinition.Builder> definition) {
        return createOrAlter(database, RelationIdentity.table(table), definition);
    }

    /** 按显式 catalog/schema/table 身份配置完整表结构，不拆分表名中的点号。 */
    public Mono<SchemaMigrationResult> createOrAlter(DatabaseDescriptor database, RelationIdentity table,
                                                      Consumer<RelationalTableDefinition.Builder> definition) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(table);
        Objects.requireNonNull(definition, "table configuration must not be null").accept(builder);
        return createOrAlter(database, builder.build());
    }

    /** 规划增量声明，不执行 SQL；未声明的现有对象保留。 */
    public Mono<SchemaMigrationPlan> plan(DatabaseDescriptor database, RelationalTableDefinition table) {
        return schemaClient.planCreateOrAlter(database, table, metadataReader);
    }

    /** 显式规划完整目标替换；目标未包含的现有对象会进入删除计划。 */
    public Mono<SchemaMigrationPlan> planReplacement(DatabaseDescriptor database, RelationalTableDefinition table) {
        return planReplacement(database, table, SchemaCompatibilityMode.EXACT);
    }

    /** 对完整目标求差异；兼容模式只分类差异，不改变替换意图或执行 SQL。 */
    public Mono<SchemaMigrationPlan> planReplacement(DatabaseDescriptor database, RelationalTableDefinition table,
                                           SchemaCompatibilityMode mode) {
        return schemaClient.reviewRelational(database, table, metadataReader, mode);
    }

    /** 执行已冻结的关系计划，使用客户端默认执行选项。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan) {
        return schemaClient.execute(plan, metadataReader);
    }

    /** 使用本次明确指定的执行选项执行已冻结计划。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan, SqlExecutionOptions options) {
        return schemaClient.execute(plan, metadataReader, options);
    }
}
