package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;

import java.util.List;
import java.util.Objects;

/**
 * 动态表单的 DDL 门面。
 *
 * <p>表与字段 SQL、迁移计划和外键差异由包内协作者负责；该门面只组织公开入口。</p>
 *
 * <p>渲染器只生成 SQL 请求和迁移计划，不访问数据库，也不启动事务。标识符、类型和自定义 DDL 片段
 * 仍由 {@link SchemaDialect} 做白名单校验；显式 {@link FormSchemaDdlRenderer} 回调输出属于可信后端 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class FormSchemaSqlRenderer {

    private final SchemaDialect dialect;
    private final RelationalSchemaSqlRenderer tables;
    private final SchemaMigrationPlanner migrations;

    private FormSchemaSqlRenderer(SchemaDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.tables = RelationalSchemaSqlRenderer.create(dialect);
        this.migrations = new SchemaMigrationPlanner(this);
    }

    /** 用指定的结构 SQL 写法创建渲染器。 */
    public static FormSchemaSqlRenderer create(SchemaDialect dialect) {
        return new FormSchemaSqlRenderer(dialect);
    }

    /** 用数据库方言创建结构 SQL 渲染器。 */
    public static FormSchemaSqlRenderer create(RdbDialect dialect) {
        return create(Objects.requireNonNull(dialect, "rdb dialect must not be null").schema());
    }

    SchemaDialect dialect() {
        return dialect;
    }

    RelationalSchemaSqlRenderer tableRenderer() {
        return tables;
    }

    /** 两种客户端直接复用渲染器拥有的无状态迁移规划器。 */
    SchemaMigrationPlanner migrationPlanner() {
        return migrations;
    }

    /** 复用方言的并发索引语法。 */
    SqlRequest preferConcurrentIndex(SqlRequest request) {
        return dialect.preferConcurrentIndex(request);
    }

    /** 生成建表 SQL，必要的序列和字段注释会按原来的顺序附在请求列表中。 */
    public List<SqlRequest> createTable(DynamicForm form) {
        return createTablePlan(form, List.of()).requests();
    }

    /** 给已有表创建目标索引。 */
    public List<SqlRequest> createIndexes(String table, List<IndexDefinition> indexes) {
        RelationIdentity relation = SchemaMigrationSupport.relation(table);
        return Objects.requireNonNull(indexes, "indexes must not be null").stream()
                .flatMap(index -> tables.render(SchemaOperation.of(SchemaOperation.Kind.ADD_INDEX,
                        relation, index.name(), null, index, SchemaOperation.Compatibility.REQUIRES_REVIEW)).stream())
                .toList();
    }

    /** 表不存在时统一生成表、索引及显式外键的有序 DDL。 */
    public SchemaMigrationPlan createTablePlan(DynamicForm target, List<IndexDefinition> targetIndexes) {
        return createTablePlan(target, targetIndexes, List.of());
    }

    public SchemaMigrationPlan createTablePlan(DynamicForm target,
                                               List<IndexDefinition> targetIndexes,
                                               List<ForeignKeyDefinition> targetForeignKeys) {
        return migrations.createTablePlan(target, targetIndexes, targetForeignKeys);
    }

    /** 根据显式变更集生成迁移 SQL。 */
    public List<SqlRequest> migrate(DynamicFormChangeSet changeSet) {
        return migrations.migrate(changeSet);
    }

    /** 根据物理快照保留变更集未描述的列属性，例如排序规则和默认值。 */
    public List<SqlRequest> migrate(DynamicFormChangeSet changeSet, SchemaSnapshot snapshot) {
        return migrations.migrate(changeSet, Objects.requireNonNull(snapshot, "schema snapshot must not be null"));
    }

    /** 安全迁移的快捷入口，只返回可以执行的 SQL。 */
    public List<SqlRequest> migrateSafely(RelationalTableDefinition current,
                                          DynamicForm target,
                                          List<IndexDefinition> targetIndexes) {
        return migrateSafelyPlan(current, target, targetIndexes).requests();
    }

    public List<SqlRequest> migrateSafely(RelationalTableDefinition current,
                                          DynamicForm target,
                                          List<IndexDefinition> targetIndexes,
                                          SchemaMigrationOptions options) {
        return migrateSafelyPlan(current, target, targetIndexes, options).requests();
    }

    /** 返回完整安全迁移计划，包含可执行 SQL 和需要上层审核的 skipped 项。 */
    public SchemaMigrationPlan migrateSafelyPlan(RelationalTableDefinition current,
                                                 DynamicForm target,
                                                 List<IndexDefinition> targetIndexes) {
        return migrateSafelyPlan(current, target, targetIndexes, List.of(), SchemaMigrationOptions.safe());
    }

    public SchemaMigrationPlan migrateSafelyPlan(RelationalTableDefinition current,
                                                 DynamicForm target,
                                                 List<IndexDefinition> targetIndexes,
                                                 SchemaMigrationOptions options) {
        return migrateSafelyPlan(current, target, targetIndexes, List.of(), options);
    }

    public SchemaMigrationPlan migrateSafelyPlan(RelationalTableDefinition current,
                                                 DynamicForm target,
                                                 List<IndexDefinition> targetIndexes,
                                                 List<ForeignKeyDefinition> targetForeignKeys,
                                                 SchemaMigrationOptions options) {
        return renderPlan(current, null, migrations.migrateSafelyPlan(current,
                                             target,
                                             targetIndexes,
                                             targetForeignKeys,
                                             options), options);
    }

    SchemaMigrationPlan renderPlan(RelationalTableDefinition current, SchemaSnapshot snapshot,
                                   SchemaMigrationPlan builtin, SchemaMigrationOptions options) {
        if (options.concurrentIndexesPreferred()) {
            builtin = builtin.steps().isEmpty() ? new SchemaMigrationPlan(
                    builtin.target(), builtin.tableExists(),
                    builtin.requests().stream().map(this::preferConcurrentIndex).toList(),
                    builtin.skippedChanges(), builtin.additionalCreatedTables())
                    : SchemaMigrationPlan.planned(builtin.target(), builtin.tableExists(),
                            builtin.steps().stream().map(step -> new SchemaPlanStep(step.operation(),
                                    step.requests().stream().map(this::preferConcurrentIndex).toList())).toList(),
                            builtin.skippedChanges(), builtin.additionalCreatedTables());
        }
        return renderPlan(current, snapshot, builtin);
    }

    SchemaMigrationPlan renderPlan(RelationalTableDefinition current, SchemaSnapshot snapshot, SchemaMigrationPlan builtin) {
        FormSchemaDdlRenderer extension = dialect.formRenderer();
        if (extension == null || !builtin.hasExecutableSql()) {
            return builtin;
        }
        List<SqlRequest> requests = requireRendered(extension.render(current, snapshot, builtin));
        return new SchemaMigrationPlan(builtin.target(),
                builtin.tableExists(), requests, builtin.skippedChanges(), builtin.additionalCreatedTables());
    }

    private static List<SqlRequest> requireRendered(List<SqlRequest> requests) {
        List<SqlRequest> result = List.copyOf(requests);
        if (result.isEmpty()) {
            throw new IllegalArgumentException("form schema renderer must not erase planned DDL");
        }
        return result;
    }

}
