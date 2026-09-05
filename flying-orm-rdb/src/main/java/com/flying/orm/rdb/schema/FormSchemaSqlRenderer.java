package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 动态表单的 DDL 门面。
 *
 * <p>公开方法和原来的使用方式保持不变，具体工作交给包内协作者：表与字段 SQL、迁移计划、外键风险
 * 报告和回滚 SQL 各自独立。这样这个类只负责入口编排，不再把所有数据库结构规则堆在一个文件里。</p>
 *
 * <p>渲染器只生成 SQL 请求和迁移计划，不访问数据库，也不启动事务。标识符、类型和自定义 DDL 片段
 * 仍由 {@link SchemaDialect} 做白名单校验。</p>
 *
 * <p>旧 String 表名保持原有语义。带 catalog、schema 或字面点号的分段关系，请使用
 * {@link RelationalSchemaSqlRenderer}，避免在兼容入口丢失物理关系身份。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class FormSchemaSqlRenderer {

    private final SchemaDialect dialect;
    private final SchemaTableSqlRenderer tables;
    private final SchemaMigrationPlanner migrations;
    private final SchemaRollbackSqlRenderer rollback;

    private FormSchemaSqlRenderer(SchemaDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "schema dialect must not be null");
        this.tables = new SchemaTableSqlRenderer(dialect);
        this.migrations = new SchemaMigrationPlanner(this);
        this.rollback = new SchemaRollbackSqlRenderer(dialect, tables);
    }

    /** 用指定的结构 SQL 写法创建渲染器。 */
    public static FormSchemaSqlRenderer create(SchemaDialect dialect) {
        return new FormSchemaSqlRenderer(dialect);
    }

    /** 用数据库方言创建结构 SQL 渲染器。 */
    public static FormSchemaSqlRenderer create(RdbDialect dialect) {
        return create(Objects.requireNonNull(dialect, "rdb dialect must not be null").schema());
    }

    /** 包内审核器读取在线 DDL 能力，业务层不需要直接依赖结构方言对象。 */
    SchemaOnlineDdlSupport onlineDdlSupport() {
        return dialect.onlineDdlSupport();
    }

    SchemaDialect dialect() {
        return dialect;
    }

    SchemaTableSqlRenderer tableRenderer() {
        return tables;
    }

    /** 包内审核器复用方言的在线索引改写规则。 */
    SqlRequest preferOnline(SqlRequest request) {
        return dialect.preferOnline(request);
    }

    /** 包内结构客户端生成锁等待保护，setup/work/cleanup 仍由客户端放在同一连接执行。 */
    SchemaDdlSessionGuard lockTimeoutGuard(Duration timeout) {
        return dialect.lockTimeoutGuard(timeout);
    }

    /** 生成建表 SQL，必要的序列和字段注释会按原来的顺序附在请求列表中。 */
    public List<SqlRequest> createTable(DynamicForm form) {
        DynamicForm safeForm = SchemaMigrationSupport.requireLegacyRelation(form);
        DynamicForm physical = ProtectedFormLayout.physical(safeForm);
        List<SqlRequest> requests = new java.util.ArrayList<>(tables.createTable(physical));
        requests.addAll(tables.createIndexes(physical.table(), physical.toTableMetadata().indexes()));
        ProtectedContainsLayout.resolve(safeForm).ifPresent(layout -> {
            requests.addAll(tables.createTable(layout.table()));
            requests.addAll(tables.createIndexes(layout.table().table(), layout.indexes()));
            layout.foreignKeys().forEach(foreignKey -> requests.add(
                    tables.createCascadeForeignKey(layout.table().table(), foreignKey)));
        });
        return List.copyOf(requests);
    }

    /** 给已有表创建目标索引。 */
    public List<SqlRequest> createIndexes(String table, List<IndexMetadata> indexes) {
        return tables.createIndexes(table, indexes);
    }

    /** 表不存在时先建表，再建索引；外键继续进入人工审核列表。 */
    public SchemaMigrationPlan createTablePlan(DynamicForm target, List<IndexMetadata> targetIndexes) {
        return createTablePlan(target, targetIndexes, List.of());
    }

    public SchemaMigrationPlan createTablePlan(DynamicForm target,
                                               List<IndexMetadata> targetIndexes,
                                               List<ForeignKeyMetadata> targetForeignKeys) {
        return migrations.createTablePlan(target, targetIndexes, targetForeignKeys);
    }

    /** 根据变更集生成直接迁移 SQL。高风险结构变更仍应使用安全迁移计划入口。 */
    public List<SqlRequest> migrate(DynamicFormChangeSet changeSet) {
        return migrations.migrate(changeSet);
    }

    /** 安全迁移的快捷入口，只返回可以执行的 SQL。 */
    public List<SqlRequest> migrateSafely(TableMetadata current,
                                          DynamicForm target,
                                          List<IndexMetadata> targetIndexes) {
        return migrateSafelyPlan(current, target, targetIndexes).requests();
    }

    public List<SqlRequest> migrateSafely(TableMetadata current,
                                          DynamicForm target,
                                          List<IndexMetadata> targetIndexes,
                                          SchemaMigrationOptions options) {
        return migrateSafelyPlan(current, target, targetIndexes, options).requests();
    }

    /** 返回完整安全迁移计划，包含可执行 SQL 和需要上层审核的 skipped 项。 */
    public SchemaMigrationPlan migrateSafelyPlan(TableMetadata current,
                                                 DynamicForm target,
                                                 List<IndexMetadata> targetIndexes) {
        return migrateSafelyPlan(current, target, targetIndexes, List.of(), SchemaMigrationOptions.safe());
    }

    public SchemaMigrationPlan migrateSafelyPlan(TableMetadata current,
                                                 DynamicForm target,
                                                 List<IndexMetadata> targetIndexes,
                                                 SchemaMigrationOptions options) {
        return migrateSafelyPlan(current, target, targetIndexes, List.of(), options);
    }

    public SchemaMigrationPlan migrateSafelyPlan(TableMetadata current,
                                                 DynamicForm target,
                                                 List<IndexMetadata> targetIndexes,
                                                 List<ForeignKeyMetadata> targetForeignKeys,
                                                 SchemaMigrationOptions options) {
        return migrations.migrateSafelyPlan(current,
                                             target,
                                             targetIndexes,
                                             targetForeignKeys,
                                             options);
    }

    /* 下面的方法只服务于 schema 包内审核器，避免把回滚拼装规则复制到审核器里。 */
    SqlRequest rollbackDropTable(String table) {
        return rollback.rollbackDropTable(table);
    }

    List<SqlRequest> rollbackDropSequences(DynamicForm form) {
        return tables.dropSequences(form.fields(), List.of());
    }

    List<SqlRequest> rollbackDropSequences(List<DynamicField> fields) {
        return tables.dropSequences(fields, List.of());
    }

    List<SqlRequest> rollbackDropSequences(List<DynamicField> fields, List<DynamicField> retainedFields) {
        return tables.dropSequences(fields, retainedFields);
    }

    SqlRequest rollbackAddColumn(String table, com.flying.orm.core.metadata.ColumnMetadata column) {
        return rollback.rollbackAddColumn(table, column);
    }

    SqlRequest rollbackDropColumn(String table, String column) {
        return rollback.rollbackDropColumn(table, column);
    }

    SqlRequest rollbackColumnType(String table,
                                  String currentColumn,
                                  com.flying.orm.core.metadata.ColumnMetadata column) {
        return rollback.rollbackColumnType(table, currentColumn, column);
    }

    SqlRequest rollbackRenameColumn(String table, String currentName, String previousName) {
        return rollback.rollbackRenameColumn(table, currentName, previousName);
    }

    SqlRequest rollbackDropIndex(String table, IndexMetadata index) {
        return rollback.rollbackDropIndex(table, index);
    }

    SqlRequest rollbackCreateIndex(String table, IndexMetadata index) {
        return rollback.rollbackCreateIndex(table, index);
    }
}
