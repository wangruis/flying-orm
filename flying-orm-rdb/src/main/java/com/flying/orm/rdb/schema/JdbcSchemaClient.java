package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionOptions;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.flying.orm.rdb.schema.SchemaMigrationSupport.metadataTables;
import static com.flying.orm.rdb.schema.SchemaMigrationSupport.metadataRelations;

/**
 * 动态表结构的原生 JDBC 客户端。
 *
 * <p>它和 {@link ReactiveSchemaClient} 共用 SQL 渲染、结构比较、显式变更选项和结果模型，
 * 但执行路径完全同步：不会创建 Reactor，也不会调用 {@code block()}。调用方只需要准备好
 * {@link SyncSqlExecutor} 和 {@link JdbcFormMetadataReader}，其余行为由客户端统一编排。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcSchemaClient {

    /** 执行和会话锁等待时限由上层管理，Schema 默认不设置治理预算。 */
    private static final SqlExecutionOptions DEFAULT_EXECUTION_OPTIONS =
            SqlExecutionOptions.unlimited();

    private final SyncSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final RdbDialect relationalDialect;
    private final SchemaMigrationObserver observer;
    private final SqlExecutionOptions defaultExecutionOptions;
    private final SchemaCacheInvalidationCoordinator metadataInvalidator;
    private final SchemaMigrationPlanner planner;
    private final JdbcSchemaMigrationExecutor migrationExecutor;

    private JdbcSchemaClient(SyncSqlExecutor executor,
                             FormSchemaSqlRenderer renderer,
                             SchemaMigrationObserver observer,
                             SqlExecutionOptions executionOptions,
                             Consumer<RelationIdentity> metadataInvalidator,
                             RdbDialect relationalDialect) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.relationalDialect = relationalDialect;
        this.observer = SchemaMigrationObservers.safe(observer);
        this.defaultExecutionOptions = Objects.requireNonNull(
                executionOptions, "default schema migration execution options must not be null");
        this.metadataInvalidator = SchemaCacheInvalidationCoordinator.from(metadataInvalidator);
        this.planner = this.renderer.migrationPlanner();
        this.migrationExecutor = new JdbcSchemaMigrationExecutor(
                this.executor,
                this.observer);
    }

    /** 根据显式 RDB 方言创建客户端；自定义 DDL 使用 {@link RdbDialect#withSchema(SchemaDialect)}。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor, RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new JdbcSchemaClient(executor,
                                    FormSchemaSqlRenderer.create(safeDialect),
                                    SchemaMigrationObserver.noop(),
                                    DEFAULT_EXECUTION_OPTIONS,
                                    SchemaCacheInvalidationCoordinator.of(),
                                    safeDialect);
    }

    /** 返回使用新迁移 observer 的不可变客户端。 */
    public JdbcSchemaClient withMigrationObserver(SchemaMigrationObserver migrationObserver) {
        return copy(migrationObserver, defaultExecutionOptions, metadataInvalidator);
    }

    /** 返回使用新默认执行保护的不可变客户端。 */
    public JdbcSchemaClient withDefaultMigrationExecutionOptions(SqlExecutionOptions options) {
        return copy(observer, options, metadataInvalidator);
    }

    /**
     * 注入额外元数据缓存失效回调。自动迁移会同时清理参与规划的 JDBC reader，
     * 这里的表名回调用于通知其他共享缓存层。
     */
    public JdbcSchemaClient withMetadataInvalidator(Consumer<RelationIdentity> invalidator) {
        return copy(observer, defaultExecutionOptions, invalidator);
    }

    /** 创建动态表对应的物理表。 */
    public long createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.createTable(safeForm),
                                                          metadataTables(safeForm),
                                                          metadataInvalidator::invalidate,
                                                          defaultExecutionOptions);
    }

    /** 创建或安全调整动态表，返回影响行数汇总。 */
    public long createOrAlter(DynamicForm form,
                              List<IndexDefinition> indexes,
                              JdbcFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader).rowsUpdated();
    }

    public long createOrAlter(DynamicForm form,
                              List<IndexDefinition> indexes,
                              JdbcFormMetadataReader metadataReader,
                              SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, List.of(), metadataReader, options).rowsUpdated();
    }

    /** 按调用方显式选项规划、执行并返回完整迁移结果；默认选项不允许破坏性变更。 */
    public SchemaMigrationResult createOrAlterDetailed(DynamicForm form,
                                                       List<IndexDefinition> indexes,
                                                       JdbcFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form,
                                     indexes,
                                     List.of(),
                                     metadataReader,
                                     SchemaMigrationOptions.safe());
    }

    public SchemaMigrationResult createOrAlterDetailed(DynamicForm form,
                                                       List<IndexDefinition> indexes,
                                                       List<ForeignKeyDefinition> foreignKeys,
                                                       JdbcFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        SchemaMigrationPlan plan = planner.planJdbc(form, indexes, foreignKeys, metadataReader, safeOptions);
        return execute(plan, metadataReader, defaultExecutionOptions);
    }

    /** 执行已生成的结构变更计划；失败时用 SqlExecutionSequenceException 保留位置、已完成 SQL 和原始原因。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan,
                                                 SqlExecutionOptions options) {
        SchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "schema migration plan must not be null");
        safePlan.requireExecutionDialect(relationalDialect);
        return migrationExecutor.execute(safePlan, metadataRelations(safePlan), metadataInvalidator::invalidate, options);
    }

    /** 使用客户端默认的 SQL 选项执行计划。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan) {
        return execute(plan, defaultExecutionOptions);
    }

    /** 使用默认 SQL 选项执行，并失效参与规划的 reader。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan,
                                         JdbcFormMetadataReader metadataReader) {
        return execute(plan, metadataReader, defaultExecutionOptions);
    }

    /** 执行冻结计划，参与规划的 reader 与客户端既有缓存一起失效。 */
    public SchemaMigrationResult execute(SchemaMigrationPlan plan,
                                         JdbcFormMetadataReader metadataReader,
                                         SqlExecutionOptions options) {
        SchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "schema migration plan must not be null");
        safePlan.requireExecutionDialect(relationalDialect);
        return migrationExecutor.execute(safePlan, metadataRelations(safePlan),
                invalidatorFor(metadataReader)::invalidate, options);
    }

    /** 直读一次当前结构，并从完整关系模型生成冻结 SQL 的计划。 */
    public SchemaMigrationPlan reviewRelational(DatabaseDescriptor database,
                                               RelationalTableDefinition desired,
                                               JdbcFormMetadataReader metadataReader,
                                               SchemaCompatibilityMode mode) {
        return planRelational(database, desired, metadataReader, mode, false);
    }

    /** 增量规划完整关系声明，未声明的现有对象保持不变。 */
    public SchemaMigrationPlan planCreateOrAlter(DatabaseDescriptor database,
                                                RelationalTableDefinition declared,
                                                JdbcFormMetadataReader metadataReader) {
        return planRelational(database, declared, metadataReader, SchemaCompatibilityMode.SAFE_INCREMENTAL, true);
    }

    private SchemaMigrationPlan planRelational(DatabaseDescriptor database,
                                              RelationalTableDefinition desired,
                                              JdbcFormMetadataReader metadataReader,
                                              SchemaCompatibilityMode mode,
                                              boolean incremental) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationalTableDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaSnapshot actual = readSnapshot(safeReader, safeDesired.identity());
        return incremental
                ? reviewer.planIncremental(database, safeDesired, actual, safeReader.snapshotCoverage())
                : reviewer.review(database, safeDesired, actual, safeReader.snapshotCoverage(), mode);
    }

    /**
     * 直读一次当前结构，并审核调用方明确提出的关系删除目标。删除始终使用精确兼容模式，
     * 不会从缺少实体或表定义推断删除。
     */
    public SchemaMigrationPlan reviewRelationalAbsent(DatabaseDescriptor database,
                                                     RelationIdentity target,
                                                     JdbcFormMetadataReader metadataReader) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationIdentity safeTarget = Objects.requireNonNull(
                target, "absent relational target identity must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaSnapshot actual = readSnapshot(safeReader, safeTarget);
        return reviewer.reviewAbsent(
                database, safeTarget, actual, safeReader.snapshotCoverage());
    }

    /** 只生成安全迁移计划，不执行 SQL。 */
    public SchemaMigrationPlan planCreateOrAlter(DynamicForm form,
                                                 List<IndexDefinition> indexes,
                                                 JdbcFormMetadataReader metadataReader) {
        return planCreateOrAlter(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public SchemaMigrationPlan planCreateOrAlter(DynamicForm form,
                                                 List<IndexDefinition> indexes,
                                                 JdbcFormMetadataReader metadataReader,
                                                 SchemaMigrationOptions options) {
        return planCreateOrAlter(form, indexes, List.of(), metadataReader, options);
    }

    /** 只规划包含外键的目标结构，不生成审核信息或执行 SQL。 */
    public SchemaMigrationPlan planCreateOrAlter(DynamicForm form,
                                                 List<IndexDefinition> indexes,
                                                 List<ForeignKeyDefinition> foreignKeys,
                                                 JdbcFormMetadataReader metadataReader,
                                                 SchemaMigrationOptions options) {
        return planner.planJdbc(form, indexes, foreignKeys, metadataReader, options);
    }

    /** 执行明确描述的动态表结构变更集合。 */
    public long migrate(DynamicFormChangeSet changeSet) {
        return migrate(changeSet, null);
    }

    /** 使用已读取的物理快照保留变更集之外的列属性。 */
    public long migrate(DynamicFormChangeSet changeSet, SchemaSnapshot snapshot) {
        DynamicFormChangeSet safeChangeSet = Objects.requireNonNull(
                changeSet, "dynamic form change set must not be null");
        return migrationExecutor.executeWithInvalidation(snapshot == null ? renderer.migrate(safeChangeSet)
                                                                 : renderer.migrate(safeChangeSet, snapshot),
                                                          List.of(safeChangeSet.target().relationIdentity()),
                                                          metadataInvalidator::invalidate,
                                                          defaultExecutionOptions);
    }

    private JdbcSchemaClient copy(SchemaMigrationObserver newObserver,
                                  SqlExecutionOptions options,
                                  Consumer<RelationIdentity> invalidator) {
        return new JdbcSchemaClient(executor,
                                    renderer,
                                    newObserver,
                                    options,
                                    invalidator,
                                    relationalDialect);
    }

    RelationalSchemaPlanReviewer relationalReviewer() {
        return RelationalSchemaPlanReviewer.create(relationalDialect);
    }

    private SchemaCacheInvalidationCoordinator invalidatorFor(JdbcFormMetadataReader reader) {
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                reader, "jdbc form metadata reader must not be null");
        return metadataInvalidator.with(safeReader, safeReader);
    }

    static SchemaSnapshot readSnapshot(JdbcFormMetadataReader reader,
                                               RelationIdentity relation) {
        if (relation.catalog().isPresent()) {
            throw new UnsupportedOperationException(
                    "catalog-qualified schema snapshots are not supported by the JDBC reader");
        }
        return reader.readSnapshot(relation);
    }

}
