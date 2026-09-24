package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 动态表结构的原生 JDBC 客户端。
 *
 * <p>它和 {@link ReactiveSchemaClient} 共用 SQL 渲染、结构比较、危险变更审核和结果模型，
 * 但执行路径完全同步：不会创建 Reactor，也不会调用 {@code block()}。调用方只需要准备好
 * {@link SyncSqlExecutor} 和 {@link JdbcFormMetadataReader}，其余行为由客户端统一编排。</p>
 *
 * <p>旧 String 表名保持原有语义。带 catalog、schema 或字面点号的分段关系，必须使用接受
 * {@link RelationalTableDefinition} 的关系型 Schema 入口，不能交给旧表单迁移入口。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcSchemaClient {

    /** 执行和会话锁等待时限由上层管理，Schema 默认不设置治理预算。 */
    private static final SchemaMigrationExecutionOptions DEFAULT_EXECUTION_OPTIONS =
            SchemaMigrationExecutionOptions.defaults();

    private final SyncSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final RdbDialect relationalDialect;
    private final SchemaMigrationObserver observer;
    private final SchemaMigrationExecutionOptions defaultExecutionOptions;
    private final SchemaCacheInvalidationCoordinator metadataInvalidator;
    private final SchemaMigrationPlanner planner;
    private final JdbcSchemaMigrationExecutor migrationExecutor;

    private JdbcSchemaClient(SyncSqlExecutor executor,
                             FormSchemaSqlRenderer renderer,
                             SchemaMigrationObserver observer,
                             SchemaMigrationExecutionOptions executionOptions,
                             Consumer<String> metadataInvalidator,
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
                this.observer,
                this.metadataInvalidator);
    }

    /** 使用显式 Schema 渲染器创建 JDBC Schema 客户端。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor, FormSchemaSqlRenderer renderer) {
        return new JdbcSchemaClient(executor,
                                    renderer,
                                    SchemaMigrationObserver.noop(),
                                    DEFAULT_EXECUTION_OPTIONS,
                                    ignored -> {
                                    },
                                    null);
    }

    /** 根据显式 RDB 方言创建客户端。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor, RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new JdbcSchemaClient(executor,
                                    FormSchemaSqlRenderer.create(safeDialect),
                                    SchemaMigrationObserver.noop(),
                                    DEFAULT_EXECUTION_OPTIONS,
                                    ignored -> {
                                    },
                                    safeDialect);
    }

    /** 返回使用新迁移 observer 的不可变客户端。 */
    public JdbcSchemaClient withMigrationObserver(SchemaMigrationObserver migrationObserver) {
        return copy(migrationObserver, defaultExecutionOptions, metadataInvalidator);
    }

    /** 返回使用新默认执行保护的不可变客户端。 */
    public JdbcSchemaClient withDefaultMigrationExecutionOptions(SchemaMigrationExecutionOptions options) {
        return copy(observer, options, metadataInvalidator);
    }

    /**
     * 注入额外元数据缓存失效回调。自动迁移会同时清理参与规划的 JDBC reader，
     * 这里的表名回调用于通知其他共享缓存层。
     */
    public JdbcSchemaClient withMetadataInvalidator(Consumer<String> invalidator) {
        return copy(observer, defaultExecutionOptions, invalidator);
    }

    /** 创建动态表对应的物理表。 */
    public long createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.createTable(safeForm),
                                                          metadataTables(safeForm),
                                                          metadataInvalidator,
                                                          defaultExecutionOptions.sqlExecutionOptions());
    }

    /** 创建或安全调整动态表，返回影响行数汇总。 */
    public long createOrAlter(DynamicForm form,
                              List<IndexMetadata> indexes,
                              JdbcFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader).rowsUpdated();
    }

    public long createOrAlter(DynamicForm form,
                              List<IndexMetadata> indexes,
                              JdbcFormMetadataReader metadataReader,
                              SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, List.of(), metadataReader, options).rowsUpdated();
    }

    /** 规划、执行并返回完整迁移结果；危险选项必须先走审核入口。 */
    public SchemaMigrationResult createOrAlterDetailed(DynamicForm form,
                                                       List<IndexMetadata> indexes,
                                                       JdbcFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form,
                                     indexes,
                                     List.of(),
                                     metadataReader,
                                     SchemaMigrationOptions.safe());
    }

    public SchemaMigrationResult createOrAlterDetailed(DynamicForm form,
                                                       List<IndexMetadata> indexes,
                                                       List<ForeignKeyMetadata> foreignKeys,
                                                       JdbcFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        if (safeOptions.requiresReviewedExecution()) {
            throw new IllegalStateException(
                    "dangerous schema migration options require reviewCreateOrAlter and executeReviewed");
        }
        SchemaMigrationPlan plan = planner.planJdbc(form, indexes, foreignKeys, metadataReader, safeOptions);
        long rows = migrationExecutor.executeWithInvalidation(
                plan.requests(), metadataTables(form, plan.additionalCreatedTables()), invalidatorFor(metadataReader),
                defaultExecutionOptions.sqlExecutionOptions());
        return new SchemaMigrationResult(plan, rows, List.of());
    }

    /** 生成带风险、回滚缺口和在线 DDL 判断的审核计划，不执行 SQL。 */
    public ReviewedSchemaMigrationPlan reviewCreateOrAlter(DynamicForm form,
                                                           List<IndexMetadata> indexes,
                                                           List<ForeignKeyMetadata> foreignKeys,
                                                           JdbcFormMetadataReader metadataReader,
                                                           SchemaMigrationOptions migrationOptions,
                                                           SchemaMigrationReviewPolicy reviewPolicy) {
        return planner.reviewJdbc(form,
                              indexes,
                              foreignKeys,
                              metadataReader,
                              migrationOptions,
                              reviewPolicy);
    }

    /** 执行审核通过且指纹匹配的计划。 */
    public SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                 SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                reviewedPlan, "reviewed schema migration plan must not be null");
        return migrationExecutor.executeReviewed(safePlan, metadataTables(safePlan.migration()), options);
    }

    /** 使用客户端默认的 DDL 执行保护执行审核计划。 */
    public SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan) {
        return executeReviewed(reviewedPlan, defaultExecutionOptions);
    }

    /** 使用客户端默认执行保护，并带上当前审核计划的精确批准。 */
    public SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                 SchemaMigrationApproval approval) {
        return executeReviewed(reviewedPlan, defaultExecutionOptions.withApproval(approval));
    }

    /** 实体完整同步让参与审核的 reader 与客户端既有缓存一起失效。 */
    SchemaMigrationResult executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                          JdbcFormMetadataReader metadataReader,
                                          SchemaMigrationApproval approval) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                reviewedPlan, "reviewed schema migration plan must not be null");
        SchemaMigrationExecutionOptions options = approval == null
                ? defaultExecutionOptions : defaultExecutionOptions.withApproval(approval);
        return new JdbcSchemaMigrationExecutor(executor, observer, invalidatorFor(metadataReader))
                .executeReviewed(safePlan, metadataTables(safePlan.migration()), options);
    }

    /** 执行冻结 SQL，并用同一个 JDBC reader 做前置指纹和执行后结构验证。 */
    public SchemaExecutionReport executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                 JdbcFormMetadataReader metadataReader,
                                                 SchemaMigrationExecutionOptions options) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                reviewedPlan, "reviewed schema plan must not be null");
        safePlan.requireExecutionDialect(relationalDialect);
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        RelationIdentity relation = safePlan.targetIdentity()
                .orElseThrow(() -> new IllegalArgumentException(
                        "reviewed schema plan must contain a verification target"));
        SchemaCacheInvalidationCoordinator invalidator = metadataInvalidator.with(
                safeReader, safeReader);
        return migrationExecutor.executeReviewed(
                safePlan,
                () -> readSnapshot(safeReader, relation),
                safeReader::snapshotCoverage,
                () -> invalidator.invalidate(relation),
                options);
    }

    /**
     * 直读一次当前结构，并从完整关系模型生成冻结 SQL 的审核计划。
     * 自定义旧 renderer 没有对应的关系方言事实，因此该入口只对按 {@link RdbDialect} 创建的客户端开放。
     */
    public ReviewedSchemaPlan reviewRelational(DatabaseDescriptor database,
                                               RelationalTableDefinition desired,
                                               JdbcFormMetadataReader metadataReader,
                                               SchemaCompatibilityMode mode) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationalTableDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "jdbc form metadata reader must not be null");
        SchemaSnapshot actual = readSnapshot(safeReader, safeDesired.identity());
        return reviewer.review(database, safeDesired, actual, safeReader.snapshotCoverage(), mode);
    }

    /**
     * 直读一次当前结构，并审核调用方明确提出的关系删除目标。删除始终使用精确兼容模式，
     * 不会从缺少实体或表定义推断删除。
     */
    public ReviewedSchemaPlan reviewRelationalAbsent(DatabaseDescriptor database,
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

    /** 使用客户端默认的逐条 SQL 执行保护。 */
    public SchemaExecutionReport executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                  JdbcFormMetadataReader metadataReader) {
        return executeReviewed(reviewedPlan, metadataReader, defaultExecutionOptions);
    }

    /** 使用客户端默认保护，并带上完整关系计划的精确批准。 */
    public SchemaExecutionReport executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                  JdbcFormMetadataReader metadataReader,
                                                  SchemaMigrationApproval approval) {
        return executeReviewed(
                reviewedPlan, metadataReader, defaultExecutionOptions.withApproval(approval));
    }

    /** 只生成安全迁移计划，不执行 SQL。 */
    public SchemaMigrationPlan planCreateOrAlter(DynamicForm form,
                                                 List<IndexMetadata> indexes,
                                                 JdbcFormMetadataReader metadataReader) {
        return planCreateOrAlter(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public SchemaMigrationPlan planCreateOrAlter(DynamicForm form,
                                                 List<IndexMetadata> indexes,
                                                 JdbcFormMetadataReader metadataReader,
                                                 SchemaMigrationOptions options) {
        return planner.planJdbc(form, indexes, List.of(), metadataReader, options);
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
                                                          List.of(safeChangeSet.target().table()),
                                                          metadataInvalidator,
                                                          defaultExecutionOptions.sqlExecutionOptions());
    }

    private JdbcSchemaClient copy(SchemaMigrationObserver newObserver,
                                  SchemaMigrationExecutionOptions options,
                                  Consumer<String> invalidator) {
        return new JdbcSchemaClient(executor,
                                    renderer,
                                    newObserver,
                                    options,
                                    invalidator,
                                    relationalDialect);
    }

    RelationalSchemaPlanReviewer relationalReviewer() {
        return RelationalSchemaPlanReviewer.create(requireRelationalDialect());
    }

    private RdbDialect requireRelationalDialect() {
        if (relationalDialect == null) {
            throw new UnsupportedOperationException(
                    "relational schema review requires a client created with an RDB dialect");
        }
        return relationalDialect;
    }

    private Consumer<String> invalidatorFor(JdbcFormMetadataReader reader) {
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

    private static List<String> metadataTables(SchemaMigrationPlan plan) {
        SchemaMigrationPlan safePlan = Objects.requireNonNull(plan, "schema migration plan must not be null");
        return metadataTables(safePlan.target(), safePlan.additionalCreatedTables());
    }

    private static List<String> metadataTables(DynamicForm form) {
        return metadataTables(form, List.of());
    }

    private static List<String> metadataTables(DynamicForm form, List<String> additionalTables) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        LinkedHashSet<String> tables = new LinkedHashSet<>();
        tables.add(safeForm.table());
        tables.addAll(Objects.requireNonNull(additionalTables, "additional schema tables must not be null"));
        ProtectedContainsLayout.resolve(safeForm)
                               .map(ProtectedContainsLayout::table)
                               .map(DynamicForm::table)
                               .ifPresent(tables::add);
        return List.copyOf(tables);
    }

}
