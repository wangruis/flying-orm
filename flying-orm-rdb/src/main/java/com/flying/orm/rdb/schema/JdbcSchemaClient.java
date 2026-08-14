package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import java.time.Duration;
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
 * <p>外部 JDBC 事务由上层系统管理。需要让 Schema 感知外部事务时，传入与
 * {@code SyncSqlExecutor} 使用同一个 {@link JdbcTransactionParticipant}；客户端只检查和复用，
 * 不提交、不回滚，也不关闭上层持有的连接。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcSchemaClient {

    /**
     * 同步 JDBC 执行器没有公开的连接级 session 能力，所以默认只开启单条 SQL 的执行保护。
     * 锁超时需要调用方显式打开，并提供外部事务连接来保证 setup/work/cleanup 使用同一会话。
     */
    private static final SchemaMigrationExecutionOptions DEFAULT_EXECUTION_OPTIONS =
            SchemaMigrationExecutionOptions.defaults().withLockTimeout(Duration.ZERO);

    private final SyncSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final SchemaMigrationObserver observer;
    private final SchemaMigrationExecutionOptions defaultExecutionOptions;
    private final SchemaDdlTransactionSupport ddlTransactionSupport;
    private final JdbcTransactionParticipant transactionParticipant;
    private final Consumer<String> metadataInvalidator;
    private final JdbcSchemaMigrationPlanner planner;
    private final JdbcSchemaMigrationExecutor migrationExecutor;

    private JdbcSchemaClient(SyncSqlExecutor executor,
                             FormSchemaSqlRenderer renderer,
                             SchemaMigrationObserver observer,
                             SchemaMigrationExecutionOptions executionOptions,
                             SchemaDdlTransactionSupport ddlTransactionSupport,
                             JdbcTransactionParticipant transactionParticipant,
                             Consumer<String> metadataInvalidator) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema SQL renderer must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
        this.defaultExecutionOptions = Objects.requireNonNull(
                executionOptions, "default schema migration execution options must not be null");
        this.ddlTransactionSupport = Objects.requireNonNull(
                ddlTransactionSupport, "DDL transaction support must not be null");
        this.transactionParticipant = Objects.requireNonNull(
                transactionParticipant, "jdbc transaction participant must not be null");
        this.metadataInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        this.planner = new JdbcSchemaMigrationPlanner(this.renderer);
        this.migrationExecutor = new JdbcSchemaMigrationExecutor(
                this.executor,
                this.renderer,
                this.observer,
                this.ddlTransactionSupport,
                this.transactionParticipant,
                this.metadataInvalidator);
    }

    /** 使用默认的无外部事务检查创建 JDBC Schema 客户端。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor, FormSchemaSqlRenderer renderer) {
        return new JdbcSchemaClient(executor,
                                    renderer,
                                    SchemaMigrationObserver.noop(),
                                    DEFAULT_EXECUTION_OPTIONS,
                                    SchemaDdlTransactionSupport.UNKNOWN,
                                    JdbcTransactionParticipant.none(),
                                    ignored -> {
                                    });
    }

    /** 根据显式 RDB 方言创建客户端；方言决定 DDL 外部事务能力和 SQL 形状。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor, RdbDialect dialect) {
        return create(executor, dialect, JdbcTransactionParticipant.none());
    }

    /** 根据方言和外部事务参与者创建客户端。 */
    public static JdbcSchemaClient create(SyncSqlExecutor executor,
                                          RdbDialect dialect,
                                          JdbcTransactionParticipant transactionParticipant) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new JdbcSchemaClient(executor,
                                    FormSchemaSqlRenderer.create(safeDialect),
                                    SchemaMigrationObserver.noop(),
                                    DEFAULT_EXECUTION_OPTIONS,
                                    SchemaDdlTransactionSupport.from(safeDialect),
                                    transactionParticipant,
                                    ignored -> {
                                    });
    }

    /** 返回使用新迁移 observer 的不可变客户端。 */
    public JdbcSchemaClient withMigrationObserver(SchemaMigrationObserver migrationObserver) {
        return copy(migrationObserver, defaultExecutionOptions, metadataInvalidator);
    }

    /** 返回使用新默认执行保护的不可变客户端。 */
    public JdbcSchemaClient withDefaultMigrationExecutionOptions(SchemaMigrationExecutionOptions options) {
        return copy(observer, options, metadataInvalidator);
    }

    /** 自定义 renderer 时显式声明 DDL 能否加入上层事务；未知能力默认按不允许处理。 */
    public JdbcSchemaClient withDdlTransactionSupport(SchemaDdlTransactionSupport support) {
        return new JdbcSchemaClient(executor,
                                    renderer,
                                    observer,
                                    defaultExecutionOptions,
                                    support,
                                    transactionParticipant,
                                    metadataInvalidator);
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
        SchemaMigrationPlan plan = planner.plan(form, indexes, foreignKeys, metadataReader, safeOptions);
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
        return planner.review(form,
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
        return planner.plan(form, indexes, List.of(), metadataReader, options);
    }

    /** 执行明确描述的动态表结构变更集合。 */
    public long migrate(DynamicFormChangeSet changeSet) {
        DynamicFormChangeSet safeChangeSet = Objects.requireNonNull(
                changeSet, "dynamic form change set must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.migrate(safeChangeSet),
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
                                    ddlTransactionSupport,
                                    transactionParticipant,
                                    invalidator);
    }

    private Consumer<String> invalidatorFor(JdbcFormMetadataReader reader) {
        JdbcFormMetadataReader safeReader = Objects.requireNonNull(
                reader, "jdbc form metadata reader must not be null");
        return table -> {
            try {
                safeReader.invalidate(table);
            } catch (RuntimeException failure) {
                SchemaMigrationObservers.rethrowVirtualMachineError(failure);
            }
            try {
                metadataInvalidator.accept(table);
            } catch (RuntimeException failure) {
                SchemaMigrationObservers.rethrowVirtualMachineError(failure);
            }
        };
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
