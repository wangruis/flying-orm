package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.cache.SchemaCacheInvalidationCoordinator;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 动态表结构维护的响应式门面。
 *
 * <p>客户端不可变并且可以并发共享。所有返回值保持惰性，订阅前不会读取元数据或执行 DDL；同一计划仍然按
 * 原顺序串行执行，第一条失败后停止。计划生成由 {@link SchemaMigrationPlanner} 负责，执行、观测与缓存清理由
 * {@link SchemaMigrationExecutor} 负责，本类只保留容易理解的调用入口。</p>
 *
 * <p>DDL 的事务能力由数据库和外部执行上下文决定，本类不会把不支持事务的 DDL 描述成可回滚操作。</p>
 *
 * <p>旧 String 表名保持原有语义。带 catalog、schema 或字面点号的分段关系，必须使用接受
 * {@link RelationalTableDefinition} 的关系型 Schema 入口，不能交给旧表单迁移入口。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class ReactiveSchemaClient {

    /**
     * 通用响应式执行器不承诺多条 SQL 复用同一连接，因此默认不能启用会话级锁等待设置。
     * 调用方显式配置非零锁等待后，执行链会要求同连接执行器并在缺少该能力时安全失败。
     */
    private static final SchemaMigrationExecutionOptions DEFAULT_EXECUTION_OPTIONS =
            SchemaMigrationExecutionOptions.defaults().withLockTimeout(Duration.ZERO);

    private final ReactiveSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final RdbDialect relationalDialect;
    private final SchemaMigrationObserver migrationObserver;
    private final SchemaMigrationExecutionOptions defaultExecutionOptions;
    private final SchemaDdlTransactionSupport ddlTransactionSupport;
    private final SchemaCacheInvalidationCoordinator metadataInvalidator;
    private final SchemaMigrationPlanner planner;
    private final SchemaMigrationExecutor migrationExecutor;

    private ReactiveSchemaClient(ReactiveSqlExecutor executor,
                                  FormSchemaSqlRenderer renderer,
                                  SchemaMigrationObserver observer,
                                  SchemaMigrationExecutionOptions executionOptions,
                                  SchemaDdlTransactionSupport ddlTransactionSupport,
                                  Consumer<String> metadataInvalidator,
                                  RdbDialect relationalDialect) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema sql renderer must not be null");
        this.relationalDialect = relationalDialect;
        this.migrationObserver = SchemaMigrationObservers.safe(observer);
        this.defaultExecutionOptions = Objects.requireNonNull(
                executionOptions, "default schema migration execution options must not be null");
        this.ddlTransactionSupport = Objects.requireNonNull(
                ddlTransactionSupport, "DDL transaction support must not be null");
        this.metadataInvalidator = SchemaCacheInvalidationCoordinator.from(metadataInvalidator);
        this.planner = new SchemaMigrationPlanner(renderer);
        this.migrationExecutor = new SchemaMigrationExecutor(
                executor, renderer, migrationObserver, ddlTransactionSupport);
    }

    /** 使用显式 Schema 渲染器创建客户端。 */
    public static ReactiveSchemaClient create(ReactiveSqlExecutor executor, FormSchemaSqlRenderer renderer) {
        return new ReactiveSchemaClient(executor, renderer,
                                        SchemaMigrationObserver.noop(),
                                        DEFAULT_EXECUTION_OPTIONS,
                                        SchemaDdlTransactionSupport.UNKNOWN,
                                        ignored -> {
                                        },
                                        null);
    }

    /** 使用自定义 Schema 渲染器时显式声明 DDL 事务能力；未知能力默认不会加入外部事务。 */
    public static ReactiveSchemaClient create(ReactiveSqlExecutor executor,
                                              FormSchemaSqlRenderer renderer,
                                              SchemaDdlTransactionSupport transactionSupport) {
        return new ReactiveSchemaClient(executor, renderer,
                                        SchemaMigrationObserver.noop(),
                                        DEFAULT_EXECUTION_OPTIONS, transactionSupport,
                                        ignored -> {
                                        },
                                        null);
    }

    /** 使用 RDB 方言创建客户端。 */
    public static ReactiveSchemaClient create(ReactiveSqlExecutor executor, RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new ReactiveSchemaClient(executor,
                                        FormSchemaSqlRenderer.create(safeDialect),
                                        SchemaMigrationObserver.noop(),
                                        DEFAULT_EXECUTION_OPTIONS,
                                        SchemaDdlTransactionSupport.from(safeDialect),
                                        ignored -> {
                                        },
                                        safeDialect);
    }

    /** 返回追加迁移观测的新客户端；普通 observer 故障与数据库结果隔离，JVM 致命错误仍原样传播。 */
    public ReactiveSchemaClient withMigrationObserver(SchemaMigrationObserver observer) {
        return new ReactiveSchemaClient(
                executor, renderer, observer, defaultExecutionOptions,
                ddlTransactionSupport, metadataInvalidator, relationalDialect);
    }

    /** 返回带默认迁移执行保护的新客户端。 */
    public ReactiveSchemaClient withDefaultMigrationExecutionOptions(SchemaMigrationExecutionOptions options) {
        return new ReactiveSchemaClient(
                executor, renderer, migrationObserver, options,
                ddlTransactionSupport, metadataInvalidator, relationalDialect);
    }

    /**
     * 为直接 DDL 入口配置表级元数据失效回调。
     *
     * <p>仅在执行已经开始后调用；回调失败不会改写已经确定的 DDL 成功、失败或取消结果。</p>
     *
     * @param invalidator 目标表缓存失效回调
     * @return 共享执行器和渲染器、仅替换失效回调的新客户端
     */
    @InternalApi
    public ReactiveSchemaClient withMetadataInvalidator(Consumer<String> invalidator) {
        return new ReactiveSchemaClient(executor,
                                        renderer,
                                        migrationObserver,
                                        defaultExecutionOptions,
                                        ddlTransactionSupport,
                                        Objects.requireNonNull(invalidator,
                                                               "schema metadata invalidator must not be null"),
                                        relationalDialect);
    }

    /** 创建动态表单对应的物理表。 */
    public Mono<Long> createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.createTable(safeForm),
                                                          metadataTables(safeForm),
                                                          metadataInvalidator,
                                                          defaultExecutionOptions.sqlExecutionOptions());
    }

    /** 表不存在时建表，已存在时只执行安全迁移。 */
    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexMetadata> indexes,
                                    ReactiveFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader).map(SchemaMigrationResult::rowsUpdated);
    }

    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexMetadata> indexes,
                                    ReactiveFormMetadataReader metadataReader,
                                    SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, metadataReader, options).map(SchemaMigrationResult::rowsUpdated);
    }

    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexMetadata> indexes,
                                    List<ForeignKeyMetadata> foreignKeys,
                                    ReactiveFormMetadataReader metadataReader,
                                    SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, foreignKeys, metadataReader, options)
                .map(SchemaMigrationResult::rowsUpdated);
    }

    /** 生成安全计划、执行并返回计划和影响行数。 */
    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexMetadata> indexes,
                                                             ReactiveFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexMetadata> indexes,
                                                             ReactiveFormMetadataReader metadataReader,
                                                             SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, List.of(), metadataReader, options);
    }

    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexMetadata> indexes,
                                                             List<ForeignKeyMetadata> foreignKeys,
                                                             ReactiveFormMetadataReader metadataReader,
                                                             SchemaMigrationOptions options) {
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        if (safeOptions.requiresReviewedExecution()) {
            return Mono.error(new IllegalStateException(
                    "dangerous schema migration options require reviewCreateOrAlter and executeReviewed"));
        }
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
         return planner.plan(form, indexes, foreignKeys, safeReader, safeOptions)
                       .flatMap(plan -> migrationExecutor.executeWithInvalidation(
                              plan.requests(), metadataTables(form, plan.additionalCreatedTables()),
                               invalidatorFor(safeReader),
                              defaultExecutionOptions.sqlExecutionOptions())
                              .map(rows -> new SchemaMigrationResult(plan, rows, List.of())));
    }

    /** 生成带风险、回滚缺口和在线 DDL 约束的审核计划，不执行 SQL。 */
    public Mono<ReviewedSchemaMigrationPlan> reviewCreateOrAlter(
            DynamicForm form,
            List<IndexMetadata> indexes,
            List<ForeignKeyMetadata> foreignKeys,
            ReactiveFormMetadataReader metadataReader,
            SchemaMigrationOptions migrationOptions,
            SchemaMigrationReviewPolicy reviewPolicy) {
        return planner.review(form, indexes, foreignKeys, metadataReader, migrationOptions, reviewPolicy);
    }

    /** 执行已审核计划；精确批准、观测和缓存失效都由同一执行组件处理。 */
    public Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationExecutionOptions options) {
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                reviewedPlan, "reviewed schema migration plan must not be null");
        return migrationExecutor.executeReviewed(
                safePlan, metadataTables(safePlan.migration()), invalidatorFor(safeReader), options);
    }

    /** 使用客户端装配时设置的默认迁移执行保护。 */
    public Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader) {
        return executeReviewed(reviewedPlan, metadataReader, defaultExecutionOptions);
    }

    /** 使用客户端默认执行保护，并带上当前审核计划的精确批准。 */
    public Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationApproval approval) {
        return executeReviewed(reviewedPlan, metadataReader, defaultExecutionOptions.withApproval(approval));
    }

    /** 执行冻结 SQL，并用同一个响应式 reader 做前置指纹和执行后结构验证。 */
    public Mono<SchemaExecutionReport> executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationExecutionOptions options) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                reviewedPlan, "reviewed schema plan must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        RelationIdentity relation = safePlan.desiredTable()
                .orElseThrow(() -> new IllegalArgumentException(
                        "reviewed schema plan must contain a desired verification table"))
                .identity();
        Consumer<String> invalidator = invalidatorFor(safeReader);
        return migrationExecutor.executeReviewed(
                safePlan,
                () -> readSnapshot(safeReader, relation),
                safeReader::snapshotCoverage,
                () -> invalidator.accept(qualifiedName(relation)),
                options);
    }

    /** 订阅后直读一次当前结构，并从完整关系模型生成冻结 SQL 的审核计划。 */
    public Mono<ReviewedSchemaPlan> reviewRelational(DatabaseDescriptor database,
                                                     RelationalTableDefinition desired,
                                                     ReactiveFormMetadataReader metadataReader,
                                                     SchemaCompatibilityMode mode) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationalTableDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        return Mono.defer(() -> readSnapshot(safeReader, safeDesired.identity()))
                .map(actual -> reviewer.review(database, safeDesired, actual, safeReader.snapshotCoverage(), mode));
    }

    /** 使用客户端默认的逐条 SQL 执行保护。 */
    public Mono<SchemaExecutionReport> executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader) {
        return executeReviewed(reviewedPlan, metadataReader, defaultExecutionOptions);
    }

    /** 使用客户端默认保护，并带上完整关系计划的精确批准。 */
    public Mono<SchemaExecutionReport> executeReviewed(ReviewedSchemaPlan reviewedPlan,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationApproval approval) {
        return executeReviewed(
                reviewedPlan, metadataReader, defaultExecutionOptions.withApproval(approval));
    }

    /** 只生成安全迁移计划，不执行 SQL。 */
    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexMetadata> indexes,
                                                       ReactiveFormMetadataReader metadataReader) {
        return planCreateOrAlter(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexMetadata> indexes,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        return planCreateOrAlter(form, indexes, List.of(), metadataReader, options);
    }

    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexMetadata> indexes,
                                                       List<ForeignKeyMetadata> foreignKeys,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        return planner.plan(form, indexes, foreignKeys, metadataReader, options);
    }

    /** 执行调用方已经明确描述的动态表单变更集。 */
    public Mono<Long> migrate(DynamicFormChangeSet changeSet) {
        DynamicFormChangeSet safeChangeSet = Objects.requireNonNull(
                changeSet, "dynamic form change set must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.migrate(safeChangeSet),
                                                          List.of(safeChangeSet.target().table()),
                                                          metadataInvalidator,
                                                          defaultExecutionOptions.sqlExecutionOptions());
    }

    /** 自动迁移把参与规划的 reader 按身份加入统一失效协调点。 */
    private Consumer<String> invalidatorFor(ReactiveFormMetadataReader reader) {
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                reader, "reactive form metadata reader must not be null");
        return metadataInvalidator.with(safeReader, safeReader::invalidate);
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

    static Mono<SchemaSnapshot> readSnapshot(ReactiveFormMetadataReader reader,
                                                     RelationIdentity relation) {
        if (relation.catalog().isPresent()) {
            return Mono.error(new UnsupportedOperationException(
                    "catalog-qualified schema snapshots are not supported by the reactive reader"));
        }
        return relation.schema().isPresent()
                ? reader.readSnapshot(relation.schema().orElseThrow(), relation.table())
                : reader.readSnapshot(relation.table());
    }

    private static String qualifiedName(RelationIdentity relation) {
        return relation.schema().map(schema -> schema + "." + relation.table())
                .orElseGet(relation::table);
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
