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
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static com.flying.orm.rdb.schema.SchemaMigrationSupport.metadataTables;
import static com.flying.orm.rdb.schema.SchemaMigrationSupport.metadataRelations;

/**
 * 动态表结构维护的响应式门面。
 *
 * <p>客户端不可变并且可以并发共享。所有返回值保持惰性，订阅前不会读取元数据或执行 DDL；同一计划仍然按
 * 原顺序串行执行，第一条失败后停止。计划生成由 {@link SchemaMigrationPlanner} 负责，执行、观测与缓存清理由
 * {@link SchemaMigrationExecutor} 负责，本类只保留容易理解的调用入口。</p>
 *
 * <p>执行结果仅描述 SQL 与结构回读事实，多语句一致性由上层保证。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class ReactiveSchemaClient {

    /** 执行和会话锁等待时限由上层管理，Schema 默认不设置治理预算。 */
    private static final SqlExecutionOptions DEFAULT_EXECUTION_OPTIONS =
            SqlExecutionOptions.unlimited();

    private final ReactiveSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final RdbDialect relationalDialect;
    private final SchemaMigrationObserver migrationObserver;
    private final SqlExecutionOptions defaultExecutionOptions;
    private final SchemaCacheInvalidationCoordinator metadataInvalidator;
    private final SchemaMigrationPlanner planner;
    private final SchemaMigrationExecutor migrationExecutor;

    private ReactiveSchemaClient(ReactiveSqlExecutor executor,
                                  FormSchemaSqlRenderer renderer,
                                  SchemaMigrationObserver observer,
                                  SqlExecutionOptions executionOptions,
                                  Consumer<RelationIdentity> metadataInvalidator,
                                  RdbDialect relationalDialect) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema sql renderer must not be null");
        this.relationalDialect = relationalDialect;
        this.migrationObserver = SchemaMigrationObservers.safe(observer);
        this.defaultExecutionOptions = Objects.requireNonNull(
                executionOptions, "default schema migration execution options must not be null");
        this.metadataInvalidator = SchemaCacheInvalidationCoordinator.from(metadataInvalidator);
        this.planner = this.renderer.migrationPlanner();
        this.migrationExecutor = new SchemaMigrationExecutor(
                executor, migrationObserver);
    }

    /** 使用 RDB 方言创建客户端；自定义 DDL 使用 {@link RdbDialect#withSchema(SchemaDialect)}。 */
    public static ReactiveSchemaClient create(ReactiveSqlExecutor executor, RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        return new ReactiveSchemaClient(executor,
                                        FormSchemaSqlRenderer.create(safeDialect),
                                        SchemaMigrationObserver.noop(),
                                        DEFAULT_EXECUTION_OPTIONS,
                                        SchemaCacheInvalidationCoordinator.of(),
                                        safeDialect);
    }

    /** 返回追加迁移观测的新客户端；普通 observer 故障与数据库结果隔离，JVM 致命错误仍原样传播。 */
    public ReactiveSchemaClient withMigrationObserver(SchemaMigrationObserver observer) {
        return new ReactiveSchemaClient(
                executor, renderer, observer, defaultExecutionOptions,
                metadataInvalidator, relationalDialect);
    }

    /** 返回带默认迁移执行保护的新客户端。 */
    public ReactiveSchemaClient withDefaultMigrationExecutionOptions(SqlExecutionOptions options) {
        return new ReactiveSchemaClient(
                executor, renderer, migrationObserver, options,
                metadataInvalidator, relationalDialect);
    }

    /**
     * 为直接 DDL 入口配置表级元数据失效回调。
     *
     * <p>仅在执行已经开始后调用；失效失败保留已执行事实，DDL 同时失败时原始数据库失败优先。</p>
     *
     * @param invalidator 目标表缓存失效回调
     * @return 共享执行器和渲染器、仅替换失效回调的新客户端
     */
    public ReactiveSchemaClient withMetadataInvalidator(Consumer<RelationIdentity> invalidator) {
        return new ReactiveSchemaClient(executor,
                                        renderer,
                                        migrationObserver,
                                        defaultExecutionOptions,
                                        Objects.requireNonNull(invalidator,
                                                               "schema metadata invalidator must not be null"),
                                        relationalDialect);
    }

    /** 创建动态表单对应的物理表。 */
    public Mono<Long> createTable(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return migrationExecutor.executeWithInvalidation(renderer.createTable(safeForm),
                                                          metadataTables(safeForm),
                                                          metadataInvalidator::invalidate,
                                                          defaultExecutionOptions);
    }

    /** 表不存在时建表，已存在时只执行安全迁移。 */
    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexDefinition> indexes,
                                    ReactiveFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader).map(SchemaMigrationResult::rowsUpdated);
    }

    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexDefinition> indexes,
                                    ReactiveFormMetadataReader metadataReader,
                                    SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, metadataReader, options).map(SchemaMigrationResult::rowsUpdated);
    }

    public Mono<Long> createOrAlter(DynamicForm form,
                                    List<IndexDefinition> indexes,
                                    List<ForeignKeyDefinition> foreignKeys,
                                    ReactiveFormMetadataReader metadataReader,
                                    SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, foreignKeys, metadataReader, options)
                .map(SchemaMigrationResult::rowsUpdated);
    }

    /** 生成安全计划、执行并返回计划和影响行数。 */
    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexDefinition> indexes,
                                                             ReactiveFormMetadataReader metadataReader) {
        return createOrAlterDetailed(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexDefinition> indexes,
                                                             ReactiveFormMetadataReader metadataReader,
                                                             SchemaMigrationOptions options) {
        return createOrAlterDetailed(form, indexes, List.of(), metadataReader, options);
    }

    public Mono<SchemaMigrationResult> createOrAlterDetailed(DynamicForm form,
                                                             List<IndexDefinition> indexes,
                                                             List<ForeignKeyDefinition> foreignKeys,
                                                             ReactiveFormMetadataReader metadataReader,
                                                             SchemaMigrationOptions options) {
        SchemaMigrationOptions safeOptions = Objects.requireNonNull(options,
                                                                     "schema migration options must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        return planner.plan(form, indexes, foreignKeys, safeReader, safeOptions)
                .flatMap(plan -> execute(plan, safeReader, defaultExecutionOptions));
    }

    /** 执行冻结计划；执行观测和缓存失效都由同一执行组件处理。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SqlExecutionOptions options) {
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        SchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "schema migration plan must not be null");
        safePlan.requireExecutionDialect(relationalDialect);
        return migrationExecutor.execute(
                safePlan, metadataRelations(safePlan), invalidatorFor(safeReader)::invalidate, options);
    }

    /** 使用客户端装配时设置的默认迁移执行保护。 */
    public Mono<SchemaMigrationResult> execute(SchemaMigrationPlan plan,
                                                       ReactiveFormMetadataReader metadataReader) {
        return execute(plan, metadataReader, defaultExecutionOptions);
    }

    /** 订阅后直读一次当前结构，并从完整关系模型生成冻结 SQL 的审核计划。 */
    public Mono<SchemaMigrationPlan> reviewRelational(DatabaseDescriptor database,
                                                     RelationalTableDefinition desired,
                                                     ReactiveFormMetadataReader metadataReader,
                                                     SchemaCompatibilityMode mode) {
        return planRelational(database, desired, metadataReader, mode, false);
    }

    /** 订阅时读取并增量规划，未声明的现有对象保持不变。 */
    public Mono<SchemaMigrationPlan> planCreateOrAlter(DatabaseDescriptor database,
                                                     RelationalTableDefinition declared,
                                                     ReactiveFormMetadataReader metadataReader) {
        return planRelational(database, declared, metadataReader, SchemaCompatibilityMode.SAFE_INCREMENTAL, true);
    }

    private Mono<SchemaMigrationPlan> planRelational(DatabaseDescriptor database,
                                                    RelationalTableDefinition desired,
                                                    ReactiveFormMetadataReader metadataReader,
                                                    SchemaCompatibilityMode mode,
                                                    boolean incremental) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationalTableDefinition safeDesired = Objects.requireNonNull(
                desired, "desired relational table must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        return Mono.defer(() -> readSnapshot(safeReader, safeDesired.identity()))
                .map(actual -> incremental
                        ? reviewer.planIncremental(database, safeDesired, actual, safeReader.snapshotCoverage())
                        : reviewer.review(database, safeDesired, actual, safeReader.snapshotCoverage(), mode));
    }

    /**
     * 订阅后直读一次当前结构，并审核调用方明确提出的关系删除目标。删除始终使用精确兼容模式，
     * 不会从缺少实体或表定义推断删除。
     */
    public Mono<SchemaMigrationPlan> reviewRelationalAbsent(
            DatabaseDescriptor database,
            RelationIdentity target,
            ReactiveFormMetadataReader metadataReader) {
        RelationalSchemaPlanReviewer reviewer = relationalReviewer();
        RelationIdentity safeTarget = Objects.requireNonNull(
                target, "absent relational target identity must not be null");
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                metadataReader, "reactive form metadata reader must not be null");
        return Mono.defer(() -> readSnapshot(safeReader, safeTarget))
                .map(actual -> reviewer.reviewAbsent(
                        database, safeTarget, actual, safeReader.snapshotCoverage()));
    }

    /** 只生成安全迁移计划，不执行 SQL。 */
    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexDefinition> indexes,
                                                       ReactiveFormMetadataReader metadataReader) {
        return planCreateOrAlter(form, indexes, metadataReader, SchemaMigrationOptions.safe());
    }

    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexDefinition> indexes,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        return planCreateOrAlter(form, indexes, List.of(), metadataReader, options);
    }

    public Mono<SchemaMigrationPlan> planCreateOrAlter(DynamicForm form,
                                                       List<IndexDefinition> indexes,
                                                       List<ForeignKeyDefinition> foreignKeys,
                                                       ReactiveFormMetadataReader metadataReader,
                                                       SchemaMigrationOptions options) {
        return planner.plan(form, indexes, foreignKeys, metadataReader, options);
    }

    /** 执行调用方已经明确描述的动态表单变更集。 */
    public Mono<Long> migrate(DynamicFormChangeSet changeSet) {
        return migrate(changeSet, null);
    }

    /** 使用已读取的物理快照保留变更集之外的列属性。 */
    public Mono<Long> migrate(DynamicFormChangeSet changeSet, SchemaSnapshot snapshot) {
        DynamicFormChangeSet safeChangeSet = Objects.requireNonNull(
                changeSet, "dynamic form change set must not be null");
        return migrationExecutor.executeWithInvalidation(snapshot == null ? renderer.migrate(safeChangeSet)
                                                                 : renderer.migrate(safeChangeSet, snapshot),
                                                          List.of(safeChangeSet.target().relationIdentity()),
                                                          metadataInvalidator::invalidate,
                                                          defaultExecutionOptions);
    }

    /** 自动迁移把参与规划的 reader 按身份加入统一失效协调点。 */
    private SchemaCacheInvalidationCoordinator invalidatorFor(ReactiveFormMetadataReader reader) {
        ReactiveFormMetadataReader safeReader = Objects.requireNonNull(
                reader, "reactive form metadata reader must not be null");
        return metadataInvalidator.with(safeReader, safeReader);
    }

    RelationalSchemaPlanReviewer relationalReviewer() {
        return RelationalSchemaPlanReviewer.create(relationalDialect);
    }

    static Mono<SchemaSnapshot> readSnapshot(ReactiveFormMetadataReader reader,
                                                     RelationIdentity relation) {
        if (relation.catalog().isPresent()) {
            return Mono.error(new UnsupportedOperationException(
                    "catalog-qualified schema snapshots are not supported by the reactive reader"));
        }
        return reader.readSnapshot(relation);
    }

}
