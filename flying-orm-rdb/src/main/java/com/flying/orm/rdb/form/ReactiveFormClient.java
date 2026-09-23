package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 动态表单响应式 CRUD 的稳定公开入口。
 *
 * <p>客户端本身只保存不可变配置和对外 API。查询、分页、写入、批量写入、Scope 合并、字段解码等细节
 * 由同包内部操作协作者完成，所以一个已经装配好的客户端可以安全复用给并发订阅。所有数据库 I/O 仍在
 * Reactor 订阅链内发生：这里不会调用 {@code block()} 或 {@code subscribe()}，也不会先收集整个输入流。</p>
 *
 * <p>同包内部的细粒度操作按职责放在 {@link ReactiveFormOperations} 管理的协作者中，同步门面直接复用这些
 * 协作者，因此 R2DBC、SQL、Scope、逻辑删除、乐观锁和执行保护始终只有一套实现。业务代码优先使用
 * QuerySpec、WriteSpec、BatchSpec 三类不可变规格，避免在调用点拼接 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
public final class ReactiveFormClient {

    private final ReactiveSqlExecutor executor;
    private final FormConfiguration configuration;
    private final ReactiveFormOperations operations;
    private final FormDataSqlRenderer renderer;
    private final FormResultDecoder results;

    private ReactiveFormClient(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer) {
        this(executor, defaults(renderer));
    }

    private ReactiveFormClient(ReactiveSqlExecutor executor, FormConfiguration configuration) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "form configuration must not be null");
        this.operations = new ReactiveFormOperations(this.executor, configuration);
        this.renderer = configuration.renderer();
        this.results = operations.results;
    }

    /** 创建动态表单响应式客户端。 */
    public static ReactiveFormClient create(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer) {
        return new ReactiveFormClient(executor, renderer);
    }

    /** 为前端结构化条件替换线程安全的解析器。 */
    public ReactiveFormClient withStructuredConditionResolver(StructuredConditionResolver resolver) {
        return configured(configuration.withResolver(resolver));
    }

    /** 设置没有显式 options 时使用的 SQL 执行保护。 */
    public ReactiveFormClient withDefaultExecutionOptions(SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return new ReactiveFormClient(
                executor.withDefaultExecutionOptions(safeOptions), configuration.withExecutionOptions(safeOptions));
    }

    /** 设置没有显式 options 时使用的批量策略。 */
    public ReactiveFormClient withDefaultBatchWriteOptions(BatchWriteOptions options) {
        return configured(configuration.withBatchOptions(
                Objects.requireNonNull(options, "batch write options must not be null")));
    }

    /** 在客户端默认范围上继续收窄。 */
    public ReactiveFormClient withDefaultDataScope(DataScope scope) {
        DataScope combined = configuration.dataScope().and(Objects.requireNonNull(
                scope, "data scope must not be null"));
        return configured(configuration.withDataScope(combined));
    }

    /** 返回绑定字段用途策略的不可变调用视图。 */
    public ReactiveFormClient withFieldUsePolicy(FieldUsePolicy policy) {
        return configured(configuration.withFieldUsePolicy(
                Objects.requireNonNull(policy, "field use policy must not be null")));
    }

    /** 返回绑定更窄查询形状预算的不可变调用视图。 */
    public ReactiveFormClient withQueryShapeLimits(QueryShapeLimits limits) {
        return configured(configuration.withQueryShapeLimits(
                Objects.requireNonNull(limits, "query shape limits must not be null")));
    }

    /** Operator 的显式查询治理视图；一次配置同时保留策略、预算与启用状态。 */
    @InternalApi
    public ReactiveFormClient withQueryGovernance(FieldUsePolicy policy, QueryShapeLimits limits) {
        return configured(configuration.withQueryGovernance(policy, limits));
    }

    /** 不执行 SQL、不获取连接，返回与执行路径相同的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(QuerySpec spec) {
        return operations.previewFieldUse(spec);
    }

    /** JOIN 预览与执行复用同一中央审批函数，且不订阅执行器。 */
    public FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) {
        return operations.previewFieldUse(spec);
    }

    /** 不执行 SQL、不获取连接，返回聚合执行将使用的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(AggregateSpec spec) {
        return operations.previewFieldUse(spec);
    }

    /** Repository 和容器集成读取批量默认策略时使用。 */
    @InternalApi
    public BatchWriteOptions defaultBatchWriteOptions() {
        return configuration.batchOptions();
    }

    /** 为当前客户端绑定实例级实体映射缓存。 */
    @InternalApi
    public ReactiveFormClient withEntityModelRegistry(EntityModelRegistry registry) {
        return configured(configuration.withEntityModels(
                Objects.requireNonNull(registry, "entity model registry must not be null")));
    }

    /** @return 当前客户端使用的实体映射注册表。 */
    @InternalApi
    public EntityModelRegistry entityModels() {
        return configuration.entityModels();
    }

    /** 实体默认投影复用客户端已冻结的字段范围；行级 Scope 仍由查询计划统一合并。 */
    @InternalApi
    public FieldScope defaultFieldScope() {
        return configuration.dataScope().fields();
    }

    /** 为实体提供复用当前 Scope、执行保护和映射缓存的 Lambda DML 入口。 */
    public <T> EntityDmlOperator<T> entity(Class<T> type) {
        return EntityDmlOperator.create(this, renderer.conditionRenderer(), type);
    }

    /** Repository 内部创建 Lambda DML 状态时复用当前已装配的条件渲染器。 */
    @InternalApi
    public SqlRenderer entityRenderer() {
        return renderer.conditionRenderer();
    }

    /** 执行不可变查询规格并返回紧凑动态行流。 */
    public Flux<DynamicRow> select(QuerySpec spec) {
        return operations().selectSpec(spec);
    }

    /** DML operator 复用完整 Form 计划、保护改写和结果解码，但不复制整套客户端配置。 */
    @InternalApi
    public Flux<DynamicRow> selectGoverned(QuerySpec spec,
                                           FieldUsePolicy policy,
                                           QueryShapeLimits limits) {
        return operations().selectSpecGoverned(spec, policy, limits);
    }

    /** 在订阅时执行带显式 SQL 锁语法的受控读取，不探测或改变调用方事务。 */
    public Flux<DynamicRow> lockingRead(LockingReadSpec spec) {
        return operations().lockingReadSpec(Objects.requireNonNull(
                spec, "locking read spec must not be null"));
    }

    /** 使用 JDBC/R2DBC 共用的计划和布局执行类型化聚合。 */
    public Flux<AggregateRow> aggregate(AggregateSpec spec) {
        return operations.aggregate(spec);
    }

    /** 执行查询规格并映射实体。 */
    public <T> Flux<T> select(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return FormResultMappingSupport.mapRows(select(spec), mapper);
    }

    /** 执行一基页码分页。 */
    public Mono<PageResult<DynamicRow>> page(QuerySpec spec, PageQuery page) {
        return operations().pageSpec(spec, page);
    }

    /** 执行页码分页并映射实体。 */
    public <T> Mono<PageResult<T>> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return FormResultMappingSupport.mapPage(page(spec, page),
                                                results.rowMapper(type, "page result type must not be null"));
    }

    /** 执行稳定游标分页，不额外执行 count SQL。 */
    public Mono<CursorPageResult<DynamicRow>> cursorPage(QuerySpec spec, CursorPageQuery page) {
        return operations().cursorPageSpec(spec, page);
    }

    /** 执行游标分页并映射实体。 */
    public <T> Mono<CursorPageResult<T>> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        return FormResultMappingSupport.mapCursorPage(cursorPage(spec, page),
                                                      results.rowMapper(type, "cursor page result type must not be null"));
    }

    /** 执行插入规格。 */
    public Mono<Long> insert(WriteSpec spec) {
        return operations().insertSpec(spec);
    }

    /** 执行轻量多表查询并返回显式投影组成的紧凑动态行。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec) {
        return operations().selectJoin(spec, null);
    }

    /** 使用本次显式执行保护执行轻量多表查询。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return operations().selectJoin(spec, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** 使用 JOIN AST 的同一 Scope/条件分别执行 count 与页数据查询。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec, PageQuery page) {
        return operations().pageJoin(spec, page, null);
    }

    /** 使用本次执行保护完成 JOIN 页码分页。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec,
                                                 PageQuery page,
                                                 SqlExecutionOptions options) {
        return operations().pageJoin(spec, page, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** Repository 的数据库生成主键回填路径；不会改变动态表单 insert 的简洁返回值。 */
    @InternalApi
    public Mono<SqlWriteResult> insertReturningKeys(WriteSpec spec) {
        return operations().insertReturningKeysSpec(spec);
    }

    /** 执行更新规格。 */
    public Mono<Long> update(WriteSpec spec) {
        return operations().updateSpec(spec);
    }

    /** 执行逻辑删除优先的删除规格。 */
    public Mono<Long> delete(WriteSpec spec) {
        return operations().deleteSpec(spec);
    }

    /** 执行明确的物理删除。 */
    public Mono<Long> physicalDelete(WriteSpec spec) {
        return operations().physicalDeleteSpec(spec);
    }

    /** 执行流式 insert、upsert 或逐行乐观更新批量规格。 */
    public Mono<BatchExecutionEvidence> writeBatch(BatchSpec spec) {
        return writeBatch(spec, null);
    }

    /** 执行锁定读取并复用当前实体映射。 */
    public <T> Flux<T> lockingRead(LockingReadSpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(
                type, "locking read result type must not be null");
        return FormResultMappingSupport.mapRows(lockingRead(spec), mapper);
    }

    /** 执行 nullable、复合排序的稳定 keyset 分页，不执行 count SQL。 */
    public Mono<KeysetPageResult<DynamicRow>> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return operations().keysetPageSpec(spec, page);
    }

    /** 执行 keyset 分页并在隐藏游标列剥离后映射实体。 */
    public <T> Mono<KeysetPageResult<T>> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        return FormResultMappingSupport.mapKeysetPage(
                keysetPage(spec, page),
                results.rowMapper(type, "keyset page result type must not be null"));
    }

    /** 在一条查询中组合稳定 keyset 与受控 SQL 锁，不执行 count。 */
    public Mono<KeysetPageResult<DynamicRow>> lockingRead(
            LockingReadSpec spec,
            KeysetPageQuery page) {
        return operations().lockingReadSpec(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(page, "keyset page query must not be null"));
    }

    /** 锁定 keyset 的类型化结果入口。 */
    public <T> Mono<KeysetPageResult<T>> lockingRead(
            LockingReadSpec spec,
            KeysetPageQuery page,
            Class<T> type) {
        return FormResultMappingSupport.mapKeysetPage(
                lockingRead(spec, page),
                results.rowMapper(type, "locking keyset result type must not be null"));
    }

    /** 执行批量并按绝对输入位置组合内部行完成回调。 */
    @com.flying.orm.rdb.internal.InternalApi
    public Mono<BatchExecutionEvidence> writeBatch(
            BatchSpec spec, java.util.function.LongFunction<? extends Publisher<Void>> rowCompleted) {
        return operations().writeBatchSpec(spec, rowCompleted);
    }

    public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchSpec spec) {
        return writeBatch(spec);
    }


    /** 同包同步门面和实体入口复用这条已经装配好的内部操作链。 */
    ReactiveFormOperations operations() {
        return operations;
    }

    private ReactiveFormClient configured(FormConfiguration configured) {
        return new ReactiveFormClient(executor, configured);
    }

    private static FormConfiguration defaults(FormDataSqlRenderer renderer) {
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        return new FormConfiguration(
                safeRenderer, StructuredConditionResolver.defaults(safeRenderer.valueCodecs()), DataScope.none(),
                SqlExecutionOptions.safeDefaults(), BatchWriteOptions.defaults(),
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }

}
