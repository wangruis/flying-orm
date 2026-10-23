package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
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
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * 动态表单响应式 CRUD 的稳定公开入口。
 *
 * <p>客户端本身只保存不可变配置和对外 API。查询、分页、写入、批量写入、Scope 合并、字段解码等细节
 * 由同包内部操作协作者完成，所以一个已经装配好的客户端可以安全复用给并发订阅。所有数据库 I/O 仍在
 * Reactor 订阅链内发生：这里不会调用 {@code block()} 或 {@code subscribe()}，也不会先收集整个输入流。</p>
 *
 * <p>同包内部的细粒度操作按职责放在 {@link ReactiveFormOperations} 管理的协作者中，同步和响应式执行
 * 共享 SQL 规划、Scope 与结果解码，不通过阻塞响应式链实现同步调用。业务代码优先使用
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
    private final Function<QuerySpec, ? extends Publisher<QuerySpec>> queryPreparation;

    private ReactiveFormClient(ReactiveSqlExecutor executor, FormConfiguration configuration) {
        this(executor, configuration, null);
    }

    private ReactiveFormClient(ReactiveSqlExecutor executor, FormConfiguration configuration,
                                 Function<QuerySpec, ? extends Publisher<QuerySpec>> queryPreparation) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "form configuration must not be null");
        this.operations = new ReactiveFormOperations(this.executor, configuration);
        this.renderer = configuration.renderer();
        this.results = operations.results;
        this.queryPreparation = queryPreparation;
    }

    /** 创建动态表单响应式客户端。 */
    public static ReactiveFormClient create(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer) {
        return new ReactiveFormClient(executor, new FormConfiguration(renderer));
    }

    /** 统一客户端直接注入最终配置，避免逐项派生时重复创建操作组件。 */
    @InternalApi
    public static ReactiveFormClient create(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer,
                                            StructuredConditionResolver resolver, EntityModelRegistry models,
                                            SqlExecutionOptions options) {
        return new ReactiveFormClient(executor, new FormConfiguration(renderer, resolver, options, models));
    }

    /**
     * 追加 QuerySpec 查询准备器，每次订阅按注册顺序执行并要求恰好返回一个规格。
     * 查询、计数、分页与锁定读取共用；分页只准备一次。异常或取消不会继续执行 SQL。
     * 不处理 JOIN、聚合、原生 SQL、写入或预览，不是全局权限拦截器；回调必须非阻塞且并发安全。
     */
    public ReactiveFormClient withQueryPreparation(Function<QuerySpec, ? extends Publisher<QuerySpec>> preparation) {
        Function<QuerySpec, ? extends Publisher<QuerySpec>> next = Objects.requireNonNull(preparation,
                "query preparation must not be null");
        Function<QuerySpec, ? extends Publisher<QuerySpec>> previous = queryPreparation;
        return new ReactiveFormClient(executor, configuration, previous == null ? next
                : spec -> Flux.from(previous.apply(spec)).single()
                        .flatMap(prepared -> Flux.from(next.apply(prepared)).single()));
    }

    /** 为前端结构化条件替换线程安全的解析器。 */
    public ReactiveFormClient withStructuredConditionResolver(StructuredConditionResolver resolver) {
        return configured(configuration.withResolver(resolver));
    }

    /** 设置没有显式 options 时使用的 SQL 执行保护。 */
    public ReactiveFormClient withDefaultExecutionOptions(SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return new ReactiveFormClient(
                executor.withDefaultExecutionOptions(safeOptions), configuration.withExecutionOptions(safeOptions), queryPreparation);
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

    /**
     * 返回当前客户端的默认批量策略；显式请求策略仍可覆盖它。
     *
     * @return 当前不可变批量策略
     */
    public BatchWriteOptions defaultBatchWriteOptions() {
        return configuration.batchOptions();
    }

    /** 为当前客户端绑定实例级实体映射缓存。 */
    @InternalApi
    public ReactiveFormClient withEntityModelRegistry(EntityModelRegistry registry) {
        return configured(configuration.withEntityModels(
                Objects.requireNonNull(registry, "entity model registry must not be null")));
    }

    /**
     * 返回当前客户端使用的实体映射注册表，供属性名条件、局部赋值和结果映射复用。
     * 注册表与客户端共享，生命周期由创建它的装配方管理。
     *
     * @return 当前客户端的实体映射注册表
     */
    public EntityModelRegistry entityModels() {
        return configuration.entityModels();
    }

    /**
     * 返回当前客户端的默认字段范围，供上层封装复用默认投影规则。
     * 本方法只返回字段范围，不代表完整行级 Scope；行级范围仍由执行链统一合并。
     *
     * @return 当前不可变字段范围
     */
    public FieldScope defaultFieldScope() {
        return configuration.dataScope().fields();
    }

    /** 为实体提供复用当前 Scope、执行保护和映射缓存的 Lambda DML 入口。 */
    public <T> EntityDmlOperator<T> entity(Class<T> type) {
        return EntityDmlOperator.create(this, renderer.conditionRenderer(), type);
    }

    /**
     * 返回当前实体执行链使用的条件渲染器，供上层复用已注册的运算符和渲染规则。
     * 使用 {@code entityModels().condition(..., entityRenderer().terms())} 可保持自定义条件语义一致。
     * 渲染器只负责 SQL 构建，不执行查询，也不替代表单执行链的 Scope 和字段治理。
     *
     * @return 当前条件渲染器
     */
    public SqlRenderer entityRenderer() {
        return renderer.conditionRenderer();
    }

    /** 执行不可变查询规格并返回紧凑动态行流。 */
    public Flux<DynamicRow> select(QuerySpec spec) {
        return canPlanDirectly(spec) ? operations.selectSpec(spec) : prepare(spec).flatMapMany(operations::selectSpec);
    }

    /** 只读取首条逻辑结果，不查询总数；未排序时由数据库决定首行。 */
    public Mono<DynamicRow> first(QuerySpec spec) {
        return canPlanDirectly(spec) ? operations.firstSpec(spec) : prepare(spec).flatMap(operations::firstSpec);
    }

    /** 统计行数；存在分组时统计组数。忽略结果投影、排序和行锁，保留条件、Scope 与字段治理。 */
    public Mono<Long> count(QuerySpec spec) {
        return canPlanDirectly(spec) ? operations.countSpec(spec) : prepare(spec).flatMap(operations::countSpec);
    }

    /** 按既有实体映射规则读取首条结果；空查询返回空 Mono。 */
    public <T> Mono<T> first(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return first(spec).map(mapper::map);
    }

    /** DML operator 复用完整 Form 计划、保护改写和结果解码，但不复制整套客户端配置。 */
    @InternalApi
    public Flux<DynamicRow> selectGoverned(QuerySpec spec,
                                           FieldUsePolicy policy,
                                           QueryShapeLimits limits) {
        return canPlanDirectly(spec) ? operations.selectSpecGoverned(spec, policy, limits)
                : prepare(spec).flatMapMany(prepared -> operations.selectSpecGoverned(prepared, policy, limits));
    }

    /** 在订阅时执行带显式 SQL 锁语法的受控读取，不探测或改变调用方事务。 */
    public Flux<DynamicRow> lockingRead(QuerySpec spec) {
        Objects.requireNonNull(spec, "locking read spec must not be null");
        return queryPreparation == null ? operations.lockingReadSpec(spec)
                : prepare(spec).flatMapMany(prepared -> operations.lockingReadSpec(
                        prepared));
    }

    /** 使用 JDBC/R2DBC 共用的计划和布局执行类型化聚合。 */
    public Flux<AggregateRow> aggregate(AggregateSpec spec) {
        return operations.aggregate(spec);
    }

    /** 数据库端聚合分页；total 是 HAVING 后的分组数，不治理两次查询的事务隔离。 */
    public Mono<PageResult<AggregateRow>> aggregatePage(AggregateSpec spec, PageQuery page) {
        return operations.aggregatePage(spec, page);
    }

    /** 执行查询规格并映射实体。 */
    public <T> Flux<T> select(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return select(spec).map(mapper::map);
    }

    /** 执行一基页码分页。 */
    public Mono<PageResult<DynamicRow>> page(QuerySpec spec, PageQuery page) {
        return page(spec, page, FormResultDecoder.IDENTITY);
    }

    /** 执行页码分页并映射实体。 */
    public <T> Mono<PageResult<T>> page(QuerySpec spec, PageQuery page, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "page result type must not be null");
        return page(spec, page, mapper);
    }

    private <T> Mono<PageResult<T>> page(QuerySpec spec, PageQuery page, RowMapper<T> mapper) {
        return canPlanDirectly(spec) ? operations.pageSpec(spec, page, mapper)
                : prepare(spec).flatMap(prepared -> operations.pageSpec(prepared, page, mapper));
    }

    /** 执行插入规格。 */
    public Mono<Long> insert(WriteSpec spec) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.insertSpec(spec)
                : Mono.defer(() -> interceptor.write(spec, operations::insertSpec));
    }

    /** 执行轻量多表查询并返回显式投影组成的紧凑动态行。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec) {
        return operations.selectJoin(spec, null);
    }

    /** 使用本次显式执行保护执行轻量多表查询。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return operations.selectJoin(spec, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** 使用 JOIN AST 的同一 Scope/条件分别执行 count 与页数据查询。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec, PageQuery page) {
        return operations.pageJoin(spec, page, null);
    }

    /** 使用本次执行保护完成 JOIN 页码分页。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec,
                                                 PageQuery page,
                                                 SqlExecutionOptions options) {
        return operations.pageJoin(spec, page, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** Repository 的数据库生成主键回填路径；不会改变动态表单 insert 的简洁返回值。 */
    @InternalApi
    public Mono<SqlWriteResult> insertReturningKeys(WriteSpec spec) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.insertReturningKeysSpec(spec)
                : Mono.defer(() -> interceptor.write(spec, operations::insertReturningKeysSpec));
    }

    /** 执行更新规格。 */
    public Mono<Long> update(WriteSpec spec) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.updateSpec(spec)
                : Mono.defer(() -> interceptor.write(spec, operations::updateSpec));
    }

    /** 执行逻辑删除优先的删除规格。 */
    public Mono<Long> delete(WriteSpec spec) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.deleteSpec(spec)
                : Mono.defer(() -> interceptor.write(spec, operations::deleteSpec));
    }

    /** 执行明确的物理删除。 */
    public Mono<Long> physicalDelete(WriteSpec spec) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.physicalDeleteSpec(spec)
                : Mono.defer(() -> interceptor.write(spec, operations::physicalDeleteSpec));
    }

    /** 执行流式 insert、upsert 或普通/乐观锁 update 批量规格。 */
    public Mono<BatchExecutionEvidence> writeBatch(BatchSpec spec) {
        return writeBatch(spec, null);
    }

    /** 执行锁定读取并复用当前实体映射。 */
    public <T> Flux<T> lockingRead(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(
                type, "locking read result type must not be null");
        return lockingRead(spec).map(mapper::map);
    }

    /** 执行 nullable、复合排序的稳定 keyset 分页，不执行 count SQL。 */
    public Mono<KeysetPageResult<DynamicRow>> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return keysetPage(spec, page, FormResultDecoder.IDENTITY);
    }

    /** 执行 keyset 分页并在隐藏游标列剥离后映射实体。 */
    public <T> Mono<KeysetPageResult<T>> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "keyset page result type must not be null");
        return keysetPage(spec, page, mapper);
    }

    private <T> Mono<KeysetPageResult<T>> keysetPage(QuerySpec spec, KeysetPageQuery page, RowMapper<T> mapper) {
        return canPlanDirectly(spec) ? operations.keysetPageSpec(spec, page, mapper)
                : prepare(spec).flatMap(prepared -> operations.keysetPageSpec(prepared, page, mapper));
    }

    /** 在一条查询中组合稳定 keyset 与受控 SQL 锁，不执行 count。 */
    public Mono<KeysetPageResult<DynamicRow>> lockingRead(
            QuerySpec spec,
            KeysetPageQuery page) {
        return lockingRead(spec, page, FormResultDecoder.IDENTITY);
    }

    private <T> Mono<KeysetPageResult<T>> lockingRead(QuerySpec spec, KeysetPageQuery page, RowMapper<T> mapper) {
        Objects.requireNonNull(spec, "locking read spec must not be null");
        Objects.requireNonNull(page, "keyset page query must not be null");
        return queryPreparation == null ? operations.lockingReadSpec(spec, page, mapper)
                : prepare(spec).flatMap(prepared -> operations.lockingReadSpec(
                        prepared, page, mapper));
    }

    /** 锁定 keyset 的类型化结果入口。 */
    public <T> Mono<KeysetPageResult<T>> lockingRead(
            QuerySpec spec,
            KeysetPageQuery page,
            Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "locking keyset result type must not be null");
        return lockingRead(spec, page, mapper);
    }

    /** 执行批量并按绝对输入位置组合内部行完成回调。 */
    @com.flying.orm.rdb.internal.InternalApi
    public Mono<BatchExecutionEvidence> writeBatch(
            BatchSpec spec, java.util.function.LongFunction<? extends Publisher<Void>> rowCompleted) {
        ReactiveWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.writeBatchSpec(spec, rowCompleted)
                : Mono.defer(() -> interceptor.batch(spec, prepared -> operations.writeBatchSpec(prepared, rowCompleted)));
    }

    private static ReactiveWriteInterceptor writeInterceptor(DynamicForm form) {
        return form.features().features().isEmpty() ? null
                : (ReactiveWriteInterceptor) form.features().find(ReactiveWriteInterceptor.ID).orElse(null);
    }

    private ReactiveFormClient configured(FormConfiguration configured) {
        return new ReactiveFormClient(executor, configured, queryPreparation);
    }

    private Mono<QuerySpec> prepare(QuerySpec spec) {
        return queryPreparation == null ? Mono.just(spec)
                : Mono.defer(() -> Flux.from(queryPreparation.apply(spec)).single());
    }

    private boolean canPlanDirectly(QuerySpec spec) {
        return queryPreparation == null && spec.form().relations().isEmpty();
    }

}
