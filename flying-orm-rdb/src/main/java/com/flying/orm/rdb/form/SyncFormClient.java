package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * 动态表单同步门面。
 *
 * <p>同步入口直接装配原生 JDBC 执行器。查询、写入和批量仍由各自的内部所有者完成，客户端只负责
 * 对外门面和不可变配置派生，不再经过只有一个实现的运行时转发层。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncFormClient {

    private final SyncSqlExecutor sqlExecutor;
    private final SyncBatchExecutor batchExecutor;
    private final FormConfiguration configuration;
    private final SyncFormOperations operations;
    private final NativeSyncFormBatchOperations batches;
    private final SqlRenderer entityRenderer;
    private final UnaryOperator<QuerySpec> queryPreparation;

    private SyncFormClient(SyncSqlExecutor sqlExecutor,
                           SyncBatchExecutor batchExecutor,
                           FormConfiguration configuration,
                           SqlRenderer entityRenderer) {
        this(sqlExecutor, batchExecutor, configuration, entityRenderer, null);
    }

    private SyncFormClient(SyncSqlExecutor sqlExecutor, SyncBatchExecutor batchExecutor,
                           FormConfiguration configuration, SqlRenderer entityRenderer,
                           UnaryOperator<QuerySpec> queryPreparation) {
        this.sqlExecutor = Objects.requireNonNull(sqlExecutor, "sync sql executor must not be null");
        this.batchExecutor = Objects.requireNonNull(batchExecutor, "sync batch executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "sync form configuration must not be null");
        this.operations = new SyncFormOperations(this.sqlExecutor, configuration);
        this.batches = new NativeSyncFormBatchOperations(
                this.batchExecutor, configuration.renderer(), operations.scopes,
                configuration.batchOptions(), configuration.fieldUsePolicy());
        this.entityRenderer = entityRenderer;
        this.queryPreparation = queryPreparation;
    }

    /**
     * 追加 QuerySpec 查询准备器，每项必须返回非 null，执行前按注册顺序调用；分页只准备一次。
     * 不处理 JOIN、聚合、原生 SQL、写入或预览，不是全局权限拦截器；回调必须并发安全。
     */
    public SyncFormClient withQueryPreparation(UnaryOperator<QuerySpec> preparation) {
        UnaryOperator<QuerySpec> next = Objects.requireNonNull(preparation, "query preparation must not be null");
        UnaryOperator<QuerySpec> previous = queryPreparation;
        return new SyncFormClient(sqlExecutor, batchExecutor, configuration, entityRenderer,
                previous == null ? next : spec -> next.apply(Objects.requireNonNull(
                        previous.apply(spec), "query preparation must return a spec")));
    }

    /**
     * 从原生同步执行器创建 JDBC 表单客户端。默认安全策略与响应式客户端一致，后续可以通过不可变
     * {@code with...} 方法继续收紧 Scope、执行保护和批量策略。
     *
     * @param sqlExecutor 原生 JDBC 单条 SQL 执行器
     * @param batchExecutor 原生 JDBC 有界批量执行器
     * @param renderer 已装配同一方言和 codec 的表单渲染器
     * @return 可并发共享的同步表单客户端
     */
    public static SyncFormClient create(SyncSqlExecutor sqlExecutor,
                                        SyncBatchExecutor batchExecutor,
                                        FormDataSqlRenderer renderer) {
        return jdbc(sqlExecutor, batchExecutor, new FormConfiguration(renderer));
    }

    /** 统一客户端直接注入最终配置，避免逐项派生时重复创建操作组件。 */
    @InternalApi
    public static SyncFormClient create(SyncSqlExecutor sqlExecutor, SyncBatchExecutor batchExecutor,
                                        FormDataSqlRenderer renderer, StructuredConditionResolver resolver,
                                        EntityModelRegistry models, SqlExecutionOptions options) {
        return jdbc(sqlExecutor, batchExecutor, new FormConfiguration(renderer, resolver, options, models));
    }

    /** 包内运行时装配入口；对外使用 {@link #create(SyncSqlExecutor, SyncBatchExecutor, FormDataSqlRenderer)}。 */
    static SyncFormClient jdbc(SyncSqlExecutor sqlExecutor,
                               SyncBatchExecutor batchExecutor,
                               FormConfiguration configuration) {
        FormConfiguration safeConfiguration = Objects.requireNonNull(
                configuration, "sync form configuration must not be null");
        return new SyncFormClient(sqlExecutor, batchExecutor, safeConfiguration,
                                  safeConfiguration.renderer().conditionRenderer());
    }

    /**
     * 返回当前客户端的默认批量策略；显式请求策略仍可覆盖它。
     *
     * @return 当前不可变批量策略
     */
    public BatchWriteOptions defaultBatchWriteOptions() { return configuration.batchOptions(); }

    /**
     * 返回当前客户端使用的实体映射注册表，供属性名条件、局部赋值和结果映射复用。
     * 注册表与客户端共享，生命周期由创建它的装配方管理。
     *
     * @return 当前客户端的实体映射注册表
     */
    public EntityModelRegistry entityModels() { return configuration.entityModels(); }

    /**
     * 返回当前客户端的默认字段范围，供上层封装复用默认投影规则。
     * 本方法只返回字段范围，不代表完整行级 Scope；行级范围仍由执行链统一合并。
     *
     * @return 当前不可变字段范围
     */
    public FieldScope defaultFieldScope() { return configuration.dataScope().fields(); }

    /** 原生 JDBC 实体 Lambda 入口，表名、字段和主键都来自统一实体元数据。 */
    public <T> SyncEntityDmlOperator<T> entity(Class<T> type) {
        return SyncEntityDmlOperator.create(this, entityRenderer, type);
    }

    /**
     * 返回当前实体执行链使用的条件渲染器，供上层复用已注册的运算符和渲染规则。
     * 使用 {@code entityModels().condition(..., entityRenderer().terms())} 可保持自定义条件语义一致。
     * 渲染器只负责 SQL 构建，不执行查询，也不替代表单执行链的 Scope 和字段治理。
     *
     * @return 当前条件渲染器
     */
    public SqlRenderer entityRenderer() {
        return entityRenderer;
    }

    public List<DynamicRow> select(QuerySpec spec) { return operations.select(prepare(spec)); }

    /** 只读取首条逻辑结果，不查询总数；未排序时由数据库决定首行，空查询返回 null。 */
    public DynamicRow first(QuerySpec spec) { return operations.selectFirst(prepare(spec), row -> row); }

    /** 统计行数；存在分组时统计组数。忽略结果投影、排序和行锁，保留条件、Scope 与字段治理。 */
    public long count(QuerySpec spec) { return operations.count(prepare(spec)); }

    /** 按既有实体映射规则读取首条结果；空查询返回 null。 */
    public <T> T first(QuerySpec spec, Class<T> type) { return operations.selectFirst(prepare(spec), type); }
    /** DML operator 复用完整 Form 计划、保护改写和结果解码，但不重建客户端。 */
    @InternalApi
    public List<DynamicRow> selectGoverned(QuerySpec spec,
                                           FieldUsePolicy policy,
                                           QueryShapeLimits limits) {
        return operations.selectGoverned(prepare(spec), policy, limits);
    }
    /** 执行带显式 SQL 锁语法的受控读取，不探测或改变调用方事务。 */
    public List<DynamicRow> lockingRead(QuerySpec spec) {
        return operations.lockingRead(prepare(spec));
    }
    /** 执行锁定读取并复用当前实体映射。 */
    public <T> List<T> lockingRead(QuerySpec spec, Class<T> type) {
        return operations.lockingRead(
                prepare(spec),
                Objects.requireNonNull(type, "locking read result type must not be null"));
    }
    /** 不执行 SQL、不获取连接，返回与执行路径相同的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(QuerySpec spec) { return operations.previewFieldUse(spec); }
    /** JOIN 预览与执行复用同一中央审批函数，且不访问执行器。 */
    public FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) { return operations.previewFieldUse(spec); }
    /** 不执行 SQL、不获取连接，返回聚合执行将使用的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(AggregateSpec spec) {
        return operations.previewFieldUse(spec);
    }
    /** 使用 JDBC/R2DBC 共用的计划和布局执行类型化聚合。 */
    public List<AggregateRow> aggregate(AggregateSpec spec) {
        return operations.aggregate(spec);
    }

    /** 数据库端聚合分页；total 是 HAVING 后的分组数，不治理两次查询的事务隔离。 */
    public PageResult<AggregateRow> aggregatePage(AggregateSpec spec, PageQuery page) {
        return operations.aggregatePage(spec, page);
    }
    /** 执行轻量多表查询并使用同步客户端的默认执行保护。 */
    public List<DynamicRow> selectJoin(JoinQuerySpec spec) { return operations.selectJoin(spec, null); }
    /** 使用本次显式执行保护执行轻量多表查询。 */
    public List<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return operations.selectJoin(spec, Objects.requireNonNull(options, "join execution options must not be null"));
    }
    /** JOIN 链式算子的内部逐行映射终端；避免先物化完整 DynamicRow 列表。 */
    @InternalApi
    public <T> List<T> selectJoinMapped(JoinQuerySpec spec, RowMapper<T> mapper) {
        return operations.selectJoin(
                Objects.requireNonNull(spec, "join query spec must not be null"), null,
                Objects.requireNonNull(mapper, "join row mapper must not be null"));
    }
    /** 使用 JOIN AST 的同一 Scope/条件分别执行 count 与页数据查询。 */
    public PageResult<DynamicRow> pageJoin(JoinQuerySpec spec, PageQuery page) {
        return operations.pageJoin(spec, page, null);
    }
    /** 使用本次执行保护完成原生 JDBC JOIN 页码分页。 */
    public PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                           PageQuery page,
                                           SqlExecutionOptions options) {
        return operations.pageJoin(spec, page, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }
    public <T> List<T> select(QuerySpec spec, Class<T> type) { return operations.select(prepare(spec), type); }

    /** 逐行消费已解码、脱敏的结果。原生 JDBC 不收集列表；回调期间连接仍在使用，不要重入同一连接。 */
    public long forEach(QuerySpec spec, Consumer<? super DynamicRow> consumer) {
        return operations.forEach(prepare(spec), row -> row, consumer);
    }
    /** 逐行映射实体/DTO 并消费，保留 Scope、字段治理和受保护条件复核。 */
    public <T> long forEach(QuerySpec spec, Class<T> type, Consumer<? super T> consumer) {
        return operations.forEach(prepare(spec), type, consumer);
    }
    /** 自定义行映射同样在字段治理之后执行；返回成功消费的结果数。 */
    public <T> long forEach(QuerySpec spec, RowMapper<T> mapper, Consumer<? super T> consumer) {
        return operations.forEach(prepare(spec), mapper, consumer);
    }

    /** 逐行处理并允许正常停止；false 表示停止，计数包含当前行，沿用 forEach 的治理与资源契约。 */
    public long forEachWhile(QuerySpec spec, Predicate<? super DynamicRow> consumer) {
        return operations.forEachWhile(prepare(spec), row -> row, consumer);
    }
    /** 映射实体/DTO 后决定是否继续；false 不表示执行失败。 */
    public <T> long forEachWhile(QuerySpec spec, Class<T> type, Predicate<? super T> consumer) {
        return operations.forEachWhile(prepare(spec), type, consumer);
    }
    /** 自定义映射在解码、脱敏和字段治理之后执行；返回已处理行数，包含停止行。 */
    public <T> long forEachWhile(QuerySpec spec, RowMapper<T> mapper, Predicate<? super T> consumer) {
        return operations.forEachWhile(prepare(spec), mapper, consumer);
    }

    /** Operator 的逐行映射终端；rowLimit 为 0 读取全部，正数限制最终消费行数。 */
    @InternalApi
    public <T> List<T> selectMapped(QuerySpec spec, RowMapper<T> mapper, int rowLimit) {
        if (rowLimit < 0) { throw new IllegalArgumentException("row limit must not be negative"); }
        return operations.selectMapped(prepare(Objects.requireNonNull(spec, "query spec must not be null")),
                Objects.requireNonNull(mapper, "row mapper must not be null"), rowLimit);
    }
    /** 实体 Lambda 的内部零或一行终端；JDBC 只读取判定基数所需的两行。 */
    @InternalApi
    public <T> T selectOne(QuerySpec spec, Class<T> type) { return operations.selectOne(prepare(spec), type); }
    public PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) { return operations.page(prepare(spec), page); }
    public <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return operations.page(prepare(spec), page, type);
    }
    /** 执行 nullable、复合排序的稳定 keyset 分页，不执行 count SQL。 */
    public KeysetPageResult<DynamicRow> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return operations.keysetPage(prepare(spec), page);
    }
    /** 执行 keyset 分页并在隐藏游标列剥离后映射实体。 */
    public <T> KeysetPageResult<T> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        return operations.keysetPage(prepare(spec), page, type);
    }
    /** 在一条查询中组合稳定 keyset 与受控 SQL 锁，不额外执行 count。 */
    public KeysetPageResult<DynamicRow> lockingRead(
            QuerySpec spec, KeysetPageQuery page) {
        return operations.lockingRead(
                prepare(spec),
                Objects.requireNonNull(page, "keyset page query must not be null"));
    }
    /** 锁定 keyset 的类型化结果入口。 */
    public <T> KeysetPageResult<T> lockingRead(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        return operations.lockingRead(
                prepare(spec),
                Objects.requireNonNull(page, "keyset page query must not be null"),
                Objects.requireNonNull(type, "locking keyset result type must not be null"));
    }
    public long insert(WriteSpec spec) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.insert(spec) : interceptor.write(spec, operations::insert);
    }

    /** Repository 的数据库生成主键回填路径；普通动态表单 insert 继续只返回影响行数。 */
    @InternalApi
    public SqlWriteResult insertReturningKeys(WriteSpec spec) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.insertReturningKeys(spec)
                : interceptor.write(spec, operations::insertReturningKeys);
    }
    public long update(WriteSpec spec) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.update(spec) : interceptor.write(spec, operations::update);
    }
    public long delete(WriteSpec spec) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.delete(spec) : interceptor.write(spec, operations::delete);
    }
    public long physicalDelete(WriteSpec spec) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? operations.physicalDelete(spec) : interceptor.write(spec, operations::physicalDelete);
    }
    public BatchExecutionEvidence writeBatch(BatchSpec spec) { return writeBatch(spec, null); }

    @com.flying.orm.rdb.internal.InternalApi
    public BatchExecutionEvidence writeBatch(BatchSpec spec, java.util.function.LongConsumer rowCompleted) {
        SyncWriteInterceptor interceptor = writeInterceptor(spec.form());
        return interceptor == null ? batches.writeBatch(spec, rowCompleted)
                : interceptor.batch(spec, prepared -> batches.writeBatch(prepared, rowCompleted));
    }

    private static SyncWriteInterceptor writeInterceptor(DynamicForm form) {
        return form.features().features().isEmpty() ? null
                : (SyncWriteInterceptor) form.features().find(SyncWriteInterceptor.ID).orElse(null);
    }

    public SyncFormClient withStructuredConditionResolver(StructuredConditionResolver resolver) {
        return configured(configuration.withResolver(resolver));
    }

    public SyncFormClient withDefaultExecutionOptions(SqlExecutionOptions options) {
        return configured(configuration.withExecutionOptions(options));
    }

    public SyncFormClient withDefaultDataScope(DataScope scope) {
        return configured(configuration.withDataScope(configuration.dataScope().and(scope)));
    }

    /** 返回绑定字段用途策略的不可变调用视图。 */
    public SyncFormClient withFieldUsePolicy(FieldUsePolicy policy) {
        return configured(configuration.withFieldUsePolicy(
                Objects.requireNonNull(policy, "field use policy must not be null")));
    }

    /** 返回绑定更窄查询形状预算的不可变调用视图。 */
    public SyncFormClient withQueryShapeLimits(QueryShapeLimits limits) {
        return configured(configuration.withQueryShapeLimits(
                Objects.requireNonNull(limits, "query shape limits must not be null")));
    }

    /** Operator 的显式查询治理视图；一次配置同时保留策略、预算与启用状态。 */
    @InternalApi
    public SyncFormClient withQueryGovernance(FieldUsePolicy policy, QueryShapeLimits limits) {
        return configured(configuration.withQueryGovernance(policy, limits));
    }

    /** 设置没有显式 options 时采用的批量策略。 */
    public SyncFormClient withDefaultBatchWriteOptions(BatchWriteOptions options) {
        return configured(configuration.withBatchOptions(
                Objects.requireNonNull(options, "batch write options must not be null")));
    }

    /** 为当前同步客户端绑定实例级、有界的实体映射缓存。 */
    @InternalApi
    public SyncFormClient withEntityModelRegistry(EntityModelRegistry entityModels) {
        return configured(configuration.withEntityModels(
                Objects.requireNonNull(entityModels, "entity model registry must not be null")));
    }

    private SyncFormClient configured(FormConfiguration configured) {
        return new SyncFormClient(sqlExecutor, batchExecutor, configured, entityRenderer, queryPreparation);
    }

    private QuerySpec prepare(QuerySpec spec) {
        if (queryPreparation == null) return spec;
        SyncBlockingGuard.rejectNonBlockingThread();
        return Objects.requireNonNull(queryPreparation.apply(spec), "query preparation must return a spec");
    }

}
