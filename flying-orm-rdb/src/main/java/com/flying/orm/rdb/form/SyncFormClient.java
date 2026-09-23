package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
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
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private SyncFormClient(SyncSqlExecutor sqlExecutor,
                           SyncBatchExecutor batchExecutor,
                           FormConfiguration configuration,
                           SqlRenderer entityRenderer) {
        this.sqlExecutor = Objects.requireNonNull(sqlExecutor, "sync sql executor must not be null");
        this.batchExecutor = Objects.requireNonNull(batchExecutor, "sync batch executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "sync form configuration must not be null");
        this.operations = new SyncFormOperations(this.sqlExecutor, configuration);
        this.batches = new NativeSyncFormBatchOperations(
                this.batchExecutor, configuration.renderer(), configuration.resolver(), configuration.dataScope(),
                configuration.batchOptions(), configuration.fieldUsePolicy());
        this.entityRenderer = entityRenderer;
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
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(
                renderer, "form data sql renderer must not be null");
        FormConfiguration configuration = new FormConfiguration(
                safeRenderer,
                StructuredConditionResolvers.defaults(safeRenderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                BatchWriteOptions.defaults(),
                EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        return jdbc(sqlExecutor, batchExecutor, configuration);
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

    @InternalApi
    public BatchWriteOptions defaultBatchWriteOptions() { return configuration.batchOptions(); }

    @InternalApi
    public EntityModelRegistry entityModels() { return configuration.entityModels(); }

    /** 实体默认投影复用客户端已冻结的字段范围；行级 Scope 仍由查询计划统一合并。 */
    @InternalApi
    public FieldScope defaultFieldScope() { return configuration.dataScope().fields(); }

    /** 原生 JDBC 实体 Lambda 入口，表名、字段和主键都来自统一实体元数据。 */
    public <T> SyncEntityDmlOperator<T> entity(Class<T> type) {
        return SyncEntityDmlOperator.create(this, entityRenderer, type);
    }

    /** Repository 内部创建 Lambda DML 状态时复用当前已装配的条件渲染器。 */
    @InternalApi
    public SqlRenderer entityRenderer() {
        return entityRenderer;
    }

    public List<DynamicRow> select(QuerySpec spec) { return operations.select(spec); }
    /** DML operator 复用完整 Form 计划、保护改写和结果解码，但不重建客户端。 */
    @InternalApi
    public List<DynamicRow> selectGoverned(QuerySpec spec,
                                           FieldUsePolicy policy,
                                           QueryShapeLimits limits) {
        return operations.selectGoverned(spec, policy, limits);
    }
    /** 执行带显式 SQL 锁语法的受控读取，不探测或改变调用方事务。 */
    public List<DynamicRow> lockingRead(LockingReadSpec spec) {
        return operations.lockingRead(Objects.requireNonNull(
                spec, "locking read spec must not be null"));
    }
    /** 执行锁定读取并复用当前实体映射。 */
    public <T> List<T> lockingRead(LockingReadSpec spec, Class<T> type) {
        return operations.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(type, "locking read result type must not be null"));
    }
    /** 不执行 SQL、不获取连接，返回与执行路径相同的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(QuerySpec spec) { return operations.previewFieldUse(spec); }
    /** JOIN 预览与执行复用同一中央审批函数，且不访问执行器。 */
    public FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) { return operations.previewFieldUse(spec); }
    /** 不执行 SQL、不获取连接，返回聚合执行将使用的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(AggregateSpec spec) {
        return operations.previewFieldUse(configuration, spec);
    }
    /** 使用 JDBC/R2DBC 共用的计划和布局执行类型化聚合。 */
    public List<AggregateRow> aggregate(AggregateSpec spec) {
        return operations.aggregate(configuration, spec);
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
    public <T> List<T> select(QuerySpec spec, Class<T> type) { return operations.select(spec, type); }

    /** Operator 的逐行映射终端；rowLimit 为 0 读取全部，正数限制最终消费行数。 */
    @InternalApi
    public <T> List<T> selectMapped(QuerySpec spec, RowMapper<T> mapper, int rowLimit) {
        if (rowLimit < 0) { throw new IllegalArgumentException("row limit must not be negative"); }
        return operations.selectMapped(Objects.requireNonNull(spec, "query spec must not be null"),
                Objects.requireNonNull(mapper, "row mapper must not be null"), rowLimit);
    }
    /** 实体 Lambda 的内部零或一行终端；JDBC 只读取判定基数所需的两行。 */
    @InternalApi
    public <T> T selectOne(QuerySpec spec, Class<T> type) { return operations.selectOne(spec, type); }
    public PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) { return operations.page(spec, page); }
    public <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return operations.page(spec, page, type);
    }
    public CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page) {
        return operations.cursorPage(spec, page);
    }
    public <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        return operations.cursorPage(spec, page, type);
    }
    /** 执行 nullable、复合排序的稳定 keyset 分页，不执行 count SQL。 */
    public KeysetPageResult<DynamicRow> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return operations.keysetPage(spec, page);
    }
    /** 执行 keyset 分页并在隐藏游标列剥离后映射实体。 */
    public <T> KeysetPageResult<T> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        return operations.keysetPage(spec, page, type);
    }
    /** 在一条查询中组合稳定 keyset 与受控 SQL 锁，不额外执行 count。 */
    public KeysetPageResult<DynamicRow> lockingRead(
            LockingReadSpec spec, KeysetPageQuery page) {
        return operations.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(page, "keyset page query must not be null"));
    }
    /** 锁定 keyset 的类型化结果入口。 */
    public <T> KeysetPageResult<T> lockingRead(
            LockingReadSpec spec, KeysetPageQuery page, Class<T> type) {
        return operations.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(page, "keyset page query must not be null"),
                Objects.requireNonNull(type, "locking keyset result type must not be null"));
    }
    public long insert(WriteSpec spec) { return operations.insert(spec); }

    /** Repository 的数据库生成主键回填路径；普通动态表单 insert 继续只返回影响行数。 */
    @InternalApi
    public SqlWriteResult insertReturningKeys(WriteSpec spec) {
        return operations.insertReturningKeys(spec);
    }
    public long update(WriteSpec spec) { return operations.update(spec); }
    public long delete(WriteSpec spec) { return operations.delete(spec); }
    public long physicalDelete(WriteSpec spec) { return operations.physicalDelete(spec); }
    public BatchExecutionEvidence writeBatch(BatchSpec spec) { return writeBatch(spec, null); }

    @com.flying.orm.rdb.internal.InternalApi
    public BatchExecutionEvidence writeBatch(BatchSpec spec, java.util.function.LongConsumer rowCompleted) {
        return batches.writeBatch(spec, rowCompleted);
    }
    public BatchExecutionEvidence writeBatchEvidence(BatchSpec spec) {
        return writeBatch(spec);
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
        return new SyncFormClient(sqlExecutor, batchExecutor, configured, entityRenderer);
    }

    List<DynamicRow> select(DynamicForm form, ConditionGroup where) {
        return select(QuerySpec.of(form, where));
    }

    List<DynamicRow> select(DynamicForm form, StructuredConditionInput input) {
        return select(QuerySpec.structured(form, input));
    }

    List<DynamicRow> select(DynamicForm form, ConditionGroup where, SqlExecutionOptions options) {
        return select(QuerySpec.of(form, where).withExecutionOptions(options));
    }

    PageResult<DynamicRow> page(DynamicForm form,
                                ConditionGroup where,
                                PageQuery page,
                                SqlExecutionOptions options) {
        return page(QuerySpec.of(form, where).withExecutionOptions(options), page);
    }

    long update(DynamicForm form,
                Map<String, Object> values,
                ConditionGroup where,
                SqlExecutionOptions options) {
        return update(WriteSpec.update(form, values, where).withExecutionOptions(options));
    }

    long delete(DynamicForm form, ConditionGroup where, SqlExecutionOptions options) {
        return delete(WriteSpec.delete(form, where).withExecutionOptions(options));
    }

    BatchExecutionEvidence insertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return insertBatch(form, rows, configuration.batchOptions());
    }

    BatchExecutionEvidence insertBatch(DynamicForm form,
                                 List<Map<String, Object>> rows,
                                 BatchWriteOptions options) {
        // 同步 JDBC 不能占用 Reactor 事件循环，必须在读取用户集合前就拒绝。
        SyncBlockingGuard.rejectNonBlockingThread();
        // 这是同步入口，Publisher 会在方法返回前消费完集合；不复制整份引用数组，避免大批量峰值内存翻倍。
        List<Map<String, Object>> safeRows = Objects.requireNonNull(rows, "batch rows must not be null");
        safeRows.forEach(row -> Objects.requireNonNull(row, "batch row must not be null"));
        return writeBatch(BatchSpec.insert(form, BatchPublishers.fromIterable(safeRows)).withOptions(options));
    }
}
