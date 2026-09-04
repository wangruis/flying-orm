package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态表单同步门面。
 *
 * <p>V2 运行时直接使用原生 JDBC 同步执行器。这个类只负责同步表单的统一入口，查询、写入、批量和实体操作
 * 都下沉到运行时协作者；因此调用方不需要感知内部执行分工。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncFormClient {

    /** 同步客户端的默认超时时间，供同步边界校验使用。 */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    private final SyncFormRuntime runtime;
    private final Duration timeout;
    private final SqlRenderer entityRenderer;

    private SyncFormClient(SyncFormRuntime runtime,
                           Duration timeout,
                           SqlRenderer entityRenderer) {
        this.runtime = Objects.requireNonNull(runtime, "sync form runtime must not be null");
        this.timeout = SyncBlockingGuard.requirePositiveTimeout(timeout, "sync client timeout");
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
        SyncFormConfiguration configuration = new SyncFormConfiguration(
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
                               SyncFormConfiguration configuration) {
        return new SyncFormClient(new JdbcSyncFormRuntime(sqlExecutor, batchExecutor, configuration),
                                  DEFAULT_TIMEOUT,
                                  configuration.renderer().conditionRenderer());
    }

    public Duration timeout() { return timeout; }

    @InternalApi
    public BatchWriteOptions defaultBatchWriteOptions() { return runtime.defaultBatchWriteOptions(); }

    @InternalApi
    public EntityModelRegistry entityModels() { return runtime.entityModels(); }

    /** @return 当前线程正在参与的外部 JDBC 事务；没有外部事务时为空。 */
    @InternalApi
    public java.util.Optional<com.flying.orm.rdb.transaction.JdbcTransactionContext> currentTransaction() {
        return runtime.currentTransaction();
    }

    /** 原生 JDBC 实体 Lambda 入口，表名、字段和主键都来自统一实体元数据。 */
    public <T> SyncEntityDmlOperator<T> entity(Class<T> type) {
        return SyncEntityDmlOperator.create(this, entityRenderer, type);
    }

    /** Repository 内部创建 Lambda DML 状态时复用当前已装配的条件渲染器。 */
    @InternalApi
    public SqlRenderer entityRenderer() {
        return entityRenderer;
    }

    public List<DynamicRow> select(QuerySpec spec) { return runtime.select(spec); }
    /** DML operator 复用完整 Form 计划、保护改写和结果解码，但不重建客户端。 */
    @InternalApi
    public List<DynamicRow> selectGoverned(QuerySpec spec,
                                           FieldUsePolicy policy,
                                           QueryShapeLimits limits) {
        return runtime.selectGoverned(spec, policy, limits);
    }
    /** 在调用方已经开启的 JDBC 事务中执行受控锁定读取；本方法不结束事务。 */
    public List<DynamicRow> lockingRead(LockingReadSpec spec) {
        return runtime.lockingRead(Objects.requireNonNull(
                spec, "locking read spec must not be null"));
    }
    /** 执行锁定读取并复用当前实体映射。 */
    public <T> List<T> lockingRead(LockingReadSpec spec, Class<T> type) {
        return runtime.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(type, "locking read result type must not be null"));
    }
    /** 不执行 SQL、不获取连接，返回与执行路径相同的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(QuerySpec spec) { return runtime.previewFieldUse(spec); }
    /** JOIN 预览与执行复用同一中央审批函数，且不访问执行器。 */
    public FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) { return runtime.previewFieldUse(spec); }
    /** 不执行 SQL、不获取连接，返回聚合执行将使用的字段用途审批快照。 */
    public FieldUseSnapshot previewFieldUse(AggregateSpec spec) { return runtime.previewFieldUse(spec); }
    /** 使用 JDBC/R2DBC 共用的计划和布局执行类型化聚合。 */
    public List<AggregateRow> aggregate(AggregateSpec spec) { return runtime.aggregate(spec); }
    /** 执行轻量多表查询并使用同步客户端的默认执行保护。 */
    public List<DynamicRow> selectJoin(JoinQuerySpec spec) { return runtime.selectJoin(spec, null); }
    /** 使用本次显式执行保护执行轻量多表查询。 */
    public List<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return runtime.selectJoin(spec, Objects.requireNonNull(options, "join execution options must not be null"));
    }
    /** JOIN 链式算子的内部逐行映射终端；避免先物化完整 DynamicRow 列表。 */
    @InternalApi
    public <T> List<T> selectJoinMapped(JoinQuerySpec spec, RowMapper<T> mapper) {
        return runtime.selectJoin(
                Objects.requireNonNull(spec, "join query spec must not be null"), null,
                Objects.requireNonNull(mapper, "join row mapper must not be null"));
    }
    /** 使用 JOIN AST 的同一 Scope/条件分别执行 count 与页数据查询。 */
    public PageResult<DynamicRow> pageJoin(JoinQuerySpec spec, PageQuery page) {
        return runtime.pageJoin(spec, page, null);
    }
    /** 使用本次执行保护完成原生 JDBC JOIN 页码分页。 */
    public PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                           PageQuery page,
                                           SqlExecutionOptions options) {
        return runtime.pageJoin(spec, page, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }
    public <T> List<T> select(QuerySpec spec, Class<T> type) { return runtime.select(spec, type); }
    /** 实体 Lambda 的内部零或一行终端；JDBC 只读取判定基数所需的两行。 */
    @InternalApi
    public <T> T selectOne(QuerySpec spec, Class<T> type) { return runtime.selectOne(spec, type); }
    public PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) { return runtime.page(spec, page); }
    public <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return runtime.page(spec, page, type);
    }
    public CursorPageResult<DynamicRow> cursorPage(QuerySpec spec, CursorPageQuery page) {
        return runtime.cursorPage(spec, page);
    }
    public <T> CursorPageResult<T> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        return runtime.cursorPage(spec, page, type);
    }
    /** 执行 nullable、复合排序的稳定 keyset 分页，不执行 count SQL。 */
    public KeysetPageResult<DynamicRow> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return runtime.keysetPage(spec, page);
    }
    /** 执行 keyset 分页并在隐藏游标列剥离后映射实体。 */
    public <T> KeysetPageResult<T> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        return runtime.keysetPage(spec, page, type);
    }
    /** 在同一个外部事务中组合稳定 keyset 与受控锁，不额外执行 count。 */
    public KeysetPageResult<DynamicRow> lockingRead(
            LockingReadSpec spec, KeysetPageQuery page) {
        return runtime.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(page, "keyset page query must not be null"));
    }
    /** 锁定 keyset 的类型化结果入口。 */
    public <T> KeysetPageResult<T> lockingRead(
            LockingReadSpec spec, KeysetPageQuery page, Class<T> type) {
        return runtime.lockingRead(
                Objects.requireNonNull(spec, "locking read spec must not be null"),
                Objects.requireNonNull(page, "keyset page query must not be null"),
                Objects.requireNonNull(type, "locking keyset result type must not be null"));
    }
    public long insert(WriteSpec spec) { return runtime.insert(spec); }

    /** Repository 的数据库生成主键回填路径；普通动态表单 insert 继续只返回影响行数。 */
    @InternalApi
    public SqlWriteResult insertReturningKeys(WriteSpec spec) {
        return runtime.insertReturningKeys(spec);
    }
    public long update(WriteSpec spec) { return runtime.update(spec); }
    public long delete(WriteSpec spec) { return runtime.delete(spec); }
    public long physicalDelete(WriteSpec spec) { return runtime.physicalDelete(spec); }
    public BatchWriteResult writeBatch(BatchSpec spec) { return runtime.writeBatch(spec); }
    public BatchExecutionEvidence writeBatchEvidence(BatchSpec spec) {
        return runtime.writeBatchEvidence(spec);
    }
    public List<BatchChunkResult> writeBatchChunks(BatchSpec spec) { return runtime.writeBatchChunks(spec); }

    public SyncFormClient withStructuredConditionResolver(StructuredConditionResolver resolver) {
        return configured(runtime.withResolver(resolver));
    }

    public SyncFormClient withDefaultExecutionOptions(SqlExecutionOptions options) {
        return configured(runtime.withExecutionOptions(options));
    }

    public SyncFormClient withDefaultDataScope(DataScope scope) {
        return configured(runtime.withDataScope(scope));
    }

    /** 返回绑定字段用途策略的不可变调用视图。 */
    public SyncFormClient withFieldUsePolicy(FieldUsePolicy policy) {
        return configured(runtime.withFieldUsePolicy(
                Objects.requireNonNull(policy, "field use policy must not be null")));
    }

    /** 返回绑定更窄查询形状预算的不可变调用视图。 */
    public SyncFormClient withQueryShapeLimits(QueryShapeLimits limits) {
        return configured(runtime.withQueryShapeLimits(
                Objects.requireNonNull(limits, "query shape limits must not be null")));
    }

    /** 设置没有显式 options 时采用的批量策略。 */
    public SyncFormClient withDefaultBatchWriteOptions(BatchWriteOptions options) {
        return configured(runtime.withBatchOptions(
                Objects.requireNonNull(options, "batch write options must not be null")));
    }

    /** 为当前同步客户端绑定实例级、有界的实体映射缓存。 */
    @InternalApi
    public SyncFormClient withEntityModelRegistry(EntityModelRegistry entityModels) {
        return configured(runtime.withEntityModels(
                Objects.requireNonNull(entityModels, "entity model registry must not be null")));
    }

    private SyncFormClient configured(SyncFormRuntime configuredRuntime) {
        return new SyncFormClient(configuredRuntime, timeout, entityRenderer);
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

    BatchWriteResult insertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return insertBatch(form, rows, runtime.defaultBatchWriteOptions());
    }

    BatchWriteResult insertBatch(DynamicForm form,
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
