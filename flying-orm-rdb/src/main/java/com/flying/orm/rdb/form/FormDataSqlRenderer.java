package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectFeature;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * 将 {@link DynamicForm} 数据操作渲染成参数化 CRUD SQL 的稳定门面。
 *
 * <p>门面保留动态表单对外的全部 DML 入口，查询、批量写入和共同的 SQL 规则交给构造期创建的内部协作对象。
 * 因此单条、批量和查询始终使用同一套字段校验、条件 AST、标识符处理、codec 与 SQL 计划缓存，
 * 也不会在每次调用时新建渲染器或分裂缓存。</p>
 *
 * <p>本类不获取连接，也不处理事务。调用失败会发生在 SQL 交给执行器之前，生成成功的请求保持参数顺序不变。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
public final class FormDataSqlRenderer {

    private final FormSqlRenderSupport support;
    private final PaginationDialect paginationDialect;
    private final UpsertDialect upsertDialect;
    private final LockingReadDialect lockingReadDialect;
    private final FormQuerySqlRenderer queryRenderer;
    private final JoinQuerySqlRenderer joinRenderer;
    final FormBatchSqlRenderer batchRenderer;
    private final FormWriteSqlRenderer writeRenderer;
    private final ProtectedFieldRuntime protectedFields;
    private final FormProtectionSqlSupport protection;
    private final FormFieldDecodingPlanCache decodingPlans;

    private FormDataSqlRenderer(FormSqlRenderSupport support,
                                PaginationDialect paginationDialect,
                                UpsertDialect upsertDialect,
                                LockingReadDialect lockingReadDialect,
                                ProtectedFieldRuntime protectedFields,
                                FormFieldDecodingPlanCache decodingPlans) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.paginationDialect = Objects.requireNonNull(paginationDialect, "pagination dialect must not be null");
        this.upsertDialect = Objects.requireNonNull(upsertDialect, "upsert dialect must not be null");
        this.lockingReadDialect = Objects.requireNonNull(
                lockingReadDialect, "locking read dialect must not be null");
        this.queryRenderer = new FormQuerySqlRenderer(this.support, this.paginationDialect);
        this.joinRenderer = new JoinQuerySqlRenderer(this.support, this.paginationDialect);
        this.batchRenderer = new FormBatchSqlRenderer(this.support, this.upsertDialect);
        this.writeRenderer = new FormWriteSqlRenderer(this.support);
        this.protectedFields = Objects.requireNonNull(
                protectedFields, "protected field runtime must not be null");
        this.decodingPlans = Objects.requireNonNull(
                decodingPlans, "form field decoding plan cache must not be null");
        this.protection = new FormProtectionSqlSupport(
                this.support, this.queryRenderer, this.writeRenderer, this.protectedFields,
                this.paginationDialect);
    }

    /**
     * 返回与当前 DML 门面共享方言引用和 term 注册表的条件渲染器。
     *
     * @return 不可变的条件渲染器
     */
    @InternalApi
    public SqlRenderer conditionRenderer() {
        return support.conditionRenderer;
    }

    /** 受治理规划读取装配时冻结的能力事实，不在请求热路径按方言名称重新推断。 */
    DialectCapabilities dialectCapabilities() {
        return support.dialectCapabilities;
    }

    /** 聚合内部桥接只在装配后读取一次，不做请求期方言探测。 */
    boolean sqlServerDialect() {
        return "sqlserver".equals(support.dialectName);
    }

    /** 聚合内部桥接复用普通查询已经确立的稳定时间排序约束。 */
    void requireStableOffsetTimeOrdering(DynamicField field) {
        support.requireStableOffsetTimeOrdering(field);
    }

    /**
     * 使用当前动态表单和方言规则渲染内部扩展查询的条件片段。
     *
     * @param form 动态表单
     * @param where 条件 AST
     * @return 已完成字段校验和值转换的参数化条件片段
     */
    @InternalApi
    public SqlFragment renderCondition(DynamicForm form, ConditionGroup where) {
        FormSqlRenderSupport.ConditionSql condition = support.condition(form, where);
        return new SqlFragment(condition.sql(), condition.parameters());
    }

    /** 聚合 planner 的包内桥接：值按声明来源规范化，别名 SQL 由受控表达式映射。 */
    SqlFragment renderCondition(DynamicForm form,
                                ConditionGroup where,
                                UnaryOperator<String> fieldIdentifierRenderer,
                                UnaryOperator<String> correlatedFieldRenderer,
                                UnaryOperator<String> outerQualifierRenderer,
                                Function<String, DynamicField> valueFieldResolver) {
        FormSqlRenderSupport.ConditionSql condition = support.condition(
                form, where, fieldIdentifierRenderer, correlatedFieldRenderer, outerQualifierRenderer,
                valueFieldResolver);
        return new SqlFragment(condition.sql(), condition.parameters());
    }

    String relationIdentifier(DynamicForm form) {
        return support.identifier(form);
    }

    /**
     * 创建指定关系型数据库方言的动态表单 SQL 渲染器。
     *
     * @param conditionRenderer 条件渲染器
     * @param dialect 数据库方言
     * @return 可并发复用的 SQL 渲染门面
     */
    public static FormDataSqlRenderer create(SqlRenderer conditionRenderer, RdbDialect dialect) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        UnaryOperator<String> identifiers = safeDialect.schema()::identifier;
        SqlRenderer dialectConditions = Objects.requireNonNull(conditionRenderer, "sql renderer must not be null")
                                                 .withIdentifierRenderer(identifiers);
        FormSqlRenderSupport support = new FormSqlRenderSupport(dialectConditions,
                                                                safeDialect.json(),
                                                                safeDialect.name(),
                                                                safeDialect.supports(DialectFeature.NATIVE_BOOLEAN),
                                                                identifiers,
                                                                StructuralPlanCaches.create(
                                                                        OrmCachePolicy.safeDefaults()),
                                                                Map.of(),
                                                                safeDialect.capabilities(),
                                                                safeDialect.schema()::relationIdentifier);
        return new FormDataSqlRenderer(support, safeDialect.pagination(), safeDialect.upsert(),
                                       safeDialect.lockingReadDialect(),
                                       ProtectedFieldRuntime.withoutKeys(),
                                       FormFieldDecodingPlanCache.create(CacheRegionPolicy.sqlPlanDefaults()));
    }

    /**
     * 使用调用方管理的结构计划缓存创建等价门面，用于让元数据失效和 SQL 计划失效落在同一张缓存图中。
     *
     * @param caches 结构计划缓存
     * @return 使用指定缓存的新门面
     */
    @InternalApi
    public FormDataSqlRenderer withPlanCaches(StructuralPlanCaches caches) {
        return new FormDataSqlRenderer(support.withPlanCaches(caches), paginationDialect, upsertDialect,
                                       lockingReadDialect,
                                       protectedFields, decodingPlans);
    }

    /** 启动期挂接 descriptor 已解析的字段 codec；普通客户端继续复用共享空映射。 */
    @InternalApi
    public FormDataSqlRenderer withEntityFieldCodecs(
            Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings) {
        return new FormDataSqlRenderer(support.withCustomFieldCodecs(mappings),
                                       paginationDialect, upsertDialect, lockingReadDialect,
                                       protectedFields, decodingPlans.emptyCopy());
    }

    /** 为统一客户端装配字段保护运行时；业务代码使用 FlyingOrmClientBuilder 配置密钥环。 */
    @InternalApi
    public FormDataSqlRenderer withProtectedFields(ProtectedFieldRuntime runtime) {
        return new FormDataSqlRenderer(support, paginationDialect, upsertDialect, lockingReadDialect,
                                       Objects.requireNonNull(runtime,
                                                              "protected field runtime must not be null"), decodingPlans);
    }

    /** 使用客户端 SQL 计划区域的有界策略保存结果解码结构计划。 */
    @InternalApi
    public FormDataSqlRenderer withResultPlanCachePolicy(CacheRegionPolicy policy) {
        return new FormDataSqlRenderer(support, paginationDialect, upsertDialect, lockingReadDialect,
                                       protectedFields,
                                       FormFieldDecodingPlanCache.create(policy));
    }

    /**
     * 渲染单条 insert SQL。
     *
     * @param form 动态表单
     * @param values 字段值
     * @return 参数化 SQL 请求
     */
    public SqlRequest insert(DynamicForm form, Map<String, Object> values) {
        return writeRenderer.insert(form, values);
    }

    /**
     * 渲染默认策略的批量 insert。
     *
     * @param form 动态表单
     * @param rows 待写入行
     * @return 统一批量写入请求
     */
    public BatchWriteRequest insertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return insertBatch(form, rows, BatchWriteOptions.defaults());
    }

    /**
     * 渲染带执行策略的批量 insert。
     *
     * @param form 动态表单
     * @param rows 待写入行
     * @param options 批量执行策略
     * @return 统一批量写入请求
     */
    public BatchWriteRequest insertBatch(DynamicForm form,
                                         List<Map<String, Object>> rows,
                                         BatchWriteOptions options) {
        return batchRenderer.insertBatch(form, rows, options);
    }

    /**
     * 渲染默认策略的批量 upsert。
     *
     * @param form 动态表单
     * @param rows 待写入行
     * @return 统一批量写入请求
     */
    public BatchWriteRequest upsertBatch(DynamicForm form, List<Map<String, Object>> rows) {
        return upsertBatch(form, rows, BatchWriteOptions.defaults());
    }

    /**
     * 渲染带执行策略的批量 upsert。
     *
     * @param form 动态表单
     * @param rows 待写入行
     * @param options 批量执行策略
     * @return 统一批量写入请求
     */
    public BatchWriteRequest upsertBatch(DynamicForm form,
                                         List<Map<String, Object>> rows,
                                         BatchWriteOptions options) {
        return batchRenderer.upsertBatch(form, rows, options);
    }

    public SqlRequest select(DynamicForm form, ConditionGroup where) {
        return queryRenderer.select(form, where);
    }

    public SqlRequest select(DynamicForm form, ConditionGroup where, PageQuery page) {
        return queryRenderer.select(form, where, page);
    }

    public SqlRequest selectOrdered(DynamicForm form, ConditionGroup where, List<PageSort> sorts) {
        return queryRenderer.selectOrdered(form, where, sorts);
    }

    public SqlRequest selectProjected(DynamicForm form,
                                      ConditionGroup where,
                                      List<String> projections,
                                      List<String> groups,
                                      List<PageSort> sorts) {
        return queryRenderer.selectProjected(form, where, projections, groups, sorts);
    }

    public SqlRequest select(DynamicForm form, ConditionGroup where, CursorPageQuery page) {
        return queryRenderer.select(form, where, page);
    }

    public SqlRequest count(DynamicForm form, ConditionGroup where) {
        return queryRenderer.count(form, where);
    }

    /** 同包 JOIN 执行链复用的独立渲染入口。 */
    JoinQuerySqlRenderer joinQueries() {
        return joinRenderer;
    }

    /**
     * 渲染带显式业务条件的 update SQL。
     *
     * @param form 动态表单
     * @param values 待更新字段
     * @param where 更新条件
     * @return 参数化 SQL 请求
     */
    public SqlRequest update(DynamicForm form, Map<String, Object> values, ConditionGroup where) {
        return writeRenderer.update(form, values, where);
    }

    /**
     * 渲染带乐观锁的 update SQL，版本字段同时出现在 set 和 where 中。
     */
    public SqlRequest update(DynamicForm form,
                             Map<String, Object> values,
                             ConditionGroup where,
                             OptimisticLockOptions lock) {
        return writeRenderer.update(form, values, where, lock);
    }

    /**
     * 固定批量乐观更新的 SQL 和参数类型；后续行必须保持同一字段和条件布局。
     */
    BatchUpdatePlan optimisticUpdatePlan(DynamicForm form,
                                         Map<String, Object> values,
                                         ConditionGroup where,
                                         OptimisticLockOptions lock,
                                         SqlRequest request) {
        return writeRenderer.optimisticUpdatePlan(form, values, where, lock, request);
    }

    public SqlRequest delete(DynamicForm form, ConditionGroup where) {
        return writeRenderer.delete(form, where);
    }

    /**
     * delete 不修改版本值，只要求 where 中的版本值能够匹配。
     */
    public SqlRequest delete(DynamicForm form, ConditionGroup where, OptimisticLockOptions lock) {
        return writeRenderer.delete(form, where, lock);
    }

    FormScalarReadPlan scalarReadPlan(DynamicField field) {
        return FormScalarReadPlan.compile(
                field, support.dialectName, support.nativeBoolean, support.valueCodecs);
    }

    Object readScalarValue(DynamicField field, Object value) {
        FormScalarReadPlan plan = scalarReadPlan(field);
        return plan == null
                ? DialectScalarValueCodec.read(value, field.databaseType(), support.valueCodecs)
                : plan.read(value);
    }

    FormFieldDecodingPlan resultDecodingPlan(DynamicForm form) {
        return decodingPlans.plan(form, this, !support.customFieldCodecs.isEmpty());
    }

    EntityTypeMappingRegistry.Mapping customFieldMapping(DynamicField field) {
        return support.customFieldMapping(field);
    }

    /** 返回客户端缓存图使用的结果计划失效入口。 */
    @InternalApi
    public MetadataCacheInvalidator resultPlanInvalidator() {
        return decodingPlans;
    }

    /**
     * 返回条件、单条写入和批量写入共同使用的 codec 注册表。
     *
     * @return 当前 codec 注册表
     */
    public ValueCodecRegistry valueCodecs() {
        return support.valueCodecs;
    }

    FormProtectionSqlSupport protection() {
        return protection;
    }

    /** 锁定读取 planner 只读取装配时冻结的受控方言能力，不按请求字符串推断。 */
    LockingReadDialect lockingReadDialect() {
        return lockingReadDialect;
    }

}
