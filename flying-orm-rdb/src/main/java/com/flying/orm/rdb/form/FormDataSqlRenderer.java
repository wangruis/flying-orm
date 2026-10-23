package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.UpsertDialect;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Collections;
import java.util.IdentityHashMap;
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

    final FormSqlRenderSupport support;
    private final PaginationDialect paginationDialect;
    private final UpsertDialect upsertDialect;
    private final LockingReadDialect lockingReadDialect;
    final FormQuerySqlRenderer queryRenderer;
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
                this.support, this.queryRenderer, this.protectedFields);
    }

    /**
     * 返回与当前 DML 门面共享方言引用和 term 注册表的条件渲染器。
     *
     * @return 不可变的条件渲染器
     */
    SqlRenderer conditionRenderer() {
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

    SqlExpressionCompiler.Operand compileExpression(ValueExpression expression,
            Function<ValueExpression.Field, SqlExpressionCompiler.Operand> fields) {
        return support.expression(expression, fields);
    }

    static DynamicField expressionField(String name, LogicalType type) {
        String declaration = type == null ? "OTHER" : switch (type) {
            case SMALL_INTEGER -> "SMALLINT";
            case INTEGER -> "INTEGER";
            case BIG_INTEGER -> "BIGINT";
            case FLOAT -> "DOUBLE PRECISION";
            case TEXT -> "VARCHAR";
            case OFFSET_TIME -> "TIME WITH TIME ZONE";
            case OFFSET_TIMESTAMP -> "TIMESTAMP WITH TIME ZONE";
            default -> type.name();
        };
        return DynamicField.of(name, declaration);
    }

    SqlRequest selectExpressions(DynamicForm form, ConditionGroup where, FormQueryExpressions.Projection projection,
                                  List<String> groups, List<PageSort> sorts,
                                  boolean first, ReadLock lock) {
        return queryRenderer.selectExpressions(form, where, projection, groups, sorts,
                first, lockingReadDialect(), lock);
    }

    boolean postgresqlDialect() {
        return "postgresql".equals(support.dialectName);
    }

    SqlRequest selectKeysetExpressions(DynamicForm form, ConditionGroup where, SqlFragment columns,
                                       HiddenProjectionLayout layout, KeysetPageNormalizer.NormalizedKeysetPage page,
                                       LockingReadDialect dialect, ReadLock lock) {
        return FormKeysetSqlRenderer.selectExpressions(support, paginationDialect, form, where, columns,
                layout, page, dialect, lock);
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
        return new SqlFragment(condition.sql(),
                OwnedBindableValues.ownedValues(condition.parameters()));
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
        return new SqlFragment(condition.sql(),
                OwnedBindableValues.ownedValues(condition.parameters()));
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
        return create(conditionRenderer, dialect, StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()),
                CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of());
    }

    /** 客户端按最终配置一次构造渲染与解码组件，不创建随后丢弃的默认组件图。 */
    @InternalApi
    public static FormDataSqlRenderer create(SqlRenderer conditionRenderer, RdbDialect dialect,
                                             StructuralPlanCaches caches, CacheRegionPolicy resultCachePolicy,
                                             ProtectedFieldRuntime protectedFields,
                                             Iterable<? extends EntityMetadata<?>> models) {
        RdbDialect safeDialect = Objects.requireNonNull(dialect, "rdb dialect must not be null");
        UnaryOperator<String> identifiers = safeDialect.schema()::identifier;
        SqlRenderer dialectConditions = Objects.requireNonNull(conditionRenderer, "sql renderer must not be null")
                                                 .withIdentifierRenderer(identifiers);
        FormSqlRenderSupport support = new FormSqlRenderSupport(dialectConditions,
                                                                safeDialect.json(),
                                                                safeDialect.name(),
                                                                safeDialect.supports(DialectCapabilityId.NATIVE_BOOLEAN),
                                                                identifiers,
                                                                caches,
                                                                entityFieldCodecs(models),
                                                                safeDialect.capabilities(),
                                                                safeDialect.schema()::relationIdentifier);
        return new FormDataSqlRenderer(support, safeDialect.pagination(), safeDialect.upsert(),
                                       safeDialect.lockingReadDialect(),
                                       protectedFields,
                                       FormFieldDecodingPlanCache.create(resultCachePolicy));
    }

    /** 从已编译实体字段一次绑定执行所需的 codec；不构造或依赖 Schema 描述。 */
    @InternalApi
    public FormDataSqlRenderer withEntityModels(Iterable<? extends EntityMetadata<?>> models) {
        return withEntityFieldCodecs(entityFieldCodecs(models));
    }

    private static Map<DynamicField, EntityTypeMappingRegistry.Mapping> entityFieldCodecs(
            Iterable<? extends EntityMetadata<?>> models) {
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings = null;
        for (EntityMetadata<?> model : models) {
            for (EntityFieldMetadata field : model.fields()) {
                EntityTypeMappingRegistry.Mapping mapping = field.customMapping();
                if (mapping != null) {
                    if (mappings == null) mappings = new IdentityHashMap<>();
                    mappings.put(field.field(), mapping);
                }
            }
        }
        return mappings == null ? Map.of() : Collections.unmodifiableMap(mappings);
    }

    /** 按字段实例快照显式 codec 绑定；实体客户端使用已编译模型入口。 */
    @InternalApi
    public FormDataSqlRenderer withEntityFieldCodecs(
            Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings) {
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> owned = mappings.isEmpty()
                ? Map.of() : Collections.unmodifiableMap(new IdentityHashMap<>(mappings));
        return new FormDataSqlRenderer(support.withCustomFieldCodecs(owned),
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
        return queryRenderer.select(form, where, null, List.of(), List.of(), null, false, null, null);
    }

    public SqlRequest select(DynamicForm form, ConditionGroup where, PageQuery page) {
        Objects.requireNonNull(page, "page query must not be null");
        return queryRenderer.select(form, where, null, List.of(), page.sorts(), page, false, null, null);
    }

    public SqlRequest selectOrdered(DynamicForm form, ConditionGroup where, List<PageSort> sorts) {
        return queryRenderer.select(form, where, null, List.of(), sorts, null, false, null, null);
    }

    public SqlRequest selectProjected(DynamicForm form,
                                      ConditionGroup where,
                                      List<String> projections,
                                      List<String> groups,
                                      List<PageSort> sorts) {
        return queryRenderer.select(form, where, Objects.requireNonNull(projections, "query projections must not be null"),
                groups, sorts, null, false, null, null);
    }

    public SqlRequest select(DynamicForm form, ConditionGroup where, KeysetPageQuery page) {
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(form, page);
        return queryRenderer.selectKeyset(form, where,
                HiddenProjectionLayout.of(support.fieldNames(form.fields()), normalized), normalized);
    }

    public SqlRequest count(DynamicForm form, ConditionGroup where) {
        return queryRenderer.count(form, where, List.of());
    }

    /**
     * 注册用 Java 查询描述的 EXISTS 条件。子查询投影的别名对应外层字段名，多个投影组成复合关联键。
     * 子查询条件、JOIN、显式 Scope 和字段编码复用当前渲染器；不继承外层客户端的默认 Scope。
     * 这是可信服务端扩展，注册到与当前渲染器相同方言的 SqlRenderer 后使用。
     */
    public SqlTermHandler relationExists(String id, Function<Object, JoinQuerySpec> query) {
        return new RelatedQueryTermHandler(id, this, query, false);
    }

    /** 与 {@link #relationExists(String, Function)} 使用相同的关联键和查询规则，生成 NOT EXISTS。 */
    public SqlTermHandler relationNotExists(String id, Function<Object, JoinQuerySpec> query) {
        return new RelatedQueryTermHandler(id, this, query, true);
    }

    SqlRequest paginate(SqlRequest request, PageQuery page) {
        return paginationDialect.paginate(request.sql(),
                com.flying.orm.core.internal.value.OwnedBindableValues.ownedValues(request.parameters()), page);
    }

    /** 最终 SQL 复用统一编译器；参数仍属于当前请求，不进入结构缓存。 */
    SqlRequest compileRequest(String sql, List<Object> parameters) {
        return support.compiledRequest(sql, parameters);
    }

    /** 保留自定义分页方言的参数标记风格及请求参数所有权。 */
    SqlRequest compileRequest(SqlRequest request) {
        return new SqlRequest(support.compiledStatement(request.sql(), request.statement().parameterCount(),
                request.bindMarkerStyle()), request.parameters());
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
     * 固定批量更新的 SQL、参数类型和行数契约；后续行必须保持同一字段和条件布局。
     */
    BatchUpdatePlan batchUpdatePlan(DynamicForm form,
                                         Map<String, Object> values,
                                         OptimisticLockOptions lock,
                                         SqlRequest request) {
        return writeRenderer.batchUpdatePlan(form, values, lock, request);
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
