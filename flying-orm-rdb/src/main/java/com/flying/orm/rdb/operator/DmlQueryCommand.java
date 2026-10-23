package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.rdb.form.spec.QueryInput;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.FormQuerySqlRenderer;
import com.flying.orm.rdb.form.FormQuerySqlRenderer.ReadShape;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 保存一条链式动态查询的可变构建状态，并生成最终不可变 SQL 请求。
 *
 * <p>这里没有 JDBC、R2DBC 或 Reactor 类型。响应式和同步门面共用本命令，因此投影校验、默认 Scope、
 * 显式 Scope、业务条件和逻辑删除永远按同一顺序合并。对象只供单次调用使用，不能跨线程共享。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public abstract class DmlQueryCommand<S extends DmlQueryCommand<S>> implements ConditionDsl<String, S> {

    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;
    private final QuerySpec.Builder query;
    private WhereDsl where;
    private LogicDeleteDefinition logicDelete;
    private FieldUsePolicy fieldUsePolicy;
    private QueryShapeLimits queryShapeLimits;

    DmlQueryCommand(SqlRenderer renderer, DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
        this.query = QuerySpec.builder();
        this.where = new WhereDsl(renderer);
    }

    DmlQueryCommand(DmlQueryCommand<?> source) {
        this.renderer = source.renderer;
        this.defaultDataScope = source.defaultDataScope;
        this.query = source.query.copy();
        this.where = new WhereDsl(renderer, source.where.build());
        this.logicDelete = source.logicDelete;
        this.fieldUsePolicy = source.fieldUsePolicy;
        this.queryShapeLimits = source.queryShapeLimits;
    }

    protected abstract S self();

    /** 追加查询字段；没有显式投影时仍由 FieldScope 决定是否允许使用星号。 */
    public S select(String... columns) {
        query.select(columns);
        return self();
    }

    /** 排除结果字段，不影响条件与排序；纯表名入口须有显式选列或受限可读字段，不能展开星号。 */
    public S exclude(String... columns) {
        query.exclude(columns);
        return self();
    }

    /** 追加计算结果列；使用 from(DynamicForm) 提供来源字段类型及治理规则。 */
    public S select(ValueExpression expression, String alias) {
        query.expression(alias, expression);
        return self();
    }

    /** 设置目标物理表，表名会立即按标识符规则校验。 */
    public S from(String table) {
        query.from(table);
        this.fieldUsePolicy = null;
        this.queryShapeLimits = null;
        return self();
    }

    /** DynamicForm/metadata 入口显式启用治理；物理表 string 入口仍是 trusted。 */
    public S from(DynamicForm form) {
        query.from(form);
        this.fieldUsePolicy = null;
        this.queryShapeLimits = null;
        return self();
    }

    /** 使用本次字段策略和查询形状预算启用 governed DML。 */
    public S from(DynamicForm form, FieldUsePolicy policy, QueryShapeLimits limits) {
        from(form);
        this.fieldUsePolicy = Objects.requireNonNull(policy, "field use policy must not be null");
        this.queryShapeLimits = Objects.requireNonNull(limits, "query shape limits must not be null");
        return self();
    }

    /** 以 AND 追加已构建的条件；OR 组作为整体保留，Scope 与字段治理仍由原执行链处理。 */
    public S where(ConditionGroup conditions) {
        where.append(Objects.requireNonNull(conditions, "where conditions must not be null"));
        return self();
    }

    /** 显式替换业务 where；Scope、前端过滤和逻辑删除条件独立保留。 */
    public S replaceWhere(Consumer<WhereDsl> consumer) {
        this.where = new WhereDsl(renderer, conditions(consumer));
        return self();
    }

    ConditionGroup conditions(Consumer<WhereDsl> consumer) {
        Objects.requireNonNull(consumer, "where consumer must not be null");
        WhereDsl dsl = new WhereDsl(renderer);
        consumer.accept(dsl);
        return dsl.build();
    }

    /** 以 AND 追加标准或已注册的业务条件，不接受 SQL 片段。 */
    public S where(String field, String operator, Object value) {
        where.where(field, operator, value);
        return self();
    }

    @Override
    public S whereIfPresent(String field, String operator, Object value) {
        where.whereIfPresent(field, operator, value);
        return self();
    }

    /** 声明逻辑删除字段及未删除、已删除业务值。 */
    public S logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        this.logicDelete = LogicDeleteDefinition.of(
                SqlIdentifiers.requireIdentifier(fieldName),
                notDeletedValue,
                deletedValue);
        return self();
    }

    /** 追加本次数据范围；它只会继续收紧门面已有的默认范围。 */
    public S scope(DataScope scope) {
        query.scope(query.scope().and(Objects.requireNonNull(scope, "data scope must not be null")));
        return self();
    }

    /** 追加排序表达式，不添加结果列；需要 from(DynamicForm)。 */
    public S orderBy(ValueExpression expression, PageSort.Direction direction) {
        if (expression instanceof ValueExpression.Field field && field.source() == null) {
            SqlIdentifiers.requireIdentifier(field.identity().name());
        }
        query.orderBy(expression, direction);
        return self();
    }

    /** 追加分组字段；读取分组行须显式 select，count 无需投影。 */
    public S groupBy(String... fields) {
        query.groupBy(fields);
        return self();
    }

    /** 指定前端条件的字段、操作符和复杂度策略。 */
    public S filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
        query.filter(input, policy);
        return self();
    }

    /** 整体替换外部查询形状，不清除服务端 where 与 Scope。 */
    public S input(QueryInput input, StructuredConditionPolicy policy) {
        query.input(input, policy);
        return self();
    }

    PageQuery inputPage() {
        return query.page().orElseThrow(() -> new IllegalStateException("query input has no page"));
    }

    void display(SensitiveDisplayMode mode) {
        query.display(mode);
    }

    /** 追加升序字段。 */
    public S orderByAsc(String field) { return orderBy(ValueExpression.field(field), PageSort.Direction.ASC); }

    /** 追加降序字段。 */
    public S orderByDesc(String field) { return orderBy(ValueExpression.field(field), PageSort.Direction.DESC); }

    /** 按给定值的先后优先排列，其他值随后；本排序项的优先级由调用顺序决定。 */
    public S orderByFirst(String field, Object... values) {
        return orderBy(ValueExpression.field(field).priority(values), PageSort.Direction.ASC);
    }

    /** 其他值在前，指定值按给定顺序排在最后；排序项优先级由调用顺序决定。 */
    public S orderByLast(String field, Object... values) {
        return orderBy(ValueExpression.lastMatch(ValueExpression.field(field).priority(values)), PageSort.Direction.ASC);
    }

    /** 导入条件、选列、排除、排序与可选分页。 */
    public S input(QueryInput input) {
        return input(input, StructuredConditionPolicy.defaults());
    }

    /** 使用默认安全策略处理前端结构化条件，与服务端 where 和 Scope 取交集。 */
    public S filter(StructuredConditionInput input) { return filter(input, StructuredConditionPolicy.defaults()); }

    /** 按字段声明展示结果。 */
    public S declaredDisplay() { display(SensitiveDisplayMode.DECLARED); return self(); }

    /** 对已声明脱敏字段使用脱敏展示；显式 FieldUsePolicy 优先，应在策略中设置 MASKED。 */
    public S masked() { display(SensitiveDisplayMode.MASKED); return self(); }

    /** 向已获授权的后端展示完整敏感值；不会放宽日志脱敏。 */
    public S showSensitive() { display(SensitiveDisplayMode.FULL); return self(); }

    /** 使用 DynamicForm 元数据和显式字段策略启用 governed DML；string 表入口仍为 trusted。 */
    public S from(DynamicForm form, FieldUsePolicy policy) {
        return from(form, policy, QueryShapeLimits.defaults());
    }

    /** 以 AND 追加一组业务条件；回调只接受条件 DSL，不影响 Scope 和前端过滤。 */
    public S where(Consumer<WhereDsl> consumer) {
        where(conditions(consumer));
        return self();
    }

    /** 使用常见的 0=未删除、1=已删除约定。 */
    public S logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    SqlRequest toRequest(RdbDialect dialect) {
        return toRequest(ReadShape.ROWS, dialect);
    }

    SqlRequest toPageRequest(PageQuery page, RdbDialect dialect) {
        return toRequest(ReadShape.ROWS, dialect, page);
    }

    SqlRequest toFirstRequest(RdbDialect dialect) {
        return toRequest(ReadShape.FIRST, dialect);
    }

    SqlRequest toCountRequest(RdbDialect dialect) {
        return toRequest(ReadShape.COUNT, dialect);
    }

    private SqlRequest toRequest(ReadShape shape, RdbDialect dialect) {
        return toRequest(shape, dialect, null);
    }

    private SqlRequest toRequest(ReadShape shape, RdbDialect dialect, PageQuery page) {
        if (governed()) {
            throw new IllegalStateException("governed query must use the Form query kernel");
        }
        DataScope effectiveScope = defaultDataScope.and(query.scope());
        ConditionGroup businessWhere = where.build();
        ConditionGroup active = effectiveScope.condition()
                .map(scopeWhere -> ConditionGroups.and(businessWhere, scopeWhere)).orElse(businessWhere);
        query.where(appendLogicDelete(active));
        QuerySpec spec = shape == ReadShape.COUNT ? query.buildCount() : query.build();
        if (page != null) spec = spec.withPage(page);
        return FormQuerySqlRenderer.trusted(renderer, spec, effectiveScope.fields(), shape, dialect);
    }

    boolean governed() {
        return query.hasModel();
    }

    /**
     * 把 governed operator 的可变构建状态冻结为正式 Form 查询规格。
     * 默认 Scope 已经绑定在 FormClient 上，这里只携带本次追加范围，避免重复应用租户条件。
     */
    GovernedQuery governedQuery(SqlExecutionOptions options) {
        return governedQuery(options, false);
    }

    GovernedQuery governedCountQuery(SqlExecutionOptions options) {
        return governedQuery(options, true);
    }

    private GovernedQuery governedQuery(SqlExecutionOptions options, boolean count) {
        if (!governed()) throw new IllegalStateException("governed query form must be configured");
        if (!count && query.hasGroups() && !query.hasProjection()) {
            throw new IllegalStateException("grouped query requires explicit select fields");
        }
        query.where(appendLogicDelete(where.build())).executionOptions(options);
        return new GovernedQuery(count ? query.buildCount() : query.build(), fieldUsePolicy, queryShapeLimits);
    }

    record GovernedQuery(QuerySpec spec, FieldUsePolicy policy, QueryShapeLimits limits) {
        GovernedQuery {
            spec = Objects.requireNonNull(spec, "governed query spec must not be null");
            if ((policy == null) != (limits == null)) {
                throw new IllegalArgumentException("field policy and query limits must be configured together");
            }
        }
    }

    private ConditionGroup appendLogicDelete(ConditionGroup active) {
        if (logicDelete == null) {
            return active;
        }
        ConditionGroup logicWhere = ConditionGroup.and()
                                                  .where(logicDelete.fieldName(),
                                                         "=",
                                                         logicDelete.notDeletedValue())
                                                  .build();
        return ConditionGroups.and(active, logicWhere);
    }
}
