package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
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
final class DmlQueryCommand {

    private final SqlRenderer renderer;
    private final DataScope defaultDataScope;
    private final List<String> projections = new ArrayList<>();
    private List<PageSort> sorts = List.of();
    private List<String> groups = List.of();
    private StructuredConditionInput filter;
    private StructuredConditionPolicy filterPolicy;
    private SensitiveDisplayMode display = SensitiveDisplayMode.DECLARED;

    private String table;
    private String renderedTable;
    private WhereDsl where;
    private LogicDeleteDefinition logicDelete;
    private DataScope scope = DataScope.none();
    private DynamicForm governedForm;
    private FieldUsePolicy fieldUsePolicy;
    private QueryShapeLimits queryShapeLimits;

    DmlQueryCommand(SqlRenderer renderer, DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
        this.where = new WhereDsl(renderer);
    }

    void select(String... columns) {
        Objects.requireNonNull(columns, "select columns must not be null");
        for (String column : columns) {
            projections.add(SqlIdentifiers.requireProjection(column, "select column"));
        }
    }

    void from(String table) {
        String safeTable = Objects.requireNonNull(table, "query table must not be null").trim();
        String safeRenderedTable = renderer.identifier(safeTable);
        this.table = safeTable;
        this.renderedTable = safeRenderedTable;
        this.governedForm = null;
        this.fieldUsePolicy = null;
        this.queryShapeLimits = null;
    }

    /** DynamicForm/metadata 入口显式启用治理；物理表 string 入口仍是 trusted。 */
    void from(DynamicForm form) {
        DynamicForm safeForm = Objects.requireNonNull(form, "query form must not be null");
        from(safeForm.table());
        this.governedForm = safeForm;
    }

    void from(DynamicForm form, FieldUsePolicy policy, QueryShapeLimits limits) {
        from(form);
        this.fieldUsePolicy = Objects.requireNonNull(policy, "field use policy must not be null");
        this.queryShapeLimits = Objects.requireNonNull(limits, "query shape limits must not be null");
    }

    void where(Consumer<WhereDsl> consumer) {
        Objects.requireNonNull(consumer, "where consumer must not be null");
        WhereDsl dsl = new WhereDsl(renderer);
        consumer.accept(dsl);
        this.where = new WhereDsl(renderer, dsl.build());
    }

    void where(String field, String operator, Object value) {
        where.where(field, operator, value);
    }

    void logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        this.logicDelete = LogicDeleteDefinition.of(
                SqlIdentifiers.requireIdentifier(fieldName, "operator logic delete field"),
                notDeletedValue,
                deletedValue);
    }

    void scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }

    void orderBy(String field, PageSort.Direction direction) {
        if (sorts.isEmpty()) { sorts = new ArrayList<>(); }
        sorts.add(new PageSort(SqlIdentifiers.requireIdentifier(field, "sort field"), direction));
    }

    void groupBy(String... fields) {
        Objects.requireNonNull(fields, "group fields must not be null");
        if (groups.isEmpty()) { groups = new ArrayList<>(); }
        for (String field : fields) {
            groups.add(SqlIdentifiers.requireIdentifier(field, "group field"));
        }
    }

    void filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
        filter = Objects.requireNonNull(input, "structured condition input must not be null");
        filterPolicy = Objects.requireNonNull(policy, "structured condition policy must not be null");
    }

    void display(SensitiveDisplayMode mode) {
        display = Objects.requireNonNull(mode, "sensitive display mode must not be null");
    }

    SqlRequest toRequest() {
        if (governed()) {
            throw new IllegalStateException("governed query must use the Form query kernel");
        }
        String safeTable = Objects.requireNonNull(table, "query table must be configured");
        if (filter != null || display != SensitiveDisplayMode.DECLARED) {
            throw new IllegalStateException("frontend filters and field protection require from(DynamicForm)");
        }
        return request(safeTable, defaultDataScope.and(scope));
    }

    boolean governed() {
        return governedForm != null;
    }

    /**
     * 把 governed operator 的可变构建状态冻结为正式 Form 查询规格。
     * 默认 Scope 已经绑定在 FormClient 上，这里只携带本次追加范围，避免重复应用租户条件。
     */
    GovernedQuery governedQuery(SqlExecutionOptions options) {
        DynamicForm form = Objects.requireNonNull(governedForm, "governed query form must be configured");
        QuerySpec spec = filter == null ? QuerySpec.of(form, operatorWhere())
                : QuerySpec.structured(form, filter).withStructuredPolicy(filterPolicy).withWhere(operatorWhere());
        spec = spec.withScope(scope);
        if (!sorts.isEmpty()) {
            spec = spec.withSorts(sorts);
        }
        if (!groups.isEmpty() && projections.isEmpty()) {
            throw new IllegalStateException("grouped query requires explicit select fields");
        }
        if (!projections.isEmpty()) {
            spec = spec.withProjection(projections, groups);
        }
        if (options != null) {
            spec = spec.withExecutionOptions(options);
        }
        spec = switch (display) {
            case DECLARED -> spec;
            case MASKED -> spec.masked();
            case FULL -> spec.showSensitive();
        };
        return new GovernedQuery(spec, fieldUsePolicy, queryShapeLimits);
    }

    private SqlRequest request(String safeTable, DataScope effectiveScope) {
        StringBuilder sql = new StringBuilder("select ");
        sql.append(selectColumns(effectiveScope.fields(), safeTable));
        sql.append(" from ").append(renderedTable);

        ConditionGroup active = activeWhere(effectiveScope);
        SqlFragment whereFragment = renderer.hasCorrelatedTerms()
                && !active.executionView().cacheable(renderer.standardConditionTermMask())
                ? renderer.renderWhere(active,
                        name -> name.indexOf('.') >= 0 ? renderer.identifier(name)
                                : renderedTable + "." + renderer.identifier(name),
                        name -> name.indexOf('.') >= 0
                                ? renderer.identifier(name.substring(0, name.lastIndexOf('.'))) : renderedTable)
                : renderer.renderWhere(active);
        if (!whereFragment.sql().isBlank()) {
            sql.append(" where ").append(whereFragment.sql());
        }
        if (!groups.isEmpty()) {
            if (projections.isEmpty()) {
                throw new IllegalStateException("grouped query requires explicit select fields");
            }
            StringJoiner groupSql = new StringJoiner(", ");
            for (String field : groups) {
                requireReadableProjection(effectiveScope.fields(), field, safeTable);
                groupSql.add(renderer.identifier(field));
            }
            sql.append(" group by ").append(groupSql);
        }
        if (!sorts.isEmpty()) {
            StringJoiner orderSql = new StringJoiner(", ");
            for (PageSort sort : sorts) {
                requireReadableProjection(effectiveScope.fields(), sort.field(), safeTable);
                orderSql.add(renderer.identifier(sort.field()) + " " + sort.sqlKeyword());
            }
            sql.append(" order by ").append(orderSql);
        }
        return new SqlRequest(sql.toString(), whereFragment.parameters());
    }

    record GovernedQuery(QuerySpec spec, FieldUsePolicy policy, QueryShapeLimits limits) {
        GovernedQuery {
            spec = Objects.requireNonNull(spec, "governed query spec must not be null");
            if ((policy == null) != (limits == null)) {
                throw new IllegalArgumentException("field policy and query limits must be configured together");
            }
        }
    }

    private String selectColumns(FieldScope fields, String resource) {
        FieldScope safeFields = Objects.requireNonNull(fields, "field scope must not be null");
        if (projections.isEmpty() && safeFields.unrestrictedRead()) {
            return "*";
        }
        StringJoiner joiner = new StringJoiner(", ");
        if (projections.isEmpty()) {
            safeFields.readableFields().stream().map(renderer::projection).forEach(joiner::add);
            if (joiner.length() == 0) {
                throw new ScopeAccessException(ScopeErrorCode.NO_READABLE_FIELDS,
                                               resource,
                                               null,
                                               "field scope leaves no readable fields for query");
            }
            return joiner.toString();
        }
        projections.stream()
                   .peek(projection -> requireReadableProjection(safeFields, projection, resource))
                   .map(renderer::projection)
                   .forEach(joiner::add);
        return joiner.toString();
    }

    private static void requireReadableProjection(FieldScope fields, String projection, String resource) {
        if (!fields.canRead(projection)) {
            throw new ScopeAccessException(ScopeErrorCode.FIELD_NOT_READABLE,
                                           resource,
                                           projection,
                                           "field [" + projection + "] is not readable for query");
        }
    }

    private ConditionGroup activeWhere(DataScope effectiveScope) {
        ConditionGroup businessWhere = where.build();
        ConditionGroup active = effectiveScope.condition()
                                                  .map(scopeWhere -> ConditionGroups.and(businessWhere, scopeWhere))
                                                  .orElse(businessWhere);
        return appendLogicDelete(active);
    }

    private ConditionGroup operatorWhere() {
        return appendLogicDelete(where.build());
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
