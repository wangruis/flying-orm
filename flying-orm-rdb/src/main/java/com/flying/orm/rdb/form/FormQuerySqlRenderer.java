package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.ReadLock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 模型与受信关系查询共用的内部 SQL 实现。
 *
 * <p>这里集中处理投影、排序和 offset/cursor 分页。条件 AST、字段识别、计划缓存和参数规则
 * 均由共享 support 提供，避免查询与写入在边界处出现两套语义。实例只在门面构造时创建一次，
 * 可以安全地被并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
@InternalApi
public final class FormQuerySqlRenderer {
    public enum ReadShape { ROWS, FIRST, COUNT }

    /** 受信关系与模型查询共用子句生成；这里只使用调用方已有的 SQL 渲染器和字段范围。 */
    public static SqlRequest trusted(SqlRenderer renderer, QuerySpec spec, FieldScope fields,
                                     ReadShape shape, RdbDialect dialect) {
        boolean count = shape == ReadShape.COUNT;
        PageQuery page = shape == ReadShape.ROWS ? spec.page().orElse(null) : null;
        ReadLock lock = count ? null : spec.lock().orElse(null);
        if (lock != null && page != null) {
            throw new UnsupportedOperationException("locking offset pagination is not supported; use keyset pagination");
        }
        LockingReadDialect locking = lock == null ? null : Objects.requireNonNull(dialect,
                "locking read requires an RDB dialect").lockingReadDialect();
        if (lock != null && (!spec.groups().isEmpty()
                || shape == ReadShape.FIRST && !locking.supportsPagination(lock))) {
            throw new UnsupportedOperationException("locking read does not support this query shape");
        }
        String table = spec.table();
        String renderedTable = dialect == null ? renderer.identifier(table)
                : dialect.schema().relationIdentifier(spec.relationIdentity());
        String selected = count ? "1" : selectColumns(renderer, spec, fields, table);
        ConditionGroup active = spec.where();
        SqlFragment where = renderer.hasCorrelatedTerms()
                && !active.executionView().cacheable(renderer.standardConditionTermMask())
                ? renderer.renderWhere(active,
                        name -> name.indexOf('.') >= 0 ? renderer.identifier(name)
                                : renderedTable + "." + renderer.identifier(name),
                        name -> name.indexOf('.') >= 0
                                ? renderer.identifier(name.substring(0, name.lastIndexOf('.'))) : renderedTable)
                : renderer.renderWhere(active);
        if (!count && !spec.groups().isEmpty() && spec.projections().isEmpty()) {
            throw new IllegalStateException("grouped query requires explicit select fields");
        }
        StringJoiner groups = new StringJoiner(", ");
        for (String field : spec.groups()) {
            requireReadableProjection(fields, field, table);
            groups.add(renderer.identifier(field));
        }
        StringJoiner order = new StringJoiner(", ");
        if (!count) {
            for (PageSort sort : spec.sorts()) {
                requireReadableProjection(fields, sort.field(), table);
                order.add(renderer.identifier(sort.field()) + " " + sort.sqlKeyword());
            }
        }
        String from = fromClause(renderedTable, where.sql(), groups.toString(),
                order.length() == 0 ? "" : " order by " + order,
                lock == null ? "" : locking.tableHint(lock));
        if (count) return new SqlRequest(countSql(from, !spec.groups().isEmpty(), dialect.name()), where.parameters());
        String suffix = lock == null ? "" : locking.suffix(lock);
        if (page != null) {
            return Objects.requireNonNull(dialect, "pagination requires an RDB dialect").pagination()
                    .paginate("select " + selected + " " + from, where.parameters(), page);
        }
        return shape == ReadShape.FIRST
                ? new SqlRequest(dialect.pagination().firstSelectSql(selected, from, where.parameters()) + suffix,
                        dialect.pagination().firstSelectParameters(where.parameters()))
                : new SqlRequest("select " + selected + " " + from + suffix, where.parameters());
    }

    private static String selectColumns(SqlRenderer renderer, QuerySpec spec, FieldScope fields, String resource) {
        List<String> projections = spec.projections();
        Set<String> exclusions = spec.excludedFields();
        FieldScope safeFields = Objects.requireNonNull(fields, "field scope must not be null");
        if (projections.isEmpty() && safeFields.unrestrictedRead()) {
            if (!exclusions.isEmpty()) {
                throw new IllegalStateException("excluding unknown table columns requires from(DynamicForm) or explicit select fields");
            }
            return "*";
        }
        StringJoiner joiner = new StringJoiner(", ");
        Collection<String> candidates = projections.isEmpty() ? safeFields.readableFields() : projections;
        for (String field : candidates) {
            if (!exclusions.isEmpty()) {
                if (field.indexOf('*') >= 0) {
                    throw new IllegalStateException("excluding wildcard columns requires from(DynamicForm) or explicit select fields");
                }
                if (exclusions.contains(FieldIdentity.of(field).key())) continue;
            }
            if (!projections.isEmpty()) requireReadableProjection(safeFields, field, resource);
            joiner.add(renderer.projection(field));
        }
        if (joiner.length() == 0) {
            if (!exclusions.isEmpty()) {
                throw new IllegalArgumentException("projected query must select at least one field");
            }
            throw new ScopeAccessException(ScopeErrorCode.NO_READABLE_FIELDS, resource, null,
                    "field scope leaves no readable fields for query");
        }
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
    private final FormSqlRenderSupport support;
    private final PaginationDialect paginationDialect;

    FormQuerySqlRenderer(FormSqlRenderSupport support, PaginationDialect paginationDialect) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.paginationDialect = Objects.requireNonNull(paginationDialect, "pagination dialect must not be null");
    }

    /** 普通、投影、分页、首条和锁定读取共用一次字段与条件准备；null 投影表示全部模型列。 */
    SqlRequest select(DynamicForm form, ConditionGroup where, List<String> projections,
                      List<String> groups, List<PageSort> sorts, PageQuery page, boolean first,
                      LockingReadDialect dialect, ReadLock lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (projections != null && projections.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one entity field");
        }
        List<String> selectedFields = projections == null ? support.fieldNames(safeForm.fields())
                : FormSqlRenderSupport.canonicalFieldNames(safeForm, projections);
        List<String> groupedFields = groups.isEmpty() ? List.of()
                : FormSqlRenderSupport.canonicalFieldNames(safeForm, groups);
        FormSqlRenderSupport.ConditionSql condition = support.condition(safeForm, where);
        String groupShape = groupedFields.isEmpty() ? "" : String.join(",", groupedFields);
        String sortShape = sorts.isEmpty() ? "" : pageSortShape(safeForm, sorts);
        String operation = first ? "select-first" : "select";
        String tableHint = "";
        String suffix = "";
        if (lock != null) {
            LockingReadDialect locking = Objects.requireNonNull(dialect, "locking read dialect must not be null");
            tableHint = locking.tableHint(lock);
            suffix = locking.suffix(lock);
            operation += "-locking-" + lockShape(lock);
        }
        List<Object> parameters = page != null
                ? paginationDialect.paginationParameters(condition.parameters(), page)
                : first ? paginationDialect.firstSelectParameters(condition.parameters()) : condition.parameters();
        String hint = tableHint;
        String ending = suffix;
        return support.request(operation, safeForm, selectedFields, condition, groupShape, sortShape,
                page == null ? "" : "offset", parameters, () -> {
                    String from = fromClause(safeForm, condition, groupedFields, orderBy(safeForm, sorts), hint);
                    String columns = selectedFields.isEmpty() ? "*" : support.identifierColumns(selectedFields);
                    String sql = first ? paginationDialect.firstSelectSql(columns, from, condition.parameters())
                            : "select " + columns + " " + from;
                    return (page == null ? sql
                            : PaginationDialect.paginateSql(paginationDialect, sql, condition.parameters(), page)) + ending;
                });
    }

    SqlRequest selectExpressions(DynamicForm form, ConditionGroup where, FormQueryExpressions.Projection projection,
                                  List<String> groups, List<PageSort> sorts,
                                  boolean first, LockingReadDialect dialect, ReadLock lock) {
        SqlFragment columns = projection.columns();
        DynamicForm resultForm = projection.resultForm();
        FormSqlRenderSupport.ConditionSql condition = support.condition(form, where);
        pageSortShape(resultForm, sorts);
        List<String> grouped = FormSqlRenderSupport.canonicalFieldNames(form, groups);
        SqlFragment ordering = projection.orderBy(sorts, field -> support.identifier(resultForm.field(field).name()));
        String relation = fromClause(form, condition, grouped, ordering.sql().isEmpty() ? "" : " " + ordering.sql(),
                lock == null ? "" : dialect.tableHint(lock));
        List<Object> parameters = new ArrayList<>(columns.parameters());
        parameters.addAll(condition.parameters());
        parameters.addAll(ordering.parameters());
        String sql = first ? paginationDialect.firstSelectSql(columns.sql(), relation, parameters)
                : "select " + columns.sql() + " " + relation;
        if (lock != null) sql += dialect.suffix(lock);
        List<Object> finalParameters = first ? paginationDialect.firstSelectParameters(parameters) : parameters;
        return new SqlRequest(support.compiledStatement(sql, finalParameters.size(), SqlBindMarkerStyle.CANONICAL),
                finalParameters);
    }

    private String fromClause(DynamicForm form, FormSqlRenderSupport.ConditionSql where, List<String> groups,
                               String order, String tableHint) {
        return fromClause(support.identifier(form), where.sql(), support.identifierColumns(groups),
                order, tableHint);
    }

    SqlRequest selectKeyset(DynamicForm form,
                            ConditionGroup where,
                            HiddenProjectionLayout layout,
                            KeysetPageNormalizer.NormalizedKeysetPage page) {
        return FormKeysetSqlRenderer.select(
                support, paginationDialect, form, where, layout, page);
    }

    /** 为已规范化的 keyset 查询附加同一个受控锁，不改变其游标布局和结果预算。 */
    SqlRequest selectKeysetLocking(DynamicForm form,
                                   ConditionGroup where,
                                   HiddenProjectionLayout layout,
                                   KeysetPageNormalizer.NormalizedKeysetPage page,
                                   LockingReadDialect dialect,
                                   ReadLock lock) {
        return FormKeysetSqlRenderer.selectLocking(
                support, paginationDialect, form, where, layout, page, dialect, lock);
    }

    SqlRequest count(DynamicForm form, ConditionGroup where, List<String> groups) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<String> groupedFields = groups.stream().map(safeForm::field).map(DynamicField::name).toList();
        return support.request("count", safeForm, List.of(), whereFragment, String.join(",", groupedFields), "", "",
                                 whereFragment.parameters(), () -> {
                             return countSql(fromClause(safeForm, whereFragment, groupedFields, "", ""),
                                     !groupedFields.isEmpty(), support.dialectName);
                       });
    }

    private static String fromClause(String table, String where, String groups,
                                     String order, String tableHint) {
        StringBuilder sql = new StringBuilder("from ").append(table).append(tableHint);
        if (!where.isBlank()) sql.append(" where ").append(where);
        if (!groups.isEmpty()) sql.append(" group by ").append(groups);
        return sql.append(order).toString();
    }

    private static String countSql(String from, boolean grouped, String dialect) {
        String function = "sqlserver".equals(dialect) ? "count_big" : "count";
        return "select " + function + "(*) as total " + (grouped
                ? "from (select 1 as group_marker " + from + ") flying_count" : from);
    }

    /** 缓存形状包含完整锁语义，后续增加锁强度时也不会复用另一种锁的 SQL。 */
    static String lockShape(ReadLock lock) {
        return lock.strength() + ":" + lock.waitMode();
    }

    private String pageSortShape(DynamicForm form, List<PageSort> sorts) {
        StringJoiner shape = new StringJoiner(";");
        for (PageSort sort : sorts) {
            DynamicField field = form.field(sort.field());
            support.requireStableOffsetTimeOrdering(field);
            shape.add(field.name() + ":" + sort.direction().name());
        }
        return shape.toString();
    }

    private String orderBy(DynamicForm form, List<PageSort> sorts) {
        if (sorts.isEmpty()) {
            return "";
        }
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (PageSort sort : sorts) {
            DynamicField field = form.field(sort.field());
            order.add(support.identifier(field.name()) + " " + sort.sqlKeyword());
        }
        return order.toString();
    }

}
