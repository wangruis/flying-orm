package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.form.KeysetPageNormalizer.NormalizedKeysetPage;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.ReadLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Keyset SQL、游标参数和缓存形状集中在同一个纯渲染 helper 中。
 *
 * @author wangr
 * @version v3.2
 */
final class FormKeysetSqlRenderer {

    private FormKeysetSqlRenderer() {
    }

    static SqlRequest select(FormSqlRenderSupport support,
                             PaginationDialect pagination,
                             DynamicForm form,
                             ConditionGroup where,
                             HiddenProjectionLayout layout,
                             KeysetPageNormalizer.NormalizedKeysetPage page) {
        return select(support, pagination, form, where, layout, page, null, null, null);
    }

    static SqlRequest selectLocking(FormSqlRenderSupport support,
                                    PaginationDialect pagination,
                                    DynamicForm form,
                                    ConditionGroup where,
                                    HiddenProjectionLayout layout,
                                    KeysetPageNormalizer.NormalizedKeysetPage page,
                                    LockingReadDialect dialect,
                                    ReadLock lock) {
        return select(support, pagination, form, where, layout, page,
                      Objects.requireNonNull(dialect, "locking read dialect must not be null"),
                      Objects.requireNonNull(lock, "read lock must not be null"), null);
    }

    static SqlRequest selectExpressions(FormSqlRenderSupport support, PaginationDialect pagination,
                                         DynamicForm form, ConditionGroup where, SqlFragment columns,
                                         HiddenProjectionLayout layout, KeysetPageNormalizer.NormalizedKeysetPage page,
                                         LockingReadDialect dialect, ReadLock lock) {
        return select(support, pagination, form, where, layout, page, dialect, lock, columns);
    }

    private static SqlRequest select(FormSqlRenderSupport support,
                                     PaginationDialect pagination,
                                     DynamicForm form,
                                     ConditionGroup where,
                                     HiddenProjectionLayout layout,
                                     KeysetPageNormalizer.NormalizedKeysetPage page,
                                     LockingReadDialect dialect,
                                     ReadLock lock,
                                     SqlFragment columns) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        HiddenProjectionLayout safeLayout = Objects.requireNonNull(
                layout, "keyset projection layout must not be null");
        KeysetPageNormalizer.NormalizedKeysetPage safePage = Objects.requireNonNull(
                page, "normalized keyset page must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<Object> whereParameters = OwnedBindableValues.ownedValues(whereFragment.parameters());
        List<Object> baseParameters = new ArrayList<>(whereParameters.size()
                + (columns == null ? 0 : columns.parameters().size()));
        if (columns != null) baseParameters.addAll(columns.parameters());
        baseParameters.addAll(whereParameters);
        appendCursorParameters(safePage,
                (field, value) -> support.writeValue(safeForm.field(field), value), baseParameters);
        PageQuery limit = PageQuery.of(1, Math.addExact(safePage.size(), 1));
        List<Object> parameters = pagination.paginationParameters(baseParameters, limit);
        String sortShape = sortShape(support, safeForm, safePage);
        String pageShape = pageShape(safePage);
        List<String> selectionShape = safeLayout.selections().stream()
                .map(selection -> selection.field() + "=" + selection.label()).toList();
        boolean locking = lock != null;
        if (locking && !dialect.supportsPagination(lock)) {
            throw new UnsupportedOperationException(
                    "locking keyset pagination is not supported by this database descriptor");
        }
        String tableHint = locking ? dialect.tableHint(lock) : "";
        String suffix = locking ? dialect.suffix(lock) : "";
        if (columns != null) {
            String sql = PaginationDialect.paginateSql(pagination,
                    sql(support, safeForm, safeLayout, whereFragment, safePage, tableHint, columns),
                    baseParameters, limit) + suffix;
            return new SqlRequest(support.compiledStatement(sql, parameters.size(),
                    com.flying.orm.core.sql.render.SqlBindMarkerStyle.CANONICAL), parameters);
        }
        String operation = locking
                ? "select-keyset-locking-" + FormQuerySqlRenderer.lockShape(lock)
                : "select-keyset";
        return support.request(
                operation, safeForm, selectionShape, whereFragment, "", sortShape, pageShape,
                parameters,
                () -> PaginationDialect.paginateSql(pagination,
                        sql(support, safeForm, safeLayout, whereFragment, safePage, tableHint, null),
                        baseParameters, limit) + suffix);
    }

    /** SQL 与绑定使用同一词典序展开；缓存命中时只生成参数，不重新拼接 SQL。 */
    static SqlFragment cursorPredicate(NormalizedKeysetPage page, Function<String, String> identifiers,
                                       BiFunction<String, Object, Object> encoder) {
        List<Object> parameters = new ArrayList<>();
        return new SqlFragment(cursorPredicate(page, identifiers, encoder, parameters), parameters);
    }

    static String cursorSql(NormalizedKeysetPage page, Function<String, String> identifiers) {
        return cursorPredicate(page, identifiers, null, null);
    }

    static void appendCursorParameters(NormalizedKeysetPage page, BiFunction<String, Object, Object> encoder,
                                        List<Object> parameters) {
        cursorPredicate(page, null, encoder, parameters);
    }

    static String cursorPredicate(NormalizedKeysetPage page, Function<String, String> identifiers,
                                   BiFunction<String, Object, Object> encoder, List<Object> parameters) {
        if (page.firstPage()) return "";
        List<Object> values = page.positionValues();
        StringJoiner branches = identifiers == null ? null : new StringJoiner(" OR ");
        int branchCount = 0;
        for (int pivot = 0; pivot < page.sortCount(); pivot++) {
            Object pivotValue = values.get(pivot);
            if (pivotValue == null && page.nullOrder(pivot) == NullOrder.LAST) continue;
            StringJoiner terms = identifiers == null ? null : new StringJoiner(" AND ");
            for (int previous = 0; previous < pivot; previous++) {
                String field = page.field(previous);
                Object value = values.get(previous);
                if (terms != null) terms.add(identifiers.apply(field) + (value == null ? " IS NULL" : " = ?"));
                if (parameters != null && value != null) parameters.add(encoder.apply(field, value));
            }
            String field = page.field(pivot);
            if (terms != null) {
                String identifier = identifiers.apply(field);
                String comparison = identifier + (page.direction(pivot) == CursorDirection.ASC ? " > ?" : " < ?");
                terms.add(pivotValue == null ? identifier + " IS NOT NULL"
                        : page.nullable(pivot) && page.nullOrder(pivot) == NullOrder.LAST
                        ? '(' + comparison + " OR " + identifier + " IS NULL)" : comparison);
                branches.add(pivot == 0 ? terms.toString() : '(' + terms.toString() + ')');
            }
            if (parameters != null && pivotValue != null) parameters.add(encoder.apply(field, pivotValue));
            branchCount++;
        }
        if (branches == null) return "";
        if (branchCount == 0) return "1 = 0";
        return branchCount == 1 ? branches.toString() : '(' + branches.toString() + ')';
    }

    private static String sql(FormSqlRenderSupport support,
                              DynamicForm form,
                              HiddenProjectionLayout layout,
                              FormSqlRenderSupport.ConditionSql where,
                              KeysetPageNormalizer.NormalizedKeysetPage page,
                              String tableHint,
                              SqlFragment columns) {
        StringJoiner selected = new StringJoiner(", ");
        if (columns != null) selected.add(columns.sql());
        for (HiddenProjectionLayout.Projection selection : layout.selections()) {
            if (columns != null && !selection.hidden()) continue;
            String column = support.identifier(form.field(selection.field()).name());
            selected.add(selection.hidden()
                    ? column + " as " + support.identifier(selection.label()) : column);
        }
        StringBuilder sql = new StringBuilder("select ").append(selected)
                .append(" from ").append(support.identifier(form)).append(tableHint);
        String cursor = cursorSql(page, field -> support.identifier(form.field(field).name()));
        if (!where.sql().isBlank() || !cursor.isBlank()) {
            sql.append(" where ");
            if (!where.sql().isBlank() && !cursor.isBlank()) {
                sql.append('(').append(where.sql()).append(") and (").append(cursor).append(')');
            } else {
                sql.append(where.sql().isBlank() ? cursor : where.sql());
            }
        }
        return sql.append(orderBy(page, field -> support.identifier(form.field(field).name()))).toString();
    }

    static String orderBy(NormalizedKeysetPage page,
                           Function<String, String> identifiers) {
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (int index = 0; index < page.sortCount(); index++) {
            String identifier = identifiers.apply(page.field(index));
            if (page.nullable(index)) {
                int nullRank = page.nullOrder(index) == NullOrder.FIRST ? 0 : 1;
                order.add("case when " + identifier + " is null then " + nullRank
                                  + " else " + (1 - nullRank) + " end asc");
            }
            order.add(identifier + " " + page.direction(index).name().toLowerCase(Locale.ROOT));
        }
        return order.toString();
    }

    private static String sortShape(FormSqlRenderSupport support,
                                    DynamicForm form,
                                    KeysetPageNormalizer.NormalizedKeysetPage page) {
        StringJoiner shape = new StringJoiner(";");
        for (int index = 0; index < page.sorts().size(); index++) {
            KeysetSort sort = page.sorts().get(index);
            support.requireStableOffsetTimeOrdering(form.field(sort.field()));
            shape.add(sort.field() + ":" + sort.direction() + ":" + sort.nullOrder()
                              + ":" + page.nullable(index));
        }
        return shape.toString();
    }

    private static String pageShape(KeysetPageNormalizer.NormalizedKeysetPage page) {
        if (page.firstPage()) {
            return "keyset:first";
        }
        StringBuilder shape = new StringBuilder("keyset:after:");
        for (Object value : page.positionValues()) {
            shape.append(value == null ? 'n' : 'v');
        }
        return shape.toString();
    }
}
