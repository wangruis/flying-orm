package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.dialect.PaginationDialect;
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
        return select(support, pagination, form, where, layout, page, null, null);
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
                      Objects.requireNonNull(lock, "read lock must not be null"));
    }

    private static SqlRequest select(FormSqlRenderSupport support,
                                     PaginationDialect pagination,
                                     DynamicForm form,
                                     ConditionGroup where,
                                     HiddenProjectionLayout layout,
                                     KeysetPageNormalizer.NormalizedKeysetPage page,
                                     LockingReadDialect dialect,
                                     ReadLock lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        HiddenProjectionLayout safeLayout = Objects.requireNonNull(
                layout, "keyset projection layout must not be null");
        KeysetPageNormalizer.NormalizedKeysetPage safePage = Objects.requireNonNull(
                page, "normalized keyset page must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        SqlFragment cursor = cursorPredicate(
                safePage,
                field -> support.identifier(support.field(safeForm, field).name()),
                (field, value) -> support.writeValue(support.field(safeForm, field), value));
        List<Object> whereParameters = OwnedBindableValues.ownedValues(whereFragment.parameters());
        List<Object> cursorParameters = OwnedBindableValues.ownedValues(cursor.parameters());
        List<Object> baseParameters = new ArrayList<>(whereParameters.size() + cursorParameters.size());
        baseParameters.addAll(whereParameters);
        baseParameters.addAll(cursorParameters);
        PageQuery limit = PageQuery.of(1, safePage.size() + 1);
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
        String operation = locking
                ? "select-keyset-locking-" + FormQuerySqlRenderer.lockShape(lock)
                : "select-keyset";
        return support.request(
                operation, safeForm, selectionShape, whereFragment, "", sortShape, pageShape,
                parameters,
                () -> PaginationDialect.paginateSql(pagination,
                        sql(support, safeForm, safeLayout, whereFragment, cursor, safePage, tableHint),
                        baseParameters, limit) + suffix);
    }

    /**
     * 展开支持 nullable 与混合方向的可移植词典序谓词。
     *
     * <p>参数按每个 OR 分支中的占位符顺序加入，且始终交给字段 codec 回调转换。</p>
     */
    static SqlFragment cursorPredicate(
            KeysetPageNormalizer.NormalizedKeysetPage page,
            Function<String, String> identifierRenderer,
            BiFunction<String, Object, Object> parameterEncoder) {
        KeysetPageNormalizer.NormalizedKeysetPage safePage = Objects.requireNonNull(
                page, "normalized keyset page must not be null");
        Function<String, String> identifiers = Objects.requireNonNull(
                identifierRenderer, "keyset identifier renderer must not be null");
        BiFunction<String, Object, Object> encoder = Objects.requireNonNull(
                parameterEncoder, "keyset parameter encoder must not be null");
        if (safePage.firstPage()) {
            return new SqlFragment("", List.of());
        }

        List<KeysetSort> sorts = safePage.sorts();
        List<Object> cursor = safePage.positionValues();
        List<String> branches = new ArrayList<>(sorts.size());
        List<Object> parameters = new ArrayList<>((sorts.size() * (sorts.size() + 1)) / 2);
        for (int pivot = 0; pivot < sorts.size(); pivot++) {
            KeysetSort pivotSort = sorts.get(pivot);
            Object pivotValue = cursor.get(pivot);
            String after = afterExpression(
                    pivotSort, pivotValue, safePage.nullable(pivot),
                    identifiers.apply(pivotSort.field()));
            if (after == null) {
                // NULLS LAST 的 null 已在本排序键末尾；仍保留后续 pivot，让 tie-breaker 继续当前 null 分组。
                continue;
            }

            List<String> terms = new ArrayList<>(pivot + 1);
            for (int previous = 0; previous < pivot; previous++) {
                KeysetSort sort = sorts.get(previous);
                Object value = cursor.get(previous);
                String identifier = identifiers.apply(sort.field());
                if (value == null) {
                    terms.add(identifier + " IS NULL");
                } else {
                    terms.add(identifier + " = ?");
                    parameters.add(encoder.apply(sort.field(), value));
                }
            }
            terms.add(after);
            if (pivotValue != null) {
                parameters.add(encoder.apply(pivotSort.field(), pivotValue));
            }
            String branch = String.join(" AND ", terms);
            branches.add(terms.size() == 1 ? branch : '(' + branch + ')');
        }

        if (branches.isEmpty()) {
            return new SqlFragment("1 = 0", List.of());
        }
        String sql = branches.size() == 1
                ? branches.getFirst()
                : '(' + String.join(" OR ", branches) + ')';
        return new SqlFragment(sql, parameters);
    }

    private static String afterExpression(KeysetSort sort,
                                          Object cursorValue,
                                          boolean nullable,
                                          String identifier) {
        String safeIdentifier = Objects.requireNonNull(
                identifier, "rendered keyset identifier must not be null");
        if (cursorValue == null) {
            return sort.nullOrder() == NullOrder.FIRST
                    ? safeIdentifier + " IS NOT NULL" : null;
        }
        String comparison = safeIdentifier
                + (sort.direction() == CursorDirection.ASC ? " > ?" : " < ?");
        return nullable && sort.nullOrder() == NullOrder.LAST
                ? '(' + comparison + " OR " + safeIdentifier + " IS NULL)"
                : comparison;
    }

    private static String sql(FormSqlRenderSupport support,
                              DynamicForm form,
                              HiddenProjectionLayout layout,
                              FormSqlRenderSupport.ConditionSql where,
                              SqlFragment cursor,
                              KeysetPageNormalizer.NormalizedKeysetPage page,
                              String tableHint) {
        StringJoiner selected = new StringJoiner(", ");
        for (HiddenProjectionLayout.Projection selection : layout.selections()) {
            String column = support.identifier(support.field(form, selection.field()).name());
            selected.add(selection.hidden()
                    ? column + " as " + support.identifier(selection.label()) : column);
        }
        StringBuilder sql = new StringBuilder("select ").append(selected)
                .append(" from ").append(support.identifier(form)).append(tableHint);
        if (!where.sql().isBlank() || !cursor.sql().isBlank()) {
            sql.append(" where ");
            if (!where.sql().isBlank() && !cursor.sql().isBlank()) {
                sql.append('(').append(where.sql()).append(") and (").append(cursor.sql()).append(')');
            } else {
                sql.append(where.sql().isBlank() ? cursor.sql() : where.sql());
            }
        }
        return sql.append(orderBy(support, form, page)).toString();
    }

    private static String orderBy(FormSqlRenderSupport support,
                                  DynamicForm form,
                                  KeysetPageNormalizer.NormalizedKeysetPage page) {
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (int index = 0; index < page.sorts().size(); index++) {
            KeysetSort sort = page.sorts().get(index);
            DynamicField field = support.field(form, sort.field());
            String identifier = support.identifier(field.name());
            if (page.nullable(index)) {
                int nullRank = sort.nullOrder() == NullOrder.FIRST ? 0 : 1;
                order.add("case when " + identifier + " is null then " + nullRank
                                  + " else " + (1 - nullRank) + " end asc");
            }
            order.add(identifier + " " + sort.direction().name().toLowerCase(Locale.ROOT));
        }
        return order.toString();
    }

    private static String sortShape(FormSqlRenderSupport support,
                                    DynamicForm form,
                                    KeysetPageNormalizer.NormalizedKeysetPage page) {
        StringJoiner shape = new StringJoiner(";");
        for (int index = 0; index < page.sorts().size(); index++) {
            KeysetSort sort = page.sorts().get(index);
            support.requireStableOffsetTimeOrdering(support.field(form, sort.field()));
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
