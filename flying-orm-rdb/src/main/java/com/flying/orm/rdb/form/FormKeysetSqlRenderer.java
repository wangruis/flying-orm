package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.ReadLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

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
        SqlFragment cursor = KeysetPredicateRenderer.render(
                safePage,
                field -> support.identifier(support.field(safeForm, field).name()),
                (field, value) -> support.writeValue(support.field(safeForm, field), value));
        List<Object> baseParameters = new ArrayList<>(
                whereFragment.parameters().size() + cursor.parameters().size());
        baseParameters.addAll(whereFragment.parameters());
        baseParameters.addAll(cursor.parameters());
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
                () -> pagination.paginate(
                        sql(support, safeForm, safeLayout, whereFragment, cursor, safePage, tableHint),
                        baseParameters, limit).sql() + suffix);
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
