package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
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
 * 动态表单查询 SQL 的内部实现。
 *
 * <p>这里集中处理投影、排序和 offset/cursor 分页。条件 AST、字段识别、计划缓存和参数规则
 * 均由共享 support 提供，避免查询与写入在边界处出现两套语义。实例只在门面构造时创建一次，
 * 可以安全地被并发复用。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormQuerySqlRenderer {

    private final FormSqlRenderSupport support;
    private final PaginationDialect paginationDialect;

    FormQuerySqlRenderer(FormSqlRenderSupport support, PaginationDialect paginationDialect) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
        this.paginationDialect = Objects.requireNonNull(paginationDialect, "pagination dialect must not be null");
    }

    SqlRequest select(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<String> fields = support.fieldNames(safeForm.fields());
        return support.request("select", safeForm, fields, whereFragment, "", "", "",
                               whereFragment.parameters(),
                               () -> selectSql(safeForm, fields, whereFragment, null));
    }

    SqlRequest select(DynamicForm form, ConditionGroup where, PageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<String> fields = support.fieldNames(safeForm.fields());
        return selectOffset("select-page", safeForm, fields, whereFragment, safePage);
    }

    /** 内部受保护字段查询显式给出业务可见列，隐藏盲索引列永不进入结果。 */
    SqlRequest selectPhysical(DynamicForm form,
                              List<String> visibleFields,
                              ConditionGroup where,
                              PageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<String> fields = Objects.requireNonNull(visibleFields, "visible fields must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        return selectOffset("select-page-protected", safeForm, fields, whereFragment, safePage);
    }

    private SqlRequest selectOffset(String operation, DynamicForm form, List<String> fields,
                                    FormSqlRenderSupport.ConditionSql where, PageQuery page) {
        String sortShape = pageSortShape(form, page.sorts());
        List<Object> parameters = paginationDialect.paginationParameters(where.parameters(), page);
        return support.request(operation, form, fields, where, "", sortShape, "offset", parameters,
                               () -> PaginationDialect.paginateSql(paginationDialect,
                               selectSql(form, fields, where, null) + orderBy(form, page.sorts()),
                               where.parameters(), page));
    }

    SqlRequest selectOrdered(DynamicForm form, ConditionGroup where, List<PageSort> sorts) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<PageSort> safeSorts = Objects.requireNonNull(sorts, "page sorts must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<String> fields = support.fieldNames(safeForm.fields());
        String sortShape = pageSortShape(safeForm, safeSorts);
        return support.request("select-ordered", safeForm, fields, whereFragment, "", sortShape, "",
                               whereFragment.parameters(),
                               () -> selectSql(safeForm, fields, whereFragment, null) + orderBy(safeForm, safeSorts));
    }

    SqlRequest selectProjected(DynamicForm form,
                               ConditionGroup where,
                               List<String> projections,
                               List<String> groups,
                               List<PageSort> sorts) {
        return selectProjected(form, where, projections, groups, sorts, false, null, null);
    }

    /** 只接收方言发布的受控锁模型，调用方不能把任意 hint 或后缀带进 SQL。 */
    SqlRequest selectProjectedLocking(DynamicForm form,
                                      ConditionGroup where,
                                      List<String> projections,
                                      List<String> groups,
                                      List<PageSort> sorts,
                                      LockingReadDialect dialect,
                                      ReadLock lock) {
        return selectProjected(form, where, projections, groups, sorts, true, dialect, lock);
    }

    private SqlRequest selectProjected(DynamicForm form,
                                       ConditionGroup where,
                                       List<String> projections,
                                       List<String> groups,
                                       List<PageSort> sorts,
                                       boolean locking,
                                       LockingReadDialect dialect,
                                       ReadLock lock) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<String> safeProjections = Objects.requireNonNull(projections, "query projections must not be null");
        List<String> safeGroups = Objects.requireNonNull(groups, "query groups must not be null");
        List<PageSort> safeSorts = Objects.requireNonNull(sorts, "query sorts must not be null");
        if (safeProjections.isEmpty()) {
            throw new IllegalArgumentException("projected query must select at least one entity field");
        }
        List<String> selectedFields = safeProjections.stream().map(safeForm::field).map(DynamicField::name).toList();
        List<String> groupedFields = safeGroups.stream().map(safeForm::field).map(DynamicField::name).toList();
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        String groupShape = String.join(",", groupedFields);
        String sortShape = pageSortShape(safeForm, safeSorts);
        final String operation;
        final String tableHint;
        final String suffix;
        if (locking) {
            LockingReadDialect safeDialect = Objects.requireNonNull(
                    dialect, "locking read dialect must not be null");
            ReadLock safeLock = Objects.requireNonNull(lock, "read lock must not be null");
            tableHint = safeDialect.tableHint(safeLock);
            suffix = safeDialect.suffix(safeLock);
            operation = "select-locking-" + lockShape(safeLock);
        } else {
            tableHint = "";
            suffix = "";
            operation = "select-projected";
        }
        return support.request(operation, safeForm, selectedFields,
                               whereFragment, groupShape, sortShape, "",
                               whereFragment.parameters(), () -> {
                           StringBuilder sql = new StringBuilder("select ")
                                   .append(support.identifierColumns(selectedFields))
                                   .append(" from ").append(support.identifier(safeForm))
                                   .append(tableHint);
                           appendWhere(sql, whereFragment);
                           if (!groupedFields.isEmpty()) {
                               sql.append(" group by ").append(support.identifierColumns(groupedFields));
                           }
                           return sql.append(orderBy(safeForm, safeSorts)).append(suffix).toString();
                       });
    }

    SqlRequest select(DynamicForm form, ConditionGroup where, CursorPageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return select(safeForm, where, CursorPageNormalizer.normalize(
                safeForm, Objects.requireNonNull(page, "cursor page query must not be null")));
    }

    SqlRequest select(DynamicForm form,
                      ConditionGroup where,
                      CursorPageNormalizer.NormalizedCursorPage page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        CursorPageNormalizer.NormalizedCursorPage safePage = Objects.requireNonNull(
                page, "normalized cursor page must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        List<String> fields = support.fieldNames(safeForm.fields());
        return selectCursor("select-cursor", safeForm, fields, whereFragment, safePage);
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

    /** 内部受保护游标查询只投影业务列，排序仍由物理表单执行本地校验。 */
    SqlRequest selectPhysical(DynamicForm form,
                              List<String> visibleFields,
                              ConditionGroup where,
                              CursorPageQuery page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return selectPhysical(safeForm, visibleFields, where, CursorPageNormalizer.normalize(
                safeForm, Objects.requireNonNull(page, "cursor page query must not be null")));
    }

    SqlRequest selectPhysical(DynamicForm form,
                              List<String> visibleFields,
                              ConditionGroup where,
                              CursorPageNormalizer.NormalizedCursorPage page) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        List<String> fields = Objects.requireNonNull(visibleFields, "visible fields must not be null");
        CursorPageNormalizer.NormalizedCursorPage safePage = Objects.requireNonNull(
                page, "normalized cursor page must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        return selectCursor("select-cursor-protected", safeForm, fields, whereFragment, safePage);
    }

    private SqlRequest selectCursor(String operation, DynamicForm form, List<String> fields,
                                    FormSqlRenderSupport.ConditionSql where,
                                    CursorPageNormalizer.NormalizedCursorPage page) {
        String sortShape = cursorSortShape(form, page.sorts());
        List<Object> baseParameters = new ArrayList<>(where.parameters());
        if (!page.firstPage()) {
            addCursorParameters(form, page, baseParameters);
        }
        PageQuery limit = PageQuery.of(1, Math.addExact(page.size(), 1));
        List<Object> parameters = paginationDialect.paginationParameters(baseParameters, limit);
        String pageShape = page.firstPage() ? "cursor:first" : "cursor:after:" + page.sorts().size();
        return support.request(operation, form, fields, where, "", sortShape, pageShape,
                               parameters, () -> PaginationDialect.paginateSql(paginationDialect,
                               selectSql(form, fields, where, page), baseParameters, limit));
    }

    SqlRequest count(DynamicForm form, ConditionGroup where) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        FormSqlRenderSupport.ConditionSql whereFragment = support.condition(safeForm, where);
        return support.request("count", safeForm, List.of(), whereFragment, "", "", "",
                               whereFragment.parameters(), () -> {
                           String count = "sqlserver".equals(support.dialectName) ? "count_big" : "count";
                           StringBuilder sql = new StringBuilder("select ").append(count).append("(*) as total from ")
                                   .append(support.identifier(safeForm));
                           appendWhere(sql, whereFragment);
                           return sql.toString();
                       });
    }

    private String selectSql(DynamicForm form,
                             List<String> fields,
                             FormSqlRenderSupport.ConditionSql where,
                             CursorPageNormalizer.NormalizedCursorPage cursorPage) {
        String selected = fields.isEmpty() ? "*" : support.identifierColumns(fields);
        StringBuilder sql = new StringBuilder("select ").append(selected)
                .append(" from ").append(support.identifier(form));
        if (cursorPage == null) {
            appendWhere(sql, where);
            return sql.toString();
        }
        if (cursorPage.firstPage()) {
            appendWhere(sql, where);
        } else {
            if (where.sql().isBlank()) {
                sql.append(" where (");
            } else {
                sql.append(" where (").append(where.sql()).append(") and (");
            }
            sql.append(cursorWhere(form, cursorPage)).append(')');
        }
        StringJoiner order = new StringJoiner(", ");
        for (CursorSort sort : cursorPage.sorts()) {
            DynamicField field = support.field(form, sort.field());
            order.add(support.identifier(field.name()) + " " + sort.direction().name().toLowerCase(Locale.ROOT));
        }
        return sql.append(" order by ").append(order).toString();
    }

    private String cursorWhere(DynamicForm form, CursorPageNormalizer.NormalizedCursorPage page) {
        StringJoiner alternatives = new StringJoiner(" or ");
        for (int pivot = 0; pivot < page.sorts().size(); pivot++) {
            StringJoiner terms = new StringJoiner(" and ");
            for (int prefix = 0; prefix < pivot; prefix++) {
                DynamicField prefixField = support.field(form, page.sorts().get(prefix).field());
                terms.add(support.identifier(prefixField.name()) + " = ?");
            }
            CursorSort sort = page.sorts().get(pivot);
            DynamicField pivotField = support.field(form, sort.field());
            terms.add(support.identifier(pivotField.name())
                              + (sort.direction() == CursorDirection.ASC ? " > ?" : " < ?"));
            alternatives.add(pivot == 0 ? terms.toString() : "(" + terms + ")");
        }
        return alternatives.toString();
    }

    /** 缓存形状包含完整锁语义，后续增加锁强度时也不会复用另一种锁的 SQL。 */
    static String lockShape(ReadLock lock) {
        return lock.strength() + ":" + lock.waitMode();
    }

    private void addCursorParameters(DynamicForm form,
                                     CursorPageNormalizer.NormalizedCursorPage page,
                                     List<Object> parameters) {
        List<Object> cursor = page.cursor();
        for (int pivot = 0; pivot < page.sorts().size(); pivot++) {
            for (int prefix = 0; prefix < pivot; prefix++) {
                DynamicField field = support.field(form, page.sorts().get(prefix).field());
                parameters.add(support.writeValue(field, cursor.get(prefix)));
            }
            DynamicField field = support.field(form, page.sorts().get(pivot).field());
            parameters.add(support.writeValue(field, cursor.get(pivot)));
        }
    }

    private String pageSortShape(DynamicForm form, List<PageSort> sorts) {
        StringJoiner shape = new StringJoiner(";");
        for (PageSort sort : sorts) {
            DynamicField field = support.field(form, sort.field());
            support.requireStableOffsetTimeOrdering(field);
            shape.add(field.name() + ":" + sort.direction().name());
        }
        return shape.toString();
    }

    private String cursorSortShape(DynamicForm form, List<CursorSort> sorts) {
        StringJoiner shape = new StringJoiner(";");
        for (CursorSort sort : sorts) {
            DynamicField field = support.field(form, sort.field());
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

    private static void appendWhere(StringBuilder sql, FormSqlRenderSupport.ConditionSql where) {
        if (!where.sql().isBlank()) {
            sql.append(" where ").append(where.sql());
        }
    }

}
