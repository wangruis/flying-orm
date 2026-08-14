package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 把多读的一行转换成稳定游标结果，JDBC 和 R2DBC 共用完全相同的游标推进规则。 */
final class FormCursorResults {

    private FormCursorResults() {
    }

    static CursorPageResult<DynamicRow> from(List<DynamicRow> fetched, CursorPageQuery page) {
        List<DynamicRow> safeRows = Objects.requireNonNull(fetched, "cursor rows must not be null");
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        boolean hasMore = safeRows.size() > safePage.size();
        List<DynamicRow> rows = hasMore
                ? List.copyOf(safeRows.subList(0, safePage.size()))
                : List.copyOf(safeRows);
        if (!hasMore) {
            return new CursorPageResult<>(rows, List.of(), false);
        }
        DynamicRow last = rows.getLast();
        List<Object> next = safePage.sorts().stream().map(sort -> cursorValue(last, sort)).toList();
        return new CursorPageResult<>(rows, next, true);
    }

    private static Object cursorValue(Map<String, Object> row, CursorSort sort) {
        if (row.containsKey(sort.field())) {
            return Objects.requireNonNull(
                    row.get(sort.field()), "cursor sort value must not be null: " + sort.field());
        }
        return row.entrySet().stream()
                  .filter(entry -> entry.getKey().equalsIgnoreCase(sort.field()))
                  .map(Map.Entry::getValue)
                  .map(value -> Objects.requireNonNull(
                          value, "cursor sort value must not be null: " + sort.field()))
                  .findFirst()
                  .orElseThrow(() -> new IllegalStateException(
                          "cursor sort field is missing from query result: " + sort.field()));
    }
}
