package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorDirection;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.sql.render.SqlFragment;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * 渲染支持 nullable 与混合方向的可移植 keyset 谓词。
 *
 * <p>算法展开标准词典序 OR 分支，不使用只有部分数据库支持的 tuple 比较。null rank 由显式
 * NULLS FIRST/LAST 决定；参数始终交给字段 codec 回调转换，不把值拼进 SQL。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class KeysetPredicateRenderer {

    private KeysetPredicateRenderer() {
    }

    static SqlFragment render(KeysetPageNormalizer.NormalizedKeysetPage page,
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
}
