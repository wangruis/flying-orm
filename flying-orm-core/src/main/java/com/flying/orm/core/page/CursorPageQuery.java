package com.flying.orm.core.page;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 基于稳定排序值的游标分页请求。
 *
 * <p>cursor 为空表示第一页；非空值禁止包含 {@code null}。游标值数量由关系型模块结合实体主键自动补齐后的
 * 稳定排序进行校验，而不是在这个框架无关值对象中只按调用方显式排序过早拒绝。这里不编码字符串令牌，
 * 上层可以按自己的签名、加密或 JSON 规则传输游标。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record CursorPageQuery(int size, List<CursorSort> sorts, List<Object> cursor) {

    public CursorPageQuery {
        if (size < 1 || size >= PageQuery.MAX_SIZE) {
            throw new IllegalArgumentException("cursor page size must be between 1 and " + (PageQuery.MAX_SIZE - 1));
        }
        sorts = List.copyOf(Objects.requireNonNull(sorts, "cursor sorts must not be null"));
        cursor = snapshotCursor(Objects.requireNonNull(cursor, "cursor values must not be null"));
        if (sorts.isEmpty()) {
            throw new IllegalArgumentException("cursor pagination requires at least one sort field");
        }
    }

    public static CursorPageQuery first(int size, CursorSort... sorts) {
        return new CursorPageQuery(size, Arrays.asList(sorts), List.of());
    }

    public static CursorPageQuery after(int size, List<?> cursor, CursorSort... sorts) {
        return new CursorPageQuery(size, Arrays.asList(sorts), cursorValues(cursor));
    }

    public boolean firstPage() {
        return cursor.isEmpty();
    }

    /**
     * 返回游标值的只读快照；数组可达图每次返回独立副本，避免影响后续 SQL 参数绑定。
     *
     * @return 当前游标的不可变快照
     */
    @Override
    public List<Object> cursor() {
        return BindableValueSnapshots.logicalValues(cursor);
    }

    static List<Object> snapshotCursor(List<?> values) {
        List<?> safeValues = Objects.requireNonNull(values, "cursor values must not be null");
        List<Object> snapshot = BindableValueSnapshots.logicalValues(safeValues);
        snapshot.forEach(value -> Objects.requireNonNull(value, "cursor value must not be null"));
        return snapshot;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> cursorValues(List<?> values) {
        return (List<Object>) Objects.requireNonNull(values, "cursor values must not be null");
    }
}
