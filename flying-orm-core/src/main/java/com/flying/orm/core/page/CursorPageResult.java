package com.flying.orm.core.page;

import java.util.List;
import java.util.Objects;

/**
 * 游标分页结果。nextCursor 已保持请求排序字段的顺序；有下一页时必须可用于构造后续请求，
 * 没有下一页时为空。
 *
 * @param <T> 结果元素类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record CursorPageResult<T>(List<T> rows, List<Object> nextCursor, boolean hasMore) {

    public CursorPageResult {
        rows = List.copyOf(Objects.requireNonNull(rows, "cursor page rows must not be null"));
        nextCursor = CursorPageQuery.snapshotCursor(Objects.requireNonNull(nextCursor,
                                                                            "next cursor must not be null"));
        if (hasMore && nextCursor.isEmpty()) {
            throw new IllegalArgumentException("next cursor must not be empty when cursor page has more rows");
        }
        if (!hasMore && !nextCursor.isEmpty()) {
            throw new IllegalArgumentException("next cursor must be empty when cursor page has no more rows");
        }
    }

    /**
     * 返回下一页游标的只读快照；数组元素每次返回独立数组图副本。
     *
     * @return 下一页游标的不可变快照
     */
    @Override
    public List<Object> nextCursor() {
        return CursorPageQuery.snapshotCursor(nextCursor);
    }
}
