package com.flying.orm.core.page;

import java.util.Objects;

/**
 * 游标分页的一项稳定排序。生产查询应把唯一键放在最后，避免相同业务排序值造成漏行或重复。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record CursorSort(String field, CursorDirection direction) {

    public CursorSort {
        field = Objects.requireNonNull(field, "cursor sort field must not be null").trim();
        if (field.isEmpty()) {
            throw new IllegalArgumentException("cursor sort field must not be blank");
        }
        direction = Objects.requireNonNull(direction, "cursor sort direction must not be null");
    }

    public static CursorSort asc(String field) {
        return new CursorSort(field, CursorDirection.ASC);
    }

    public static CursorSort desc(String field) {
        return new CursorSort(field, CursorDirection.DESC);
    }
}
