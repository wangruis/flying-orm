package com.flying.orm.core.join;

import com.flying.orm.core.page.PageSort;

import java.util.Objects;

/**
 * JOIN 查询中的结构化排序项。
 *
 * @param field 排序字段
 * @param direction 排序方向
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinOrder(JoinFieldRef field, PageSort.Direction direction) {

    /** 空方向按升序处理，与普通分页排序保持一致。 */
    public JoinOrder {
        field = Objects.requireNonNull(field, "join order field must not be null");
        direction = direction == null ? PageSort.Direction.ASC : direction;
    }
}
