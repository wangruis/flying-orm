package com.flying.orm.core.join;

import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.expression.ValueExpression;

/**
 * JOIN 查询中的结构化排序项。
 *
 * @param field 排序字段
 * @param direction 排序方向
 * @param expression 可选计算排序，与 field 二选一
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public record JoinOrder(JoinFieldRef field, PageSort.Direction direction, ValueExpression expression) {

    public JoinOrder(JoinFieldRef field, PageSort.Direction direction) {
        this(field, direction, null);
    }

    /** 空方向按升序处理，与普通分页排序保持一致。 */
    public JoinOrder {
        if ((field == null) == (expression == null)) {
            throw new IllegalArgumentException("join order requires either a field or an expression");
        }
        direction = direction == null ? PageSort.Direction.ASC : direction;
    }
}
