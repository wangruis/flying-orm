package com.flying.orm.core.page;

import com.flying.orm.core.expression.ValueExpression;

import java.util.Objects;

/**
 * 后端排序表达式与方向；仅描述顺序，不生成 SQL，也不添加返回列。
 * @param expression 字段或计算表达式
 * @param direction 排序方向，null 使用升序
 * @author wangr
 * @version v4.1.2
 */
public record ExpressionSort(ValueExpression expression, PageSort.Direction direction) {
    public ExpressionSort {
        expression = Objects.requireNonNull(expression, "sort expression must not be null");
        direction = direction == null ? PageSort.Direction.ASC : direction;
    }
}
