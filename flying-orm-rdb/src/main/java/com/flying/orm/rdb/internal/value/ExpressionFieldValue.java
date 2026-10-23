package com.flying.orm.rdb.internal.value;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * 表达式常量的字段语义；不是数据库字段读取，也不提前执行 codec。
 * @author wangr
 * @version v4.1.2
 */
public record ExpressionFieldValue(DynamicField field, Object value) {
    public ExpressionFieldValue {
        value = BindableValueSnapshots.logicalValue(value);
    }

    /** 为优先级比较值保留各自来源字段；复用节点且不提前运行 codec。 */
    public static ValueExpression bindPriorities(ValueExpression expression, Function<ValueExpression.Field, DynamicField> fields) {
        return mapPriorityValues(expression, field -> {
            DynamicField source = fields.apply(field);
            return source == null ? null : value -> new ExpressionFieldValue(source, value);
        });
    }

    /** 按优先比较的字段映射逻辑值；null mapper 保留原值，已绑定字段值不重复转换。 */
    public static ValueExpression mapPriorityValues(ValueExpression expression,
                                                    Function<ValueExpression.Field, UnaryOperator<Object>> mappers) {
        return transform(expression, null, mappers);
    }

    /** 来源绑定与优先值绑定共用非递归遍历；不变分支保留原节点。 */
    public static ValueExpression mapFields(ValueExpression expression,
                                            Function<ValueExpression.Field, ValueExpression> fields) {
        return transform(expression, fields, null);
    }

    private static ValueExpression transform(ValueExpression expression,
            Function<ValueExpression.Field, ValueExpression> fields,
            Function<ValueExpression.Field, UnaryOperator<Object>> mappers) {
        if (expression instanceof ValueExpression.Field field && fields != null) return fields.apply(field);
        if (expression instanceof ValueExpression.Field || expression instanceof ValueExpression.Value
                || expression instanceof ValueExpression.IntegerConstant) return expression;
        Map<ValueExpression, ValueExpression> resolved = new IdentityHashMap<>();
        Deque<ExpressionVisit> pending = new ArrayDeque<>();
        pending.push(new ExpressionVisit(expression, false));
        while (!pending.isEmpty()) {
            ExpressionVisit visit = pending.pop();
            ValueExpression node = visit.expression();
            if (resolved.containsKey(node)) continue;
            if (node instanceof ValueExpression.Field || node instanceof ValueExpression.Value
                    || node instanceof ValueExpression.IntegerConstant) {
                resolved.put(node, node instanceof ValueExpression.Field field && fields != null ? fields.apply(field) : node);
            } else if (!visit.expanded()) {
                pending.push(new ExpressionVisit(node, true));
                if (node instanceof ValueExpression.Call call) {
                    for (int index = call.arguments().size() - 1; index >= 0; index--) {
                        pending.push(new ExpressionVisit(call.arguments().get(index), false));
                    }
                } else {
                    ValueExpression.Arithmetic arithmetic = (ValueExpression.Arithmetic) node;
                    pending.push(new ExpressionVisit(arithmetic.right(), false));
                    pending.push(new ExpressionVisit(arithmetic.left(), false));
                }
            } else if (node instanceof ValueExpression.Arithmetic arithmetic) {
                ValueExpression left = resolved.get(arithmetic.left());
                ValueExpression right = resolved.get(arithmetic.right());
                resolved.put(node, left == arithmetic.left() && right == arithmetic.right() ? node
                        : new ValueExpression.Arithmetic(arithmetic.operator(), left, right));
            } else {
                ValueExpression.Call call = (ValueExpression.Call) node;
                List<ValueExpression> arguments = null;
                for (int index = 0; index < call.arguments().size(); index++) {
                    ValueExpression argument = call.arguments().get(index);
                    ValueExpression bound = resolved.get(argument);
                    if (argument == bound) continue;
                    if (arguments == null) arguments = new ArrayList<>(call.arguments());
                    arguments.set(index, bound);
                }
                ValueExpression.Call mapped = arguments == null ? call : new ValueExpression.Call(call.id(), arguments);
                resolved.put(node, mappers == null ? mapped : mapPriorityCall(mapped, mappers));
            }
        }
        return resolved.get(expression);
    }

    private static ValueExpression mapPriorityCall(ValueExpression.Call call,
                                                    Function<ValueExpression.Field, UnaryOperator<Object>> mappers) {
        boolean paired = call.id().equals("priority-first-match") || call.id().equals("priority-last-match");
        boolean mixed = call.id().equals("priority-mixed-match");
        if ((!paired && !mixed && !call.id().equals("priority")) || call.arguments().size() < 2) return call;
        int firstValue = mixed ? 2 : 1;
        int stride = mixed ? 3 : paired ? 2 : 1;
        List<ValueExpression> arguments = null;
        UnaryOperator<Object> mapper = null;
        for (int index = firstValue; index < call.arguments().size(); index += stride) {
            if (stride > 1 || index == firstValue) {
                if (!(call.arguments().get(stride > 1 ? index - 1 : 0) instanceof ValueExpression.Field field)) {
                    if (stride > 1) continue;
                    return call;
                }
                mapper = mappers.apply(field);
                if (mapper == null) {
                    if (stride > 1) continue;
                    return call;
                }
            }
            if (!(call.arguments().get(index) instanceof ValueExpression.Value value)) continue;
            Object logical = value.value();
            if (logical instanceof ExpressionFieldValue) continue;
            Object mapped = mapper.apply(logical);
            if (mapped == logical) continue;
            if (arguments == null) arguments = new ArrayList<>(call.arguments());
            arguments.set(index, ValueExpression.value(mapped));
        }
        return arguments == null ? call : new ValueExpression.Call(call.id(), arguments);
    }

    private record ExpressionVisit(ValueExpression expression, boolean expanded) { }

    @Override
    public Object value() {
        return BindableValueSnapshots.logicalValue(value);
    }
}
