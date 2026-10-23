package com.flying.orm.rdb.form;



import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/** 报表结果别名的来源、值语义与排序编译；不生成额外结果列。 */
record AggregateResultTarget(List<AggregateField> sources, DynamicField result, String expression,
                             DynamicField valueField, List<Object> parameters) {
    AggregateResultTarget {
        result = Objects.requireNonNull(result, "aggregate result field must not be null");
        expression = Objects.requireNonNull(expression, "aggregate result expression must not be null");
        valueField = Objects.requireNonNull(valueField, "aggregate value source field must not be null");
    }

    void requireUse(FieldUseRequirements.Builder requirements, FieldUse use) {
        for (AggregateField source : sources) source.requireUse(requirements, use);
    }

    private static AggregateResultTarget resolve(ValueExpression.Field field, Map<String, AggregateResultTarget> targets) {
        AggregateResultTarget target = targets.get(field.identity().key());
        if (field.source() != null || target == null) {
            throw new IllegalArgumentException("aggregate ordering may reference only declared result aliases");
        }
        return target;
    }

    static List<Object> appendOrder(StringBuilder sql, List<PageSort> sorts, Map<String, ValueExpression> expressions,
                             Map<String, AggregateResultTarget> targets, FormAggregateReadSupport reads,
                             FieldUseRequirements.Builder requirements, String qualifier) {
        if (sorts.isEmpty()) return List.of();
        List<Object> parameters = expressions.isEmpty() ? List.of() : new ArrayList<>();
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (PageSort sort : sorts) {
            ValueExpression expression = expressions.get(sort.field());
            if (expression == null) {
                AggregateResultTarget target = resolve(new ValueExpression.Field(FieldIdentity.of(sort.field()), null), targets);
                reads.renderer.requireStableOffsetTimeOrdering(target.result());
                target.requireUse(requirements, FieldUse.SORT);
                order.add(reads.renderer.conditionRenderer().identifier(target.result().name()) + " " + sort.sqlKeyword());
                continue;
            }
            ValueExpression bound = ExpressionFieldValue.bindPriorities(expression, field -> resolve(field, targets).valueField());
            SqlExpressionCompiler.Operand operand = reads.renderer.compileExpression(bound, field -> {
                AggregateResultTarget target = resolve(field, targets);
                target.requireUse(requirements, FieldUse.SORT);
                SqlFragment fragment = qualifier == null ? new SqlFragment(target.expression(), target.parameters())
                        : new SqlFragment(qualifier + "." + reads.renderer.conditionRenderer().identifier(target.result().name()), List.of());
                LogicalType type = target.result().databaseType().isArray() ? LogicalType.OTHER
                        : target.result().databaseType().logicalType();
                return new SqlExpressionCompiler.Operand(fragment, type);
            });
            if (operand.type() == LogicalType.OFFSET_TIME) {
                reads.renderer.requireStableOffsetTimeOrdering(FormDataSqlRenderer.expressionField("aggregate_order", operand.type()));
            }
            order.add(operand.fragment().sql() + " " + sort.sqlKeyword());
            parameters.addAll(operand.fragment().parameters());
        }
        sql.append(order);
        return parameters;
    }
}
