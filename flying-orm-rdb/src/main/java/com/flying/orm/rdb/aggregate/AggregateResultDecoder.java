package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.rdb.result.DynamicRow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 按 planner 的固定布局把 JDBC/R2DBC 动态行解码成类型化聚合行。
 *
 * <p>结果集内所有行共享同一 {@link AggregateRowLayout}。驱动对 COUNT、SUM/AVG 和
 * MIN/MAX 的返回差异在此收敛，客户端与同步/响应式执行器不再各自转换。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class AggregateResultDecoder {

    private final FormAggregatePlanner.Plan plan;

    public AggregateResultDecoder(FormAggregatePlanner.Plan plan) {
        this.plan = Objects.requireNonNull(plan, "aggregate plan must not be null");
    }

    /** 解码一行，并拒绝列数量与共享布局不一致的执行器结果。 */
    public AggregateRow decode(DynamicRow row) {
        DynamicRow safeRow = Objects.requireNonNull(row, "aggregate result row must not be null");
        if (safeRow.columnCount() != plan.layout().size()) {
            throw new IllegalArgumentException(
                    "aggregate result column count does not match the planned layout");
        }
        List<Object> values = new ArrayList<>(plan.layout().size());
        int index = 0;
        for (DynamicField field : plan.groupFields()) {
            Object decoded = plan.reads().decode(field, safeRow.value(index++));
            values.add(visibleGroupValue(field, decoded));
        }
        for (int aggregateIndex = 0;
             aggregateIndex < plan.aggregateFields().size();
             aggregateIndex++) {
            DynamicField field = plan.aggregateFields().get(aggregateIndex);
            AggregateExpression<?> expression = plan.layout().aggregates().get(aggregateIndex);
            values.add(decodeAggregate(expression, field, safeRow.value(index++)));
        }
        return AggregateRow.of(plan.layout(), values);
    }

    private Object visibleGroupValue(DynamicField field, Object value) {
        FieldVisibility visibility = plan.fieldUse().visibility(field.name());
        if (visibility == FieldVisibility.HIDDEN) {
            return null;
        }
        requireMaskDefinition(field, visibility);
        SensitiveDisplayMode displayMode = visibility == FieldVisibility.MASKED
                ? SensitiveDisplayMode.MASKED : plan.displayMode();
        return plan.reads().maskGroupValue(plan.form(), field, value, displayMode);
    }

    private Object decodeAggregate(AggregateExpression<?> expression,
                                   DynamicField field,
                                   Object value) {
        FieldVisibility visibility = plan.fieldUse().visibility(field.name());
        if (visibility == FieldVisibility.HIDDEN) {
            return null;
        }
        requireMaskDefinition(field, visibility);
        if (value == null) {
            return null;
        }
        Object decoded = switch (expression.function()) {
            case COUNT, COUNT_DISTINCT -> exactLong(value);
            case SUM, AVG -> decimal(value);
            case MIN, MAX -> plan.reads().decode(field, value, expression.javaType());
        };
        if (expression.function() == AggregateFunction.MIN || expression.function() == AggregateFunction.MAX) {
            SensitiveDisplayMode displayMode = visibility == FieldVisibility.MASKED
                    ? SensitiveDisplayMode.MASKED : plan.displayMode();
            return plan.reads().maskGroupValue(plan.form(), field, decoded, displayMode);
        }
        return visibility == FieldVisibility.MASKED
                ? plan.reads().maskGroupValue(
                        plan.form(), field, decoded, SensitiveDisplayMode.MASKED)
                : decoded;
    }

    private void requireMaskDefinition(DynamicField field, FieldVisibility visibility) {
        if (visibility != FieldVisibility.MASKED
                || plan.form().protections().masked(field.name()).isPresent()) {
            return;
        }
        throw new ScopeAccessException(
                ScopeErrorCode.FIELD_NOT_READABLE,
                plan.form().id(),
                field.name(),
                "field [" + field.name() + "] is MASKED but has no masking definition");
    }

    private static Long exactLong(Object value) {
        if (value instanceof BigInteger integer) {
            try {
                return integer.longValueExact();
            } catch (ArithmeticException error) {
                throw invalidCount(error);
            }
        }
        if (value instanceof BigDecimal decimal) {
            try {
                return decimal.longValueExact();
            } catch (ArithmeticException error) {
                throw invalidCount(error);
            }
        }
        if (value instanceof Number number) {
            try {
                return new BigDecimal(number.toString()).longValueExact();
            } catch (ArithmeticException | NumberFormatException error) {
                throw invalidCount(error);
            }
        }
        if (value instanceof CharSequence text) {
            try {
                return Long.parseLong(text.toString());
            } catch (NumberFormatException error) {
                throw invalidCount(error);
            }
        }
        throw invalidCount(null);
    }

    private static BigDecimal decimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (value instanceof Number number) {
            try {
                return new BigDecimal(number.toString());
            } catch (NumberFormatException error) {
                throw invalidDecimal(error);
            }
        }
        if (value instanceof CharSequence text) {
            try {
                return new BigDecimal(text.toString());
            } catch (NumberFormatException error) {
                throw invalidDecimal(error);
            }
        }
        throw invalidDecimal(null);
    }

    private static IllegalArgumentException invalidCount(Throwable cause) {
        return new IllegalArgumentException(
                "COUNT aggregate result must be an exact long integer", cause);
    }

    private static IllegalArgumentException invalidDecimal(Throwable cause) {
        return new IllegalArgumentException(
                "SUM and AVG aggregate result must be a decimal number", cause);
    }
}
