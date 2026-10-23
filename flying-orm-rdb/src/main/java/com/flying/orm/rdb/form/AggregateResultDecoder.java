package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateFunction;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateRowLayout;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.mapping.RowMapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * 按 planner 的固定布局把 JDBC/R2DBC 动态行解码成类型化聚合行。
 *
 * <p>结果集内所有行共享同一 {@link AggregateRowLayout}。驱动对 COUNT、SUM/AVG 和
 * MIN/MAX 的返回差异在此收敛，客户端与同步/响应式执行器不再各自转换。</p>
 *
 * @author wangr
 * @version v3.2
 */
final class AggregateResultDecoder implements RowMapper<AggregateRow> {

    private static final UnaryOperator<Object> COUNT_READER = RowMapper::exactCount;
    private static final UnaryOperator<Object> DECIMAL_READER = AggregateResultDecoder::decimal;
    private static final UnaryOperator<Object> HIDDEN_READER = value -> null;

    private final FormAggregatePlanner.Plan plan;
    private final List<UnaryOperator<Object>> readers;

    AggregateResultDecoder(FormAggregatePlanner.Plan plan) {
        this.plan = Objects.requireNonNull(plan, "aggregate plan must not be null");
        this.readers = prepareReaders();
    }

    /** 字段类型、聚合函数和权限只在本次结果集开始时解析；逐行直接使用固定列读取规则。 */
    private List<UnaryOperator<Object>> prepareReaders() {
        List<UnaryOperator<Object>> resolved = new ArrayList<>(plan.layout().size());
        for (int index = 0; index < plan.groupFields().size(); index++) {
            AggregateInput input = plan.groupFields().get(index);
            FieldVisibility visibility = input.visibility(plan.fieldUse());
            if (plan.layout().groups().get(index).computed()) {
                resolved.add(visibility == FieldVisibility.HIDDEN ? HIDDEN_READER : plan.reads().decoder(input.valueField(), null));
                continue;
            }
            AggregateField source = input.sources().getFirst();
            UnaryOperator<Object> decode = plan.reads().decoder(source.field(), null);
            resolved.add(visibility == FieldVisibility.FULL
                    && source.form().protections().masked(source.field().name()).isEmpty()
                    ? decode : value -> visibleGroupValue(source, decode.apply(value), visibility));
        }
        for (int index = 0; index < plan.aggregateFields().size(); index++) {
            AggregateInput input = plan.aggregateFields().get(index);
            AggregateExpression<?> expression = plan.layout().aggregates().get(index);
            resolved.add(aggregateReader(expression, input, input.visibility(plan.fieldUse())));
        }
        return resolved;
    }

    private UnaryOperator<Object> aggregateReader(AggregateExpression<?> expression, AggregateInput input,
                                                   FieldVisibility visibility) {
        UnaryOperator<Object> decode = valueReader(expression, input.valueField());
        if (visibility == FieldVisibility.HIDDEN) return HIDDEN_READER;
        if (expression.function() == AggregateFunction.COUNT_ROWS
                || expression.operand() != null && !(expression.operand() instanceof ValueExpression.Field)) {
            return decode;
        }
        AggregateField source = input.sources().getFirst();
        boolean extrema = expression.function() == AggregateFunction.MIN || expression.function() == AggregateFunction.MAX;
        if (visibility == FieldVisibility.FULL
                && (!extrema || source.form().protections().masked(source.field().name()).isEmpty())) {
            return decode;
        }
        return value -> decodeMaskedAggregate(source, value, visibility, decode);
    }

    /** 解码一行，并拒绝列数量与共享布局不一致的执行器结果。 */
    AggregateRow decode(DynamicRow row) {
        requireLayout(row);
        return decodeValues(row);
    }

    @Override
    public AggregateRow map(DynamicRow row) {
        return decode(row);
    }

    /** 复用执行器的查询局部值读取器，不保留驱动行或可复用缓冲。 */
    @Override
    public Function<IntFunction<Object>, AggregateRow> bindColumns(DynamicRow columns) {
        requireLayout(columns);
        return this::decodeValues;
    }

    private void requireLayout(DynamicRow row) {
        if (Objects.requireNonNull(row, "aggregate result row must not be null").columnCount() != plan.layout().size()) {
            throw new IllegalArgumentException(
                    "aggregate result column count does not match the planned layout");
        }
    }

    private AggregateRow decodeValues(IntFunction<Object> row) {
        List<Object> values = new ArrayList<>(plan.layout().size());
        for (int index = 0; index < readers.size(); index++) {
            values.add(readers.get(index).apply(row.apply(index)));
        }
        return AggregateRow.of(plan.layout(), OwnedBindableValues.publishOwned(values));
    }

    private Object visibleGroupValue(AggregateField source, Object value, FieldVisibility visibility) {
        DynamicField field = source.field();
        if (visibility == FieldVisibility.HIDDEN) {
            return null;
        }
        requireMaskDefinition(source, visibility);
        SensitiveDisplayMode displayMode = visibility == FieldVisibility.MASKED
                ? SensitiveDisplayMode.MASKED : plan.displayMode();
        return plan.reads().maskGroupValue(source.form(), field, value, displayMode);
    }

    private Object decodeMaskedAggregate(AggregateField source, Object value,
                                         FieldVisibility visibility, UnaryOperator<Object> decode) {
        requireMaskDefinition(source, visibility);
        if (value == null) {
            return null;
        }
        SensitiveDisplayMode displayMode = visibility == FieldVisibility.MASKED
                ? SensitiveDisplayMode.MASKED : plan.displayMode();
        return plan.reads().maskGroupValue(source.form(), source.field(), decode.apply(value), displayMode);
    }

    private UnaryOperator<Object> valueReader(AggregateExpression<?> expression, DynamicField field) {
        return switch (expression.function()) {
            case COUNT_ROWS, COUNT, COUNT_DISTINCT -> COUNT_READER;
            case SUM, AVG, STDDEV_POP, STDDEV_SAMPLE, VARIANCE_POP, VARIANCE_SAMPLE -> DECIMAL_READER;
            case MIN, MAX, REGISTERED -> plan.reads().decoder(field, expression.javaType());
        };
    }

    private void requireMaskDefinition(AggregateField source, FieldVisibility visibility) {
        DynamicField field = source.field();
        if (visibility != FieldVisibility.MASKED
                || source.form().protections().masked(field.name()).isPresent()) {
            return;
        }
        throw new ScopeAccessException(
                ScopeErrorCode.FIELD_NOT_READABLE,
                source.form().id(),
                field.name(),
                "field [" + field.name() + "] is MASKED but has no masking definition");
    }

    private static BigDecimal decimal(Object value) {
        if (value == null) return null;
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

    private static IllegalArgumentException invalidDecimal(Throwable cause) {
        return new IllegalArgumentException(
                "numeric aggregate result must be a decimal number", cause);
    }
}
