package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.List;
import java.util.Objects;

/**
 * 一行类型化聚合结果。
 *
 * <p>列索引由共享 layout 预计算；按表达式读取是 O(1)，不会在每次 get 时扫描 alias。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class AggregateRow {

    private final AggregateRowLayout layout;
    private final List<Object> values;

    private AggregateRow(AggregateRowLayout layout, List<?> values) {
        this.layout = Objects.requireNonNull(layout, "aggregate row layout must not be null");
        this.values = BindableValueSnapshots.immutableValues(
                Objects.requireNonNull(values, "aggregate row values must not be null"));
        if (layout.size() != this.values.size()) {
            throw new IllegalArgumentException("aggregate row value count does not match its layout");
        }
    }

    public static AggregateRow of(AggregateRowLayout layout, List<?> values) {
        return new AggregateRow(layout, values);
    }

    public AggregateRowLayout layout() {
        return layout;
    }

    public <T> T get(AggregateExpression<T> expression) {
        AggregateExpression<T> safeExpression = Objects.requireNonNull(
                expression, "aggregate expression must not be null");
        Object value = values.get(layout.indexOf(safeExpression));
        return value == null ? null : safeExpression.javaType().cast(value);
    }

    public <T> T get(GroupSelection group, Class<T> javaType) {
        Object value = values.get(layout.indexOf(group));
        return value == null ? null : Objects.requireNonNull(
                javaType, "aggregate group Java type must not be null").cast(value);
    }

    public List<Object> values() {
        return values;
    }
}
