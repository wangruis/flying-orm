package com.flying.orm.core.page;

import com.flying.orm.core.internal.value.BindableValueSnapshots;

import java.util.List;
import java.util.Objects;

/**
 * 一次 keyset 位置的类型化值序列。
 *
 * <p>值保持调用方传入的 Java 类型，也允许 {@code null}。可变数组和可变 JDBC 标量在边界上只快照
 * 一次；访问时再返回安全副本，防止调用方改坏后续参数绑定。字符串令牌的签名、加密和传输仍由上层负责。</p>
 *
 * @param values 按最终稳定排序顺序排列的位置值
 * @author wangr
 * @version v3.2
 */
public record CursorPosition(List<Object> values) {

    private static final CursorPosition FIRST = new CursorPosition(List.of());

    public CursorPosition {
        values = BindableValueSnapshots.immutableValues(
                Objects.requireNonNull(values, "cursor position values must not be null"));
    }

    /** 返回第一页使用的共享空位置。 */
    public static CursorPosition first() {
        return FIRST;
    }

    /** 从任意只读或可变列表建立独立位置快照。 */
    public static CursorPosition of(List<?> values) {
        Objects.requireNonNull(values, "cursor position values must not be null");
        if (values.isEmpty()) {
            return FIRST;
        }
        return new CursorPosition(cast(values));
    }

    public boolean isFirst() {
        return values.isEmpty();
    }

    @Override
    public List<Object> values() {
        return BindableValueSnapshots.immutableValues(values);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> cast(List<?> values) {
        return (List<Object>) values;
    }
}
