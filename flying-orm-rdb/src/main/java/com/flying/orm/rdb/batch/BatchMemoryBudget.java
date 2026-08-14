package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Parameter;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.temporal.TemporalAccessor;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * 批量参数的稳定内存估算器。
 *
 * <p>这不是 JVM 对象布局测量器，而是用于执行保护的保守权重：文本按 UTF-8 字节数计算，二进制按真实长度，
 * 数组、集合和 Map 递归累计并加少量容器开销。规则只依赖值本身，同一输入每次都会得到相同结果。</p>
 *
 * <p>无法在安全递归深度或 long 算术范围内完成可信估算时返回 {@link Long#MAX_VALUE}。有界消费者必须把该值
 * 视为未知预算并失败闭合，不能把它当成可接受的精确等值。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@InternalApi
public final class BatchMemoryBudget {

    private static final long ROW_OVERHEAD = 24L;
    private static final long REFERENCE_BYTES = 8L;
    private static final int MAX_NESTING_DEPTH = 64;

    private BatchMemoryBudget() {
    }

    public static long estimateRowBytes(Object[] row) {
        return estimateRowBytes(row, new IdentityHashMap<>(), 0);
    }

    /**
     * 单次识别并估算不含嵌套或可变载荷的普通标量行；需要图遍历时返回 {@code -1}。
     * 该快路径让批量所有权检查复用估算结果，不削减未知值的失败闭合处理。
     */
    public static long estimateShallowScalarRowBytes(Object[] row) {
        if (row == null) {
            return REFERENCE_BYTES;
        }
        long total = ROW_OVERHEAD + (long) row.length * REFERENCE_BYTES;
        for (Object value : row) {
            long bytes = shallowScalarBytes(value);
            if (bytes < 0L) {
                return -1L;
            }
            total = saturatedAdd(total, bytes);
        }
        return total;
    }

    /**
     * 估算一个批量参数对象占用的内存权重。
     *
     * <p>Repository 在真正交给驱动前，可能暂时同时保留实体和由实体生成的 Map、数组等参数对象。
     * 这里复用批量写入器的同一套估算规则，避免两层各算各的，合起来却已经超过内存上限。</p>
     */
    public static long estimateValueBytes(Object value) {
        return estimateValueBytes(value, new IdentityHashMap<>(), 0);
    }

    /**
     * 直接估算紧凑动态行，不把它展开成 Map 或临时 Object[]。
     */
    public static long estimateRowBytes(DynamicRow row) {
        DynamicRow safeRow = java.util.Objects.requireNonNull(row, "dynamic row must not be null");
        IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        long total = ROW_OVERHEAD + (long) safeRow.columnCount() * REFERENCE_BYTES;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            total = saturatedAdd(total, estimateValueBytes(safeRow.value(index), seen, 1));
        }
        return total;
    }

    private static long estimateRowBytes(Object[] row, IdentityHashMap<Object, Boolean> seen, int depth) {
        if (row == null) {
            return REFERENCE_BYTES;
        }
        if (seen.put(row, Boolean.TRUE) != null) {
            return REFERENCE_BYTES;
        }
        long total = ROW_OVERHEAD + (long) row.length * REFERENCE_BYTES;
        for (Object value : row) {
            total = saturatedAdd(total, estimateValueBytes(value, seen, depth + 1));
        }
        return total;
    }

    private static long estimateValueBytes(Object value, IdentityHashMap<Object, Boolean> seen, int depth) {
        if (value == null) return REFERENCE_BYTES;
        if (value instanceof byte[] bytes) return 16L + bytes.length;
        // 分片会继续强引用 ByteBuffer；remaining 只表示本次可读区间，不能代表被保留的底层容量。
        if (value instanceof ByteBuffer buffer) return 24L + buffer.capacity();
        if (value instanceof CharSequence text) return 24L + utf8Length(text);
        if (depth > MAX_NESTING_DEPTH) return Long.MAX_VALUE;
        if (value instanceof SqlTypedValue typedValue) {
            return estimateWrappedValue(typedValue, typedValue.value(), seen, depth);
        }
        if (value instanceof Parameter parameter) {
            return estimateWrappedValue(parameter, parameter.getValue(), seen, depth);
        }
        if (value instanceof BigDecimal decimal) return estimateBigDecimalBytes(decimal);
        if (value instanceof BigInteger integer) return estimateBigIntegerBytes(integer);
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) return 24L;
        Class<?> valueType = value.getClass();
        if (valueType.isArray()) {
            if (value instanceof Object[] array) return estimateRowBytes(array, seen, depth);
            return primitiveArrayBytes(value, valueType.getComponentType());
        }
        if (seen.put(value, Boolean.TRUE) != null) return REFERENCE_BYTES;
        if (value instanceof Collection<?> collection) {
            long total = 24L + (long) collection.size() * REFERENCE_BYTES;
            for (Object item : collection) total = saturatedAdd(total, estimateValueBytes(item, seen, depth + 1));
            return total;
        }
        if (value instanceof Map<?, ?> map) {
            long total = 32L + (long) map.size() * 32L;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                total = saturatedAdd(total, estimateValueBytes(entry.getKey(), seen, depth + 1));
                total = saturatedAdd(total, estimateValueBytes(entry.getValue(), seen, depth + 1));
            }
            return total;
        }
        return 64L;
    }

    private static long estimateWrappedValue(Object wrapper,
                                             Object value,
                                             IdentityHashMap<Object, Boolean> seen,
                                             int depth) {
        if (seen.put(wrapper, Boolean.TRUE) != null) {
            return REFERENCE_BYTES;
        }
        return saturatedAdd(24L, estimateValueBytes(value, seen, depth + 1));
    }

    private static long shallowScalarBytes(Object value) {
        if (value == null) return REFERENCE_BYTES;
        if (value instanceof String text) return 24L + utf8Length(text);
        if (value instanceof BigDecimal decimal) return estimateBigDecimalBytes(decimal);
        if (value instanceof BigInteger integer) return estimateBigIntegerBytes(integer);
        if (value instanceof Number || value instanceof Boolean || value instanceof Character) return 24L;
        if (value instanceof Enum<?> || value instanceof TemporalAccessor) return 64L;
        return -1L;
    }

    /**
     * BigDecimal 本体可能很小，但负 scale 会让回执摘要和驱动转换产生很长的十进制表示，预算必须提前覆盖该放大。
     */
    private static long estimateBigDecimalBytes(BigDecimal decimal) {
        long precision = decimal.precision();
        long scale = decimal.scale();
        long plainLength;
        if (scale == 0L) {
            plainLength = precision;
        } else if (scale < 0L) {
            plainLength = saturatedAdd(precision, -scale);
        } else if (scale < precision) {
            plainLength = saturatedAdd(precision, 1L);
        } else {
            plainLength = saturatedAdd(scale, 2L);
        }
        if (decimal.signum() < 0) {
            plainLength = saturatedAdd(plainLength, 1L);
        }
        return saturatedAdd(32L,
                            saturatedAdd(bigIntegerMagnitudeBytes(decimal.unscaledValue()), plainLength));
    }

    private static long estimateBigIntegerBytes(BigInteger integer) {
        return saturatedAdd(24L, bigIntegerMagnitudeBytes(integer));
    }

    /** bitLength 不分配新的 magnitude 数组，额外一字节覆盖符号位和零值。 */
    private static long bigIntegerMagnitudeBytes(BigInteger integer) {
        return ((long) integer.bitLength() + Byte.SIZE) / Byte.SIZE;
    }

    private static long primitiveArrayBytes(Object array, Class<?> componentType) {
        int bytesPerElement = componentType == boolean.class || componentType == byte.class ? 1
                : componentType == char.class || componentType == short.class ? 2
                : componentType == int.class || componentType == float.class ? 4 : 8;
        return saturatedAdd(16L, saturatedMultiply(Array.getLength(array), bytesPerElement));
    }

    /** 直接扫描字符，不为了估算再分配一份 UTF-8 byte[]。 */
    private static long utf8Length(CharSequence text) {
        long bytes = 0L;
        for (int index = 0; index < text.length(); index++) {
            char current = text.charAt(index);
            if (current <= 0x7f) {
                bytes++;
            } else if (current <= 0x7ff) {
                bytes += 2;
            } else if (Character.isHighSurrogate(current)
                    && index + 1 < text.length()
                    && Character.isLowSurrogate(text.charAt(index + 1))) {
                bytes += 4;
                index++;
            } else {
                bytes += 3;
            }
        }
        return bytes;
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private static long saturatedMultiply(long left, long right) {
        return left != 0L && right > Long.MAX_VALUE / left ? Long.MAX_VALUE : left * right;
    }
}
