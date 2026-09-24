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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 批量参数的稳定内存估算器。
 *
 * <p>这不是 JVM 对象布局测量器，而是用于执行保护的保守权重：文本按 UTF-8 字节数计算，二进制按真实长度，
 * 数组、集合和 Map 迭代累计并加少量容器开销。规则只依赖值本身，同一输入每次都会得到相同结果。</p>
 *
 * <p>超出 long 算术范围时返回 {@link Long#MAX_VALUE}。有界消费者必须把该值
 * 视为未知预算并失败闭合，不能把它当成可接受的精确等值。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@InternalApi
public final class BatchMemoryBudget {

    private static final long ROW_OVERHEAD = 24L;
    private static final long REFERENCE_BYTES = 8L;

    private BatchMemoryBudget() {
    }

    public static long estimateRowBytes(Object[] row) {
        return estimateRowBytes(row, row == null ? 0 : row.length, new IdentityHashMap<>());
    }

    /** Estimates a parameter prefix without allocating a trimmed array for internal metadata rows. */
    public static long estimateRowBytes(Object[] row, int length) {
        Object[] safeRow = java.util.Objects.requireNonNull(row, "batch row must not be null");
        if (length < 0 || length > safeRow.length) {
            throw new IllegalArgumentException("batch row estimate length is out of bounds");
        }
        return estimateRowBytes(safeRow, length, new IdentityHashMap<>());
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
        return estimateValueBytes(value, new IdentityHashMap<>());
    }

    /**
     * 直接估算紧凑动态行，不把它展开成 Map 或临时 Object[]。
     */
    public static long estimateRowBytes(DynamicRow row) {
        DynamicRow safeRow = java.util.Objects.requireNonNull(row, "dynamic row must not be null");
        IdentityHashMap<Object, Boolean> seen = new IdentityHashMap<>();
        long total = ROW_OVERHEAD + (long) safeRow.columnCount() * REFERENCE_BYTES;
        for (int index = 0; index < safeRow.columnCount(); index++) {
            total = saturatedAdd(total, estimateValueBytes(safeRow.value(index), seen));
        }
        return total;
    }

    private static long estimateRowBytes(Object[] row,
                                         int length,
                                         IdentityHashMap<Object, Boolean> seen) {
        if (row == null) {
            return REFERENCE_BYTES;
        }
        if (seen.put(row, Boolean.TRUE) != null) {
            return REFERENCE_BYTES;
        }
        return estimateSeenRowBytes(row, length, seen);
    }

    private static long estimateSeenRowBytes(Object[] row,
                                             int length,
                                             IdentityHashMap<Object, Boolean> seen) {
        long total = ROW_OVERHEAD + (long) length * REFERENCE_BYTES;
        for (int index = 0; index < length; index++) {
            total = saturatedAdd(total, estimateValueBytes(row[index], seen));
        }
        return total;
    }

    private static long estimateValueBytes(Object value, IdentityHashMap<Object, Boolean> seen) {
        long total = 0L;
        ArrayDeque<Iterator<?>> pending = null;
        Object current = value;
        while (true) {
            long bytes;
            Iterator<?> children = null;
            if (current == null || seen.put(current, Boolean.TRUE) != null) {
                bytes = REFERENCE_BYTES;
            } else if (current instanceof byte[] binary) {
                bytes = 16L + binary.length;
            } else if (current instanceof ByteBuffer buffer) {
                // remaining 不能代表输入缓冲继续强引用的底层容量。
                bytes = 24L + buffer.capacity();
            } else if (current instanceof CharSequence text) {
                bytes = 24L + utf8Length(text);
            } else if (current instanceof SqlTypedValue typedValue) {
                total = saturatedAdd(total, 24L);
                current = typedValue.value();
                continue;
            } else if (current instanceof Parameter parameter) {
                total = saturatedAdd(total, 24L);
                current = parameter.getValue();
                continue;
            } else if (current instanceof BigDecimal decimal) {
                bytes = estimateBigDecimalBytes(decimal);
            } else if (current instanceof BigInteger integer) {
                bytes = estimateBigIntegerBytes(integer);
            } else if (current instanceof Number || current instanceof Boolean || current instanceof Character) {
                bytes = 24L;
            } else if (current instanceof Object[] array) {
                bytes = ROW_OVERHEAD + (long) array.length * REFERENCE_BYTES;
                children = Arrays.asList(array).iterator();
            } else if (current.getClass().isArray()) {
                bytes = primitiveArrayBytes(current, current.getClass().getComponentType());
            } else if (current instanceof Collection<?> collection) {
                bytes = 24L + (long) collection.size() * REFERENCE_BYTES;
                children = collection.iterator();
            } else if (current instanceof Map<?, ?> map) {
                bytes = 32L + (long) map.size() * 32L;
                children = mapValues(map);
            } else {
                bytes = 64L;
            }
            total = saturatedAdd(total, bytes);
            if (children != null) {
                if (pending == null) {
                    pending = new ArrayDeque<>();
                }
                pending.push(children);
            }
            // 显式深度优先遍历只保存当前容器的迭代位置，不按集合宽度复制元素。
            while (pending != null && !pending.isEmpty() && !pending.peek().hasNext()) {
                pending.pop();
            }
            if (pending == null || pending.isEmpty()) {
                return total;
            }
            current = pending.peek().next();
        }
    }

    private static Iterator<Object> mapValues(Map<?, ?> map) {
        Iterator<? extends Map.Entry<?, ?>> entries = map.entrySet().iterator();
        return new Iterator<>() {
            private Map.Entry<?, ?> entry;

            @Override
            public boolean hasNext() {
                return entry != null || entries.hasNext();
            }

            @Override
            public Object next() {
                if (entry != null) {
                    Object value = entry.getValue();
                    entry = null;
                    return value;
                }
                entry = entries.next();
                return entry.getKey();
            }
        };
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
     * BigDecimal 本体可能很小，但负 scale 会让驱动转换产生很长的十进制表示，预算必须提前覆盖该放大。
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
