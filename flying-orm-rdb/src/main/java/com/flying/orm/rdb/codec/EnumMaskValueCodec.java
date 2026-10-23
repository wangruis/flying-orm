package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ToLongFunction;

/**
 * 枚举数组按集合编码为 BIGINT 位掩码，接入现有实体类型映射；不改变单枚举的存储规则。
 * 位值在构造时解析一次，读写不再调用业务函数。回读按枚举声明顺序，重复元素合并。
 *
 * @param <E> 枚举类型
 * @author wangr
 * @version v4.1.2
 */
public final class EnumMaskValueCodec<E extends Enum<E>> implements ValueCodec {

    private final E[] constants;
    private final Class<?> arrayType;
    private final long[] bits;
    private final long knownMask;

    private EnumMaskValueCodec(Class<E> type, ToLongFunction<E> bitValue) {
        constants = Objects.requireNonNull(type, "enum type must not be null").getEnumConstants();
        if (constants == null) {
            throw new IllegalArgumentException("mask codec requires an enum type");
        }
        Objects.requireNonNull(bitValue, "enum bit function must not be null");
        arrayType = constants.getClass();
        bits = new long[constants.length];
        long known = 0;
        for (E constant : constants) {
            long bit = bitValue.applyAsLong(constant);
            if (Long.bitCount(bit) != 1 || (known & bit) != 0) {
                throw new IllegalArgumentException("enum mask values must be distinct single bits");
            }
            bits[constant.ordinal()] = bit;
            known |= bit;
        }
        knownMask = known;
    }

    /** 显式位值不依赖声明顺序；同一枚举的位值必须唯一且只置一位，包括 long 的符号位。 */
    public static <E extends Enum<E>> EnumMaskValueCodec<E> of(Class<E> type, ToLongFunction<E> bitValue) {
        return new EnumMaskValueCodec<>(type, bitValue);
    }

    /** 序号映射只适用于顺序固定的枚举；超过 long 可表示的位数会因位值重复而拒绝。 */
    public static <E extends Enum<E>> EnumMaskValueCodec<E> ordinal(Class<E> type) {
        return of(type, value -> 1L << value.ordinal());
    }

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == arrayType;
    }

    /** 构造条件参数；空集合为零，重复元素只占一位。 */
    @SafeVarargs
    public final long mask(E... values) {
        Objects.requireNonNull(values, "enum mask values must not be null");
        if (!arrayType.isInstance(values)) {
            throw new IllegalArgumentException("enum mask values have the wrong enum type");
        }
        long mask = 0;
        for (E value : values) {
            mask |= bits[Objects.requireNonNull(value, "enum mask element must not be null").ordinal()];
        }
        return mask;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Long write(Object value) {
        if (value == null) {
            return null;
        }
        if (!arrayType.isInstance(value)) {
            throw new IllegalArgumentException("enum mask value must be " + arrayType.getTypeName());
        }
        return mask((E[]) value);
    }

    @Override
    public E[] read(Object value, Class<?> targetType) {
        if (!supports(targetType)) {
            throw new IllegalArgumentException("enum mask target must be " + arrayType.getTypeName());
        }
        if (value == null) {
            return null;
        }
        long mask = ValueCodecRegistry.standard().read(value, Long.class);
        if ((mask & ~knownMask) != 0) {
            throw new IllegalArgumentException("enum mask contains unmapped bits");
        }
        E[] selected = Arrays.copyOf(constants, Long.bitCount(mask));
        int index = 0;
        for (E constant : constants) {
            if ((mask & bits[constant.ordinal()]) != 0) {
                selected[index++] = constant;
            }
        }
        return selected;
    }
}
