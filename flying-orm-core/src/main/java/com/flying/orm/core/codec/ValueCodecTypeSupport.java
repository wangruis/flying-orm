package com.flying.orm.core.codec;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

/** 数值 codec 共用的精确转换和基本类型包装规则。 */
final class ValueCodecTypeSupport {

    private ValueCodecTypeSupport() {
    }

    @SuppressWarnings("unchecked")
    static <T> Class<T> boxed(Class<T> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (type == int.class) {
            return (Class<T>) Integer.class;
        }
        if (type == long.class) {
            return (Class<T>) Long.class;
        }
        if (type == boolean.class) {
            return (Class<T>) Boolean.class;
        }
        if (type == double.class) {
            return (Class<T>) Double.class;
        }
        if (type == float.class) {
            return (Class<T>) Float.class;
        }
        if (type == short.class) {
            return (Class<T>) Short.class;
        }
        if (type == byte.class) {
            return (Class<T>) Byte.class;
        }
        if (type == char.class) {
            return (Class<T>) Character.class;
        }
        return type;
    }

    static String text(Object value) {
        String text = Objects.requireNonNull(value, "value must not be null").toString().trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("value text must not be blank");
        }
        return text;
    }

    static BigDecimal decimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        if (value instanceof BigInteger integer) {
            return new BigDecimal(integer);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Number number) {
            // 不能借道 double；AtomicLong 等 Number 实现可能含有超过 2^53 的精确整数。
            return decimalText(number.toString());
        }
        return decimalText(text(value));
    }

    static int exactInteger(BigDecimal value) {
        try {
            return value.intValueExact();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("number cannot be converted to integer without loss");
        }
    }

    static long exactLong(BigDecimal value) {
        try {
            return value.longValueExact();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("number cannot be converted to long without loss");
        }
    }

    static BigInteger exactBigInteger(BigDecimal value) {
        try {
            return value.toBigIntegerExact();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("number cannot be converted to big integer without loss");
        }
    }

    private static BigDecimal decimalText(String text) {
        try {
            return new BigDecimal(text);
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("value cannot be converted to decimal");
        }
    }

}
