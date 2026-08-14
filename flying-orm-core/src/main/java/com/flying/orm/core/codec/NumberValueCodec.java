package com.flying.orm.core.codec;

import java.math.BigDecimal;
import java.math.BigInteger;

/** 数值先收口到 BigDecimal，再做精确整数或显式浮点转换。 */
final class NumberValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return Number.class.isAssignableFrom(targetType);
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        BigDecimal number = ValueCodecTypeSupport.decimal(value);
        if (targetType == Integer.class) {
            return ValueCodecTypeSupport.exactInteger(number);
        }
        if (targetType == Long.class) {
            return ValueCodecTypeSupport.exactLong(number);
        }
        if (targetType == BigDecimal.class) {
            return number;
        }
        if (targetType == BigInteger.class) {
            return ValueCodecTypeSupport.exactBigInteger(number);
        }
        if (targetType == Double.class) {
            double converted = number.doubleValue();
            if (!Double.isFinite(converted)) {
                throw new IllegalArgumentException("number is out of double range");
            }
            return converted;
        }
        if (targetType == Float.class) {
            float converted = number.floatValue();
            if (!Float.isFinite(converted)) {
                throw new IllegalArgumentException("number is out of float range");
            }
            return converted;
        }
        if (targetType == Short.class) {
            int integer = ValueCodecTypeSupport.exactInteger(number);
            if (integer < Short.MIN_VALUE || integer > Short.MAX_VALUE) {
                throw new IllegalArgumentException("number is out of short range");
            }
            return (short) integer;
        }
        if (targetType == Byte.class) {
            int integer = ValueCodecTypeSupport.exactInteger(number);
            if (integer < Byte.MIN_VALUE || integer > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("number is out of byte range");
            }
            return (byte) integer;
        }
        throw new IllegalArgumentException("number type is not supported: " + targetType.getName());
    }
}
