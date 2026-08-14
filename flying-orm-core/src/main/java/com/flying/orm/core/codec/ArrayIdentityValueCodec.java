package com.flying.orm.core.codec;

/** 已被字段 codec 强类型化的数组直接交给驱动，跨类型数组转换必须使用字段感知 codec。 */
final class ArrayIdentityValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType.isArray();
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        if (targetType.isInstance(value)) {
            return value;
        }
        throw new IllegalArgumentException("array conversion requires a field-aware codec");
    }
}
