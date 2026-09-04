package com.flying.orm.core.codec;

/** 枚举按常量名持久化，避免 ordinal 随声明顺序变化而破坏历史数据。 */
final class EnumValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        // 带独立方法实现的枚举常量，其运行时类型是枚举的子类，仍按声明名称绑定。
        return Enum.class.isAssignableFrom(targetType);
    }

    @Override
    public Object write(Object value) {
        return ((Enum<?>) value).name();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object read(Object value, Class<?> targetType) {
        try {
            return Enum.valueOf((Class<? extends Enum>) targetType, ValueCodecTypeSupport.text(value));
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("value cannot be converted to enum");
        }
    }
}
