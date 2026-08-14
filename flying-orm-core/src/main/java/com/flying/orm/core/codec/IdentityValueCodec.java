package com.flying.orm.core.codec;

/** String、Object 和 Character 的通用兜底转换。 */
final class IdentityValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == String.class || targetType == Object.class || targetType == Character.class;
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        if (targetType == String.class) {
            return value.toString();
        }
        if (targetType == Character.class) {
            String text = ValueCodecTypeSupport.text(value);
            if (text.length() == 1) {
                return text.charAt(0);
            }
            throw new IllegalArgumentException("character value must contain exactly one character");
        }
        return value;
    }
}
