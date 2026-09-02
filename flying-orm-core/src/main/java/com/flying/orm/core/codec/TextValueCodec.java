package com.flying.orm.core.codec;

import java.nio.CharBuffer;

/** 大文本和普通文本都保留空白，只将可变字符序列收口为最通用的 String。 */
final class TextValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == String.class
                || targetType == CharSequence.class
                || targetType == StringBuilder.class
                || targetType == StringBuffer.class
                || targetType == CharBuffer.class
                || targetType == Character.class
                || targetType == char[].class;
    }

    @Override
    public Object write(Object value) {
        return value instanceof char[] characters ? new String(characters) : value.toString();
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        String text = value instanceof char[] characters ? new String(characters) : value.toString();
        if (targetType == String.class || targetType == CharSequence.class) {
            return text;
        }
        if (targetType == StringBuilder.class) {
            return new StringBuilder(text);
        }
        if (targetType == StringBuffer.class) {
            return new StringBuffer(text);
        }
        if (targetType == CharBuffer.class) {
            return CharBuffer.wrap(text);
        }
        if (targetType == Character.class) {
            if (text.length() != 1) {
                throw new IllegalArgumentException("character value must contain exactly one character");
            }
            return text.charAt(0);
        }
        if (targetType == char[].class) {
            return text.toCharArray();
        }
        throw new IllegalArgumentException("text value cannot be converted to " + targetType.getName());
    }
}
