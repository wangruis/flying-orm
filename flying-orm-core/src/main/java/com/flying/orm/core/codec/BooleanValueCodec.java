package com.flying.orm.core.codec;

import java.math.BigDecimal;
import java.util.Locale;

/** 兼容常见数据库布尔表示，同时拒绝含义不明确的文本。 */
final class BooleanValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == Boolean.class;
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        if (value instanceof Number number) {
            // 不能借 intValue() 判断真假，大整数低位为 0 或小数会被错误解释。
            return ValueCodecTypeSupport.decimal(number).compareTo(BigDecimal.ZERO) != 0;
        }
        return switch (ValueCodecTypeSupport.text(value).toLowerCase(Locale.ROOT)) {
            case "true", "1", "yes", "y", "on" -> true;
            case "false", "0", "no", "n", "off" -> false;
            default -> throw new IllegalArgumentException("boolean value is not supported");
        };
    }
}
