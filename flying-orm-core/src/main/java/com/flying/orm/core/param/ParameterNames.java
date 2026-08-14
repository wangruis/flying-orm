package com.flying.orm.core.param;

import java.util.Locale;

/**
 * ParameterNames 统一参数驱动条件的名称校验和规范化策略。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
final class ParameterNames {

    private ParameterNames() {
    }

    static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    static String normalize(String value, String fieldName) {
        return requireText(value, fieldName).toLowerCase(Locale.ROOT);
    }

    static int mapCapacity(int size) {
        return Math.max(4, (int) (size / 0.75F) + 1);
    }
}
