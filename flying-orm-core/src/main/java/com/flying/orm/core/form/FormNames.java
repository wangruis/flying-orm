package com.flying.orm.core.form;

import java.util.Locale;

/**
 * FormNames 统一动态表单名称的基础校验和规范化策略。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
final class FormNames {

    private FormNames() {
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
