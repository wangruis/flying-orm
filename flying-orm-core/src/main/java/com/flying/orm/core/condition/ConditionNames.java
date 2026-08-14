package com.flying.orm.core.condition;

import java.util.Locale;

/**
 * 条件模块的名称处理工具，统一字段名和 term id 的基础校验与规范化规则。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
final class ConditionNames {

    private ConditionNames() {
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
