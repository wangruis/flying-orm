package com.flying.orm.core.sql.render;

import java.util.Locale;

/**
 * SQL 渲染模块名称处理工具，负责渲染前的基础校验和 term id 规范化。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
final class RenderNames {

    private RenderNames() {
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
