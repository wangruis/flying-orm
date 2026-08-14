package com.flying.orm.core.metadata;

import java.util.Locale;

/**
 * 元数据名称处理工具，统一表名、列名等标识符的基础校验和规范化策略。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
final class MetadataNames {

    private MetadataNames() {
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
