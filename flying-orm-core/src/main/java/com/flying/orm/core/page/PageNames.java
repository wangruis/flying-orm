package com.flying.orm.core.page;

/**
 * 分页模块名称处理工具，集中处理基础文本校验。
 *
 * @author wangr
 * @date 2026-07-22
 * @version v1.0
 */
final class PageNames {

    private PageNames() {
    }

    static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }
}
