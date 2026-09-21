package com.flying.orm.core.error;

import java.util.Objects;

/**
 * ORM 对外统一的错误小对象。上层只认这一种结构，不必再逐个解析异常文本。
 *
 * @param category 错误大类，例如 CONDITION、SCOPE、DATABASE
 * @param code 稳定错误码
 * @param resource 出错资源，例如表单名或 SQLState
 * @param path 前端输入路径，例如 conditions[2].value
 * @param field 相关字段
 * @param message 给开发人员看的说明
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record OrmErrorReport(String category,
                             String code,
                             String resource,
                             String path,
                             String field,
                             String message) {

    public OrmErrorReport {
        category = requireText(category, "error category");
        code = requireText(code, "error code");
        message = Objects.requireNonNull(message, "error message must not be null");
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
