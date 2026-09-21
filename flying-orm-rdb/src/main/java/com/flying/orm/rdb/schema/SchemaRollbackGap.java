package com.flying.orm.rdb.schema;

import java.util.Objects;

/**
 * 回滚计划无法自动恢复的部分。结构 SQL 能重新建列，不代表被删掉的数据也能回来。
 *
 * @param kind 缺口类型
 * @param objectName 表、列、索引或约束名
 * @param reason 需要人工处理的原因
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaRollbackGap(Kind kind, String objectName, String reason) {

    public SchemaRollbackGap {
        kind = Objects.requireNonNull(kind, "rollback gap kind must not be null");
        objectName = requireText(objectName, "rollback gap object name");
        reason = requireText(reason, "rollback gap reason");
    }

    public enum Kind {
        DATA_CANNOT_BE_RESTORED,
        PRIMARY_KEY_REQUIRES_REVIEW,
        FOREIGN_KEY_REQUIRES_REVIEW,
        INDEX_REQUIRES_REVIEW
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }
}
