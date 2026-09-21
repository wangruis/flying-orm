package com.flying.orm.rdb.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 被安全策略拦下来的结构变更。它不是错误，而是提醒上层：这类动作最好先让人确认。
 *
 * @param kind   变更类型
 * @param name   字段名或索引名
 * @param reason 为什么不自动执行
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SkippedSchemaChange(Kind kind,
                                  String name,
                                  String reason,
                                  Map<String, Object> details,
                                  List<String> suggestedSteps) {

    public enum Kind {
        DROP_COLUMN,
        CHANGE_COLUMN,
        CHANGE_PRIMARY_KEY,
        DROP_INDEX,
        CHANGE_INDEX,
        ADD_FOREIGN_KEY,
        DROP_FOREIGN_KEY,
        CHANGE_FOREIGN_KEY
    }

    public SkippedSchemaChange(Kind kind, String name, String reason) {
        this(kind, name, reason, Map.of(), List.of());
    }

    public SkippedSchemaChange {
        kind = Objects.requireNonNull(kind, "skipped schema change kind must not be null");
        name = requireText(name, "skipped schema change name");
        reason = requireText(reason, "skipped schema change reason");
        details = Map.copyOf(Objects.requireNonNull(details, "skipped schema change details must not be null"));
        suggestedSteps = List.copyOf(Objects.requireNonNull(suggestedSteps,
                                                            "skipped schema change suggested steps must not be null"));
    }

    /**
     * 给日志、审计或前端提示用的一行摘要。
     *
     * @return 变更类型、对象名和跳过原因
     */
    public String summary() {
        return kind + " " + name + ": " + reason;
    }

    private static String requireText(String value, String fieldName) {
        String text = Objects.requireNonNull(value, fieldName + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return text;
    }
}
