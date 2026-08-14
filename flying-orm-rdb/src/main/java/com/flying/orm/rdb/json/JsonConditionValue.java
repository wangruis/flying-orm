package com.flying.orm.rdb.json;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * JSON 高级条件的内部值对象。前端输入先经过校验再变成它，渲染器不用再猜输入结构。
 *
 * @param kind  条件类型
 * @param path  已校验的 JSON 路径片段
 * @param value 比较值或包含值
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public record JsonConditionValue(Kind kind, List<String> path, Object value) {

    public JsonConditionValue {
        kind = Objects.requireNonNull(kind, "json condition kind must not be null");
        path = List.copyOf(Objects.requireNonNull(path, "json condition path must not be null"));
        if (kind != Kind.CONTAINS && path.isEmpty()) {
            throw new IllegalArgumentException("json condition path must not be empty");
        }
        if (kind == Kind.PATH_EQUALS && value == null) {
            throw new IllegalArgumentException("json condition compare value is required");
        }
        if (kind == Kind.CONTAINS && value == null) {
            throw new IllegalArgumentException("json condition contains value is required");
        }
        if (kind == Kind.ARRAY_CONTAINS && value == null) {
            throw new IllegalArgumentException("json array element value is required");
        }
        if (kind == Kind.ARRAY_CONTAINS
                && (value instanceof Map<?, ?> || value instanceof Iterable<?> || value.getClass().isArray())) {
            throw new IllegalArgumentException("json array element value must be scalar");
        }
    }

    public static JsonConditionValue pathEquals(List<String> path, Object value) {
        return new JsonConditionValue(Kind.PATH_EQUALS, path, value);
    }

    public static JsonConditionValue contains(Object value) {
        return new JsonConditionValue(Kind.CONTAINS, List.of(), value);
    }

    public static JsonConditionValue exists(List<String> path) {
        return new JsonConditionValue(Kind.EXISTS, path, null);
    }

    public static JsonConditionValue arrayContains(List<String> path, Object value) {
        return new JsonConditionValue(Kind.ARRAY_CONTAINS, path, value);
    }

    public enum Kind {
        PATH_EQUALS,
        CONTAINS,
        EXISTS,
        ARRAY_CONTAINS
    }
}
