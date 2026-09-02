package com.flying.orm.rdb.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

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

    private static final Pattern JSON_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    public JsonConditionValue {
        kind = Objects.requireNonNull(kind, "json condition kind must not be null");
        path = List.copyOf(Objects.requireNonNull(path, "json condition path must not be null"));
        if (kind == Kind.CONTAINS && !path.isEmpty()) {
            throw new IllegalArgumentException("json contains condition must not declare a path");
        }
        if (kind != Kind.CONTAINS && path.isEmpty()) {
            throw new IllegalArgumentException("json condition path must not be empty");
        }
        if (path.stream().anyMatch(segment -> segment == null || !JSON_KEY.matcher(segment).matches())) {
            throw new IllegalArgumentException("json condition path contains an invalid segment");
        }
        if (kind == Kind.EXISTS) {
            if (value != null) {
                throw new IllegalArgumentException("json exists condition must not declare a compare value");
            }
        } else {
            value = Objects.requireNonNull(value, "json condition value is required");
        }
        if (kind == Kind.CONTAINS) {
            value = JsonValueCodec.write(value);
        }
        if (kind == Kind.PATH_EQUALS || kind == Kind.ARRAY_CONTAINS) {
            value = scalarValue(value);
        }
    }

    private static Object scalarValue(Object value) {
        if (value instanceof CharSequence text) {
            String snapshot = text.toString();
            if (snapshot.isEmpty()) {
                throw new IllegalArgumentException("json path compare value must not be empty");
            }
            return snapshot;
        }
        if (value instanceof Character character) {
            return character.toString();
        }
        if (value instanceof Boolean
                || value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || value instanceof BigInteger || value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof AtomicInteger number) {
            return number.get();
        }
        if (value instanceof AtomicLong number) {
            return number.get();
        }
        if (value instanceof Float number && Float.isFinite(number)) {
            return number;
        }
        if (value instanceof Double number && Double.isFinite(number)) {
            return number;
        }
        throw new IllegalArgumentException("json path compare value must be a finite JSON scalar");
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
