package com.flying.orm.rdb.json;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * JSON 条件值对象。前端输入先经过校验再变成它，后端也可直接构建，渲染器不用再猜输入结构。
 *
 * @param kind  条件类型
 * @param path  属性名、JSON 双引号属性名或非负数组下标（如 [0]）组成的路径片段；空列表表示根值
 * @param value 比较值或包含值
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public record JsonConditionValue(Kind kind, List<String> path, Object value) {

    // 双引号片段只接受 JSON 字符串转义；占有量词避免长属性名产生递归回溯。
    private static final Pattern PATH_SEGMENT = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*|\\[[0-9]+\\]|\"(?:[^\"\\\\\\x00-\\x1F]|\\\\(?:[\"\\\\/bfnrt]|u[0-9A-Fa-f]{4}))*+\"");

    public JsonConditionValue {
        kind = Objects.requireNonNull(kind, "json condition kind must not be null");
        path = List.copyOf(Objects.requireNonNull(path, "json condition path must not be null"));
        if (kind == Kind.CONTAINS && !path.isEmpty()) {
            throw new IllegalArgumentException("json contains condition must not declare a path");
        }
        for (String segment : path) {
            if (!PATH_SEGMENT.matcher(segment).matches()) {
                throw new IllegalArgumentException("json condition path contains an invalid segment");
            }
        }
        if (kind == Kind.EXISTS || kind == Kind.PATH_NULL) {
            if (value != null) {
                throw new IllegalArgumentException("json path-only condition must not declare a compare value");
            }
        } else {
            value = Objects.requireNonNull(value, "json condition value is required");
        }
        if (kind == Kind.CONTAINS) {
            value = JsonValueCodec.write(value);
        }
        if (kind == Kind.PATH_EQUALS || kind == Kind.PATH_COMPARE || kind == Kind.ARRAY_CONTAINS) {
            value = scalarValue(value);
        }
        if (kind == Kind.PATH_PATTERN) {
            if (!(value instanceof CharSequence pattern)) {
                throw new IllegalArgumentException("json path LIKE pattern must be text");
            }
            value = pattern.toString();
        }
        if (kind == Kind.PATH_MEMBERSHIP) {
            value = membershipValues(value);
        }
    }

    static JsonConditionValue requireKind(Object value, Kind expected) {
        if (value instanceof JsonConditionValue condition && condition.kind() == expected) {
            return condition;
        }
        throw new IllegalArgumentException("json condition kind must be " + expected);
    }

    static List<String> parsePath(String path) {
        if (path.equals("$")) {
            return List.of();
        }
        int offset = path.startsWith("$.") ? 2 : path.startsWith("$[") ? 1 : 0;
        List<String> segments = new ArrayList<>();
        Matcher matcher = PATH_SEGMENT.matcher(path);
        do {
            if (!matcher.region(offset, path.length()).lookingAt()
                    || path.charAt(offset) == '[' && offset > 0 && path.charAt(offset - 1) == '.') {
                throw new IllegalArgumentException("json path requires names or nonnegative array indexes");
            }
            segments.add(matcher.group());
            offset = matcher.end();
            if (offset < path.length() && path.charAt(offset) == '.') {
                offset++;
                if (offset == path.length()) {
                    throw new IllegalArgumentException("json path must not end with a dot");
                }
            } else if (offset < path.length() && path.charAt(offset) != '[') {
                throw new IllegalArgumentException("json path requires a dot or array index after each segment");
            }
        } while (offset < path.length());
        return segments;
    }

    private static List<Object> membershipValues(Object source) {
        List<Object> values;
        if (source instanceof Collection<?> collection) {
            values = new ArrayList<>(collection.size());
            for (Object item : collection) {
                values.add(item == null ? null : scalarValue(item));
            }
        } else if (source.getClass().isArray()) {
            int size = Array.getLength(source);
            values = new ArrayList<>(size);
            for (int index = 0; index < size; index++) {
                Object item = Array.get(source, index);
                values.add(item == null ? null : scalarValue(item));
            }
        } else {
            throw new IllegalArgumentException("json path membership values must be a Collection or Java array");
        }
        return Collections.unmodifiableList(values);
    }

    private static Object scalarValue(Object value) {
        if (value instanceof CharSequence text) {
            return text.toString();
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

    /** 路径不等及大小比较值；保留 JSON 标量类型，使用数据库原生比较语义。 */
    public static JsonConditionValue pathCompare(List<String> path, Object value) {
        return new JsonConditionValue(Kind.PATH_COMPARE, path, value);
    }

    /** 路径文本的 LIKE/NOT LIKE 模式；不自动追加通配符或修剪空白。 */
    public static JsonConditionValue pathLike(List<String> path, CharSequence pattern) {
        return new JsonConditionValue(Kind.PATH_PATTERN, path, pattern);
    }

    /** 路径空值检查；缺失路径、JSON null 与整列 SQL NULL 均视为空，字符串 "null" 不为空。 */
    public static JsonConditionValue pathNull(List<String> path) {
        return new JsonConditionValue(Kind.PATH_NULL, path, null);
    }

    /** 路径 IN/NOT IN 集合；空集合有效，null 元素表示 JSON null，元素类型保持不变。 */
    public static JsonConditionValue pathIn(List<String> path, Object values) {
        return new JsonConditionValue(Kind.PATH_MEMBERSHIP, path, values);
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
        ARRAY_CONTAINS,
        PATH_COMPARE,
        PATH_PATTERN,
        PATH_NULL,
        PATH_MEMBERSHIP
    }
}
