package com.flying.orm.rdb.json;

import java.util.Objects;

/**
 * JSON 参数在 SQL 里怎么写。Java 侧始终绑定 JSON 文本，数据库方言只补自己需要的类型提示。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
@FunctionalInterface
public interface JsonDialect {

    JsonDialect PLAIN = bindMarker -> requireBindMarker(bindMarker);

    // H2 不加 FORMAT JSON 时，会把绑定的 JSON 文本当成一个普通字符串塞进 JSON 列。
    JsonDialect H2 = bindMarker -> requireBindMarker(bindMarker) + " format json";

    JsonDialect POSTGRESQL = bindMarker -> "cast(" + requireBindMarker(bindMarker) + " as jsonb)";

    /**
     * 把一个普通绑定标记变成数据库能识别的 JSON 值表达式。
     *
     * @param bindMarker 一般是 {@code ?}
     * @return 可以直接放进 values 或 set 后面的表达式
     */
    String valueExpression(String bindMarker);

    static JsonDialect plain() {
        return PLAIN;
    }

    static JsonDialect h2() {
        return H2;
    }

    static JsonDialect postgresql() {
        return POSTGRESQL;
    }

    private static String requireBindMarker(String bindMarker) {
        String marker = Objects.requireNonNull(bindMarker, "json bind marker must not be null").trim();
        if (marker.isEmpty()) {
            throw new IllegalArgumentException("json bind marker must not be blank");
        }
        return marker;
    }
}
