package com.flying.orm.rdb.internal.binding;

import com.flying.orm.rdb.internal.InternalApi;

import java.util.Objects;

/**
 * 在共享 SQL 请求中携带空值的 Java 类型，不引入 JDBC 或 R2DBC 驱动对象。
 *
 * @param javaType 驱动绑定空值时使用的非原始 Java 类型
 * @author wangr
 * @date 2026-08-25
 * @version v3.1
 */
@InternalApi
public record SqlNullParameter(Class<?> javaType) {

    public SqlNullParameter {
        javaType = Objects.requireNonNull(javaType, "SQL null parameter type must not be null");
        if (javaType.isPrimitive() || javaType == Void.class) {
            throw new IllegalArgumentException("SQL null parameter type must be a non-primitive value type");
        }
    }
}
