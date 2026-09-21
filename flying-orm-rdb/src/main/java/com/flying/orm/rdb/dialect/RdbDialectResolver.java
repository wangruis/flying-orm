package com.flying.orm.rdb.dialect;

import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Optional;

/**
 * 将上层显式配置的方言名称解析为内置 SQL 方言，不读取连接、工厂、URL 或驱动。
 *
 * @author wangr
 * @version v4.1.0
 */
public final class RdbDialectResolver {

    private RdbDialectResolver() {
    }

    /**
     * @param name 上层显式选择的方言名称或已支持别名
     * @return 对应内置方言；名称未知时为空
     */
    public static Optional<RdbDialect> tryResolveName(String name) {
        return switch (DatabaseProduct.fromName(name)) {
            case H2 -> Optional.of(RdbDialect.h2());
            case MYSQL -> Optional.of(RdbDialect.mysql());
            case POSTGRESQL -> Optional.of(RdbDialect.postgresql());
            case ORACLE -> Optional.of(RdbDialect.oracle());
            case SQL_SERVER -> Optional.of(RdbDialect.sqlServer());
            case UNKNOWN -> Optional.empty();
        };
    }
}
