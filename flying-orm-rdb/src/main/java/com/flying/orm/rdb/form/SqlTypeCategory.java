package com.flying.orm.rdb.form;

import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * 按完整数据库类型名进行严格分类，避免使用 {@code contains("INT")} 一类子串判断把
 * {@code INTERVAL}、{@code POINT} 等非数值类型误判为可执行算术更新的列。
 *
 * <p>分类前仅移除精度、长度以及 MySQL 的 {@code UNSIGNED}/{@code ZEROFILL} 修饰；数组和其他复合
 * 类型不会降级成其元素类型。该类型仅服务 SQL 生成前的本地校验，不替代数据库方言的类型系统。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
enum SqlTypeCategory {

    NUMERIC,
    OTHER;

    private static final Set<String> NUMERIC_TYPES = Set.of(
            "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT",
            "DEC", "DECIMAL", "NUMERIC", "NUMBER", "FLOAT", "REAL", "DOUBLE",
            "DOUBLE PRECISION", "BINARY_FLOAT", "BINARY_DOUBLE", "MONEY", "SMALLMONEY",
            "SERIAL", "BIGSERIAL");

    /**
     * 对数据库类型执行保守分类，未知类型一律返回 {@link #OTHER}。
     *
     * @param dataType 动态字段声明的数据库类型
     * @return 严格类型类别
     */
    static SqlTypeCategory of(String dataType) {
        String normalized = normalize(dataType);
        return NUMERIC_TYPES.contains(normalized) ? NUMERIC : OTHER;
    }

    private static String normalize(String dataType) {
        String type = Objects.requireNonNull(dataType, "SQL data type must not be null")
                             .trim()
                             .toUpperCase(Locale.ROOT);
        int arguments = type.indexOf('(');
        if (arguments >= 0) {
            type = type.substring(0, arguments).trim();
        }
        type = removeSuffix(type, " ZEROFILL");
        type = removeSuffix(type, " UNSIGNED");
        return type;
    }

    private static String removeSuffix(String type, String suffix) {
        return type.endsWith(suffix) ? type.substring(0, type.length() - suffix.length()).trim() : type;
    }
}
