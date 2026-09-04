package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.type.LogicalType;

/** Renders the SQL expression owned by one validated aggregate function. */
final class AggregateFunctionSqlRenderer {

    private AggregateFunctionSqlRenderer() {
    }

    static String render(AggregateFunction function,
                         String field,
                         LogicalType sourceType,
                         boolean sqlServerDialect) {
        return switch (function) {
            case COUNT -> "count(" + field + ")";
            case COUNT_DISTINCT -> "count(distinct " + field + ")";
            // SQL Server 会把整数 SUM/AVG 保留在源整数类型族。聚合前提升输入，
            // 同时避免 SUM 溢出和 AVG 整除截断；聚合后再 cast 已无法恢复丢失的事实。
            case SUM -> "sum(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case AVG -> "avg(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case MIN -> "min(" + field + ")";
            case MAX -> "max(" + field + ")";
        };
    }

    private static String stableDecimalInput(String field,
                                             LogicalType sourceType,
                                             boolean sqlServerDialect) {
        boolean integerSource = switch (sourceType) {
            case SMALL_INTEGER, INTEGER, BIG_INTEGER -> true;
            default -> false;
        };
        return sqlServerDialect && integerSource
                ? "cast(" + field + " as decimal(38,10))"
                : field;
    }
}
