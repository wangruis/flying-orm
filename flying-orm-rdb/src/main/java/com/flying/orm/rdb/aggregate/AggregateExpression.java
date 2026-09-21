package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.type.LogicalType;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 一项带稳定类型契约的聚合表达式。
 *
 * <p>COUNT 固定返回 {@link Long}，SUM/AVG 固定规范成 {@link BigDecimal}。MIN/MAX 沿用源字段
 * 的逻辑类型和 codec，因此调用方需声明期望 Java 类型，planner 会再与真实字段元数据核对。</p>
 *
 * @param function 受控函数
 * @param sourceField 规范源字段
 * @param alias 唯一结果别名
 * @param resultLogicalType 稳定逻辑结果类型
 * @param javaType 稳定 Java 结果类型
 * @param <T> AggregateRow.get 返回的类型
 * @author wangr
 * @version v3.2
 */
public record AggregateExpression<T>(AggregateFunction function,
                                     String sourceField,
                                     String alias,
                                     LogicalType resultLogicalType,
                                     Class<T> javaType) {

    public AggregateExpression {
        function = Objects.requireNonNull(function, "aggregate function must not be null");
        sourceField = FieldIdentity.of(sourceField).name();
        alias = FieldIdentity.of(alias).name();
        resultLogicalType = Objects.requireNonNull(
                resultLogicalType, "aggregate result logical type must not be null");
        javaType = Objects.requireNonNull(javaType, "aggregate result Java type must not be null");
        validateContract(function, resultLogicalType, javaType);
    }

    public static AggregateExpression<Long> count(String field, String alias) {
        return new AggregateExpression<>(
                AggregateFunction.COUNT, field, alias, LogicalType.BIG_INTEGER, Long.class);
    }

    public static AggregateExpression<Long> countDistinct(String field, String alias) {
        return new AggregateExpression<>(
                AggregateFunction.COUNT_DISTINCT, field, alias, LogicalType.BIG_INTEGER, Long.class);
    }

    public static AggregateExpression<BigDecimal> sum(String field, String alias) {
        return new AggregateExpression<>(
                AggregateFunction.SUM, field, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    public static AggregateExpression<BigDecimal> avg(String field, String alias) {
        return new AggregateExpression<>(
                AggregateFunction.AVG, field, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    public static <T> AggregateExpression<T> min(String field,
                                                  String alias,
                                                  LogicalType logicalType,
                                                  Class<T> javaType) {
        return new AggregateExpression<>(AggregateFunction.MIN, field, alias, logicalType, javaType);
    }

    public static <T> AggregateExpression<T> max(String field,
                                                  String alias,
                                                  LogicalType logicalType,
                                                  Class<T> javaType) {
        return new AggregateExpression<>(AggregateFunction.MAX, field, alias, logicalType, javaType);
    }

    private static void validateContract(AggregateFunction function,
                                         LogicalType logicalType,
                                         Class<?> javaType) {
        switch (function) {
            case COUNT, COUNT_DISTINCT -> {
                if (logicalType != LogicalType.BIG_INTEGER || javaType != Long.class) {
                    throw new IllegalArgumentException("COUNT aggregate must return Long/BIG_INTEGER");
                }
            }
            case SUM, AVG -> {
                if (logicalType != LogicalType.DECIMAL || javaType != BigDecimal.class) {
                    throw new IllegalArgumentException("SUM and AVG aggregates must return BigDecimal/DECIMAL");
                }
            }
            case MIN, MAX -> {
                if (!comparable(logicalType)
                        || !Comparable.class.isAssignableFrom(javaType)) {
                    throw new IllegalArgumentException(
                            "MIN and MAX require a comparable logical and Java result type");
                }
            }
        }
    }

    private static boolean comparable(LogicalType logicalType) {
        return switch (logicalType) {
            case SMALL_INTEGER, INTEGER, BIG_INTEGER, DECIMAL, FLOAT,
                    TEXT, DATE, TIME, OFFSET_TIME, TIMESTAMP, OFFSET_TIMESTAMP, UUID -> true;
            case BOOLEAN, BINARY, JSON, XML, VECTOR, INTERVAL, OTHER -> false;
        };
    }
}
