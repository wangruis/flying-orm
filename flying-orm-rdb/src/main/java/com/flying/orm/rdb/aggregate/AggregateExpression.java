package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.type.LogicalType;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * 一项带稳定类型契约的聚合表达式。
 *
 * <p>COUNT 固定返回 {@link Long}，SUM/AVG 及标准差、方差规范成 {@link BigDecimal}。MIN/MAX 对直接字段
 * 沿用其逻辑类型和 codec，对计算结果使用编译后的类型；planner 核对调用方声明的 Java 类型。</p>
 *
 * @param function 受控函数
 * @param alias 唯一结果别名
 * @param resultLogicalType 稳定逻辑结果类型
 * @param javaType 稳定 Java 结果类型
 * @param operand 字段或计算表达式；仅 COUNT_ROWS 使用 null
 * @param <T> AggregateRow.get 返回的类型
 * @author wangr
 * @version v3.2
 */
public record AggregateExpression<T>(AggregateFunction function,
                                     ValueExpression operand,
                                     String alias,
                                     LogicalType resultLogicalType,
                                     Class<T> javaType) {

    public AggregateExpression {
        function = Objects.requireNonNull(function, "aggregate function must not be null");
        if (function == AggregateFunction.REGISTERED && !(operand instanceof ValueExpression.Call)) {
            throw new IllegalArgumentException("registered aggregate requires a function call");
        }
        if (function == AggregateFunction.COUNT_ROWS) {
            if (operand != null) {
                throw new IllegalArgumentException("COUNT_ROWS must not declare a source field");
            }
        } else {
            operand = Objects.requireNonNull(operand, "aggregate operand must not be null");
        }
        alias = Names.requireText(alias, "field name");
        resultLogicalType = Objects.requireNonNull(
                resultLogicalType, "aggregate result logical type must not be null");
        javaType = Objects.requireNonNull(javaType, "aggregate result Java type must not be null");
        validateContract(function, resultLogicalType, javaType);
    }

    /** 统计过滤后的行数，包含字段为 NULL 的行；不读取或继承任一字段的展示策略。 */
    public static AggregateExpression<Long> countRows(String alias) {
        return new AggregateExpression<>(
                AggregateFunction.COUNT_ROWS, null, alias, LogicalType.BIG_INTEGER, Long.class);
    }

    /** 统计指定字段非 NULL 的行数。 */
    public static AggregateExpression<Long> count(String field, String alias) {
        return count(ValueExpression.field(field), alias);
    }

    public static AggregateExpression<Long> countDistinct(String field, String alias) {
        return countDistinct(ValueExpression.field(field), alias);
    }

    /** 统计表达式结果非 NULL 的行数；不同于 COUNT(*)。 */
    public static AggregateExpression<Long> count(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.COUNT, operand, alias, LogicalType.BIG_INTEGER, Long.class);
    }

    /** 统计表达式结果去重后的非 NULL 值数量。 */
    public static AggregateExpression<Long> countDistinct(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.COUNT_DISTINCT, operand, alias, LogicalType.BIG_INTEGER, Long.class);
    }

    public static AggregateExpression<BigDecimal> sum(String field, String alias) {
        return sum(ValueExpression.field(field), alias);
    }

    public static AggregateExpression<BigDecimal> avg(String field, String alias) {
        return avg(ValueExpression.field(field), alias);
    }

    /** 对参数化数值表达式求和，字段治理覆盖所有操作数字段。 */
    public static AggregateExpression<BigDecimal> sum(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.SUM, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    /** 对参数化数值表达式求平均值，结果沿用 BigDecimal 契约。 */
    public static AggregateExpression<BigDecimal> avg(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.AVG, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    /** 总体标准差，忽略 NULL；结果转换不增加数据库计算精度。 */
    public static AggregateExpression<BigDecimal> stddevPop(String field, String alias) {
        return stddevPop(ValueExpression.field(field), alias);
    }

    /** 样本标准差，非空值不足两个时返回 NULL。 */
    public static AggregateExpression<BigDecimal> stddevSample(String field, String alias) {
        return stddevSample(ValueExpression.field(field), alias);
    }

    /** 总体方差，忽略 NULL；结果转换不增加数据库计算精度。 */
    public static AggregateExpression<BigDecimal> variancePop(String field, String alias) {
        return variancePop(ValueExpression.field(field), alias);
    }

    /** 样本方差，非空值不足两个时返回 NULL。 */
    public static AggregateExpression<BigDecimal> varianceSample(String field, String alias) {
        return varianceSample(ValueExpression.field(field), alias);
    }

    /** 参数化数值表达式的总体标准差。 */
    public static AggregateExpression<BigDecimal> stddevPop(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.STDDEV_POP, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    /** 参数化数值表达式的样本标准差。 */
    public static AggregateExpression<BigDecimal> stddevSample(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.STDDEV_SAMPLE, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    /** 参数化数值表达式的总体方差。 */
    public static AggregateExpression<BigDecimal> variancePop(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.VARIANCE_POP, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    /** 参数化数值表达式的样本方差。 */
    public static AggregateExpression<BigDecimal> varianceSample(ValueExpression operand, String alias) {
        return new AggregateExpression<>(AggregateFunction.VARIANCE_SAMPLE, operand, alias, LogicalType.DECIMAL, BigDecimal.class);
    }

    public static <T> AggregateExpression<T> min(String field,
                                                  String alias,
                                                  LogicalType logicalType,
                                                  Class<T> javaType) {
        return min(ValueExpression.field(field), alias, logicalType, javaType);
    }

    public static <T> AggregateExpression<T> max(String field,
                                                  String alias,
                                                  LogicalType logicalType,
                                                  Class<T> javaType) {
        return max(ValueExpression.field(field), alias, logicalType, javaType);
    }

    /** 参数化表达式的最小值；声明类型必须与编译后的操作数类型一致。 */
    public static <T> AggregateExpression<T> min(ValueExpression operand, String alias,
                                                LogicalType logicalType, Class<T> javaType) {
        return new AggregateExpression<>(AggregateFunction.MIN, operand, alias, logicalType, javaType);
    }

    /** 参数化表达式的最大值；不把文本、时间等结果强制转成数值。 */
    public static <T> AggregateExpression<T> max(ValueExpression operand, String alias,
                                                LogicalType logicalType, Class<T> javaType) {
        return new AggregateExpression<>(AggregateFunction.MAX, operand, alias, logicalType, javaType);
    }

    /**
     * 调用后端已注册的聚合函数，复用普通表达式的参数绑定和字段来源治理。
     * ID 不是 SQL 名称；注册方负责方言与聚合语义，返回类型由编译器核对，Java 类型沿用结果 codec。
     */
    public static <T> AggregateExpression<T> function(String id, String alias,
                                                     LogicalType logicalType, Class<T> javaType,
                                                     ValueExpression... arguments) {
        return new AggregateExpression<>(AggregateFunction.REGISTERED, ValueExpression.call(id, arguments),
                alias, logicalType, javaType);
    }

    private static void validateContract(AggregateFunction function,
                                         LogicalType logicalType,
                                         Class<?> javaType) {
        switch (function) {
            case REGISTERED -> {
                // 注册函数的结果类型由共享表达式编译器在规划时提供。
            }
            case COUNT_ROWS, COUNT, COUNT_DISTINCT -> {
                if (logicalType != LogicalType.BIG_INTEGER || javaType != Long.class) {
                    throw new IllegalArgumentException("COUNT aggregate must return Long/BIG_INTEGER");
                }
            }
            case SUM, AVG, STDDEV_POP, STDDEV_SAMPLE, VARIANCE_POP, VARIANCE_SAMPLE -> {
                if (logicalType != LogicalType.DECIMAL || javaType != BigDecimal.class) {
                    throw new IllegalArgumentException("numeric aggregates must return BigDecimal/DECIMAL");
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
