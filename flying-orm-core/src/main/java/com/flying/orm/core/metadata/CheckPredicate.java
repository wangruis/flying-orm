package com.flying.orm.core.metadata;

import com.flying.orm.core.internal.Names;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * CHECK 约束使用的封闭谓词模型。
 *
 * <p>这里只接受列、运算符和受控字面量，不提供原始 SQL 入口。方言可以据此安全渲染，调用方也不会把
 * 数据库专用 SQL 偶然带进规范元数据。</p>
 *
 * @author wangr
 * @version v3.2
 */
public sealed interface CheckPredicate permits CheckPredicate.Comparison,
        CheckPredicate.Range,
        CheckPredicate.In,
        CheckPredicate.NullCheck,
        CheckPredicate.Logical,
        CheckPredicate.Negation {

    static CheckPredicate compare(String column, ComparisonOperator operator, Object value) {
        return new Comparison(column, operator, value);
    }

    /** 创建包含上下界的范围谓词。 */
    static CheckPredicate range(String column, Object lower, Object upper) {
        return new Range(column, lower, true, upper, true);
    }

    /** 创建可以分别控制上下界开闭的范围谓词。 */
    static CheckPredicate range(String column,
                                Object lower,
                                boolean lowerInclusive,
                                Object upper,
                                boolean upperInclusive) {
        return new Range(column, lower, lowerInclusive, upper, upperInclusive);
    }

    static CheckPredicate in(String column, Collection<?> values) {
        return new In(column, values == null ? null : List.copyOf(values));
    }

    static CheckPredicate isNull(String column) {
        return new NullCheck(column, false);
    }

    static CheckPredicate isNotNull(String column) {
        return new NullCheck(column, true);
    }

    static CheckPredicate and(CheckPredicate... predicates) {
        return new Logical(LogicalOperator.AND, Arrays.asList(predicates));
    }

    static CheckPredicate or(CheckPredicate... predicates) {
        return new Logical(LogicalOperator.OR, Arrays.asList(predicates));
    }

    static CheckPredicate not(CheckPredicate predicate) {
        return new Negation(predicate);
    }

    /** 单列比较。value 在对象进入元数据时就完成受控类型校验。 */
    record Comparison(String column, ComparisonOperator operator, Object value) implements CheckPredicate {

        public Comparison {
            column = requireColumnName(column);
            operator = Objects.requireNonNull(operator, "check comparison operator must not be null");
            value = literal(value);
        }
    }

    /** 单列范围；上下界均为受控字面量。 */
    record Range(String column,
                 Object lower,
                 boolean lowerInclusive,
                 Object upper,
                 boolean upperInclusive) implements CheckPredicate {

        public Range {
            column = requireColumnName(column);
            lower = literal(lower);
            upper = literal(upper);
        }
    }

    /** 单列集合判断；集合在构造时复制，后续修改调用方集合不会污染约束。 */
    record In(String column, List<Object> values) implements CheckPredicate {

        public In(String column, Collection<?> values) {
            this(column, normalizeValues(values));
        }

        public In {
            column = requireColumnName(column);
            values = normalizeValues(values);
        }
    }

    /** NULL 判断；negated 为 true 表示 IS NOT NULL。 */
    record NullCheck(String column, boolean negated) implements CheckPredicate {

        public NullCheck {
            column = requireColumnName(column);
        }
    }

    /** AND 或 OR 组合。子谓词顺序保留，便于稳定指纹和可读 DDL。 */
    record Logical(LogicalOperator operator, List<CheckPredicate> predicates) implements CheckPredicate {

        public Logical {
            operator = Objects.requireNonNull(operator, "check logical operator must not be null");
            if (predicates == null) {
                throw new IllegalArgumentException("check logical predicates must not be null");
            }
            predicates = List.copyOf(predicates);
            if (predicates.size() < 2) {
                throw new IllegalArgumentException("check logical predicate requires at least two children");
            }
        }
    }

    /** 对一个结构化谓词取反。 */
    record Negation(CheckPredicate predicate) implements CheckPredicate {

        public Negation {
            predicate = Objects.requireNonNull(predicate, "negated check predicate must not be null");
        }
    }

    enum ComparisonOperator {
        EQUAL,
        NOT_EQUAL,
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL
    }

    enum LogicalOperator {
        AND,
        OR
    }

    private static String requireColumnName(String value) {
        return Names.requireText(value, "check predicate column name");
    }

    private static List<Object> normalizeValues(Collection<?> values) {
        if (values == null) {
            throw new IllegalArgumentException("check predicate values must not be null");
        }
        List<Object> normalized = values.stream().map(CheckPredicate::literal).toList();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("check predicate values must not be empty");
        }
        return normalized;
    }

    /**
     * 收口可移植的字面量类型。拒绝任意对象，避免对象的 toString 被误当成 SQL 或数据库表达式。
     */
    private static Object literal(Object value) {
        if (value instanceof String
                || value instanceof Boolean
                || value instanceof Byte
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof BigInteger
                || value instanceof BigDecimal
                || value instanceof LocalDate
                || value instanceof LocalTime
                || value instanceof LocalDateTime
                || value instanceof OffsetTime
                || value instanceof OffsetDateTime
                || value instanceof Instant
                || value instanceof UUID) {
            return value;
        }
        if (value instanceof Character character) {
            return character.toString();
        }
        if (value instanceof Float number && Float.isFinite(number)) {
            return value;
        }
        if (value instanceof Double number && Double.isFinite(number)) {
            return value;
        }
        throw new IllegalArgumentException("check predicate literal type is not supported");
    }
}
