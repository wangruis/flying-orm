package com.flying.orm.core.metadata;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * 不携带任意 SQL 文本的受控列默认值。
 *
 * <p>字面量只接受稳定、不可变的常用标量；数据库时间关键字由明确的 kind 表达。方言编译器因此不需要
 * 把调用方字符串当 SQL 拼接，也不会在生成 DDL 时重新猜测值的含义。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class ColumnDefault {

    /** ORM 能跨方言理解的默认值种类。 */
    public enum Kind {
        NONE,
        LITERAL,
        CURRENT_DATE,
        CURRENT_TIME,
        CURRENT_TIMESTAMP
    }

    private static final ColumnDefault NONE = new ColumnDefault(Kind.NONE, null);
    private static final ColumnDefault CURRENT_DATE = new ColumnDefault(Kind.CURRENT_DATE, null);
    private static final ColumnDefault CURRENT_TIME = new ColumnDefault(Kind.CURRENT_TIME, null);
    private static final ColumnDefault CURRENT_TIMESTAMP = new ColumnDefault(Kind.CURRENT_TIMESTAMP, null);

    private final Kind kind;

    private final Object value;

    private ColumnDefault(Kind kind, Object value) {
        this.kind = Objects.requireNonNull(kind, "column default kind must not be null");
        this.value = value;
    }

    public static ColumnDefault none() {
        return NONE;
    }

    /** 创建受控字面量；null 应通过 nullable 表达，不作为 DDL 默认值。 */
    public static ColumnDefault literal(Object value) {
        return new ColumnDefault(Kind.LITERAL, requireLiteral(value));
    }

    public static ColumnDefault currentDate() {
        return CURRENT_DATE;
    }

    public static ColumnDefault currentTime() {
        return CURRENT_TIME;
    }

    public static ColumnDefault currentTimestamp() {
        return CURRENT_TIMESTAMP;
    }

    public Kind kind() {
        return kind;
    }

    public Optional<Object> value() {
        return Optional.ofNullable(value);
    }

    @Override
    public boolean equals(Object candidate) {
        return this == candidate || candidate instanceof ColumnDefault other
                && kind == other.kind && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, value);
    }

    @Override
    public String toString() {
        return value == null ? kind.name() : kind.name() + '[' + value.toString() + ']';
    }

    private static Object requireLiteral(Object value) {
        Object safeValue = Objects.requireNonNull(value, "column default literal must not be null");
        if ((safeValue instanceof Float floatValue && !Float.isFinite(floatValue))
                || (safeValue instanceof Double doubleValue && !Double.isFinite(doubleValue))) {
            throw new IllegalArgumentException("column default floating-point literal must be finite");
        }
        if (safeValue instanceof Enum<?> enumValue) {
            return enumValue.name();
        }
        if (safeValue instanceof String
                || safeValue instanceof Boolean
                || safeValue instanceof Byte
                || safeValue instanceof Short
                || safeValue instanceof Integer
                || safeValue instanceof Long
                || safeValue instanceof Float
                || safeValue instanceof Double
                || safeValue instanceof BigInteger
                || safeValue instanceof BigDecimal
                || safeValue instanceof UUID
                || safeValue instanceof LocalDate
                || safeValue instanceof LocalTime
                || safeValue instanceof LocalDateTime
                || safeValue instanceof OffsetTime
                || safeValue instanceof OffsetDateTime
                || safeValue instanceof Instant) {
            return safeValue;
        }
        throw new IllegalArgumentException("column default literal type is not supported");
    }
}
