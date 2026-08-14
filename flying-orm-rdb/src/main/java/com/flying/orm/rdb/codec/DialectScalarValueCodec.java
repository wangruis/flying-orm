package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Locale;
import java.util.Objects;

/**
 * 动态字段的常用标量适配器。它把逻辑字段类型转换成驱动绑定需要的 Java 类型，也把驱动返回的
 * BigDecimal、Timestamp 等对象收口成稳定的 Java 值。
 *
 * <p>Oracle 23ai 之前没有 SQL BOOLEAN，逻辑 BOOLEAN 写入 NUMBER(1) 时明确绑定 1/0；23ai 和
 * SQL Server BIT 则继续绑定 Boolean。规则同时被单条写入、批量布局和表单结果读取复用，避免三条链路各自猜类型。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class DialectScalarValueCodec {

    private DialectScalarValueCodec() {
    }

    public static boolean supports(String dataType) {
        String type = normalize(dataType);
        return type.contains("BIGINT")
                || type.equals("INT")
                || type.startsWith("INT(")
                || type.contains("INTEGER")
                || type.contains("DECIMAL")
                || type.contains("NUMERIC")
                || type.contains("DOUBLE")
                || type.contains("FLOAT")
                || type.contains("REAL")
                || type.contains("BOOL")
                || type.contains("TIMESTAMP")
                || type.contains("DATETIME")
                || type.equals("DATE")
                || type.equals("TIME");
    }

    public static Class<?> parameterType(String dataType, String dialectName, boolean nativeBoolean) {
        String type = normalize(dataType);
        if (type.contains("BIGINT")) {
            return Long.class;
        }
        if (type.equals("INT") || type.startsWith("INT(") || type.contains("INTEGER")) {
            return Integer.class;
        }
        if (type.contains("DECIMAL") || type.contains("NUMERIC")) {
            return BigDecimal.class;
        }
        if (type.contains("DOUBLE") || type.contains("FLOAT") || type.contains("REAL")) {
            return Double.class;
        }
        if (type.contains("BOOL")) {
            return isLegacyOracleBoolean(dialectName, nativeBoolean) ? Integer.class : Boolean.class;
        }
        if (type.contains("TIMESTAMP") || type.contains("DATETIME")) {
            return LocalDateTime.class;
        }
        if (type.equals("DATE")) {
            return LocalDate.class;
        }
        if (type.equals("TIME")) {
            return LocalTime.class;
        }
        return Object.class;
    }

    public static Object write(Object value,
                               String dataType,
                               String dialectName,
                               boolean nativeBoolean,
                               ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        Class<?> targetType = parameterType(dataType, dialectName, nativeBoolean);
        // 普通数字和时间值不在这里强转。动态表单允许上层 codec 或驱动接收更宽的值形态，
        // 例如某些系统会给 BIGINT 主键传字符串。这里贸然转成 Long，反而会把原本能工作的调用拦在 SQL 之前。
        // Boolean 是例外：Oracle 旧版本要明确写成 NUMBER(1) 的 1/0，不能交给不同驱动各自猜。
        if (targetType == Boolean.class || (targetType == Integer.class && normalize(dataType).contains("BOOL"))) {
            Boolean bool = codecs.read(value, Boolean.class);
            return targetType == Integer.class ? (bool ? 1 : 0) : bool;
        }
        return codecs.write(value);
    }

    public static Object read(Object value, String dataType, ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        String type = normalize(dataType);
        Class<?> targetType = parameterType(type, "generic", true);
        if (targetType == Object.class) {
            return value;
        }
        if (targetType == Boolean.class) {
            return codecs.read(value, Boolean.class);
        }
        // 真正的数据库驱动通常把 NUMBER、Timestamp 这类值作为 Number 或 java.sql 时间对象返回，
        // 这时统一成稳定的 Java 类型很有价值。字符串可能来自自定义执行器，也可能是业务刻意保留的值，
        // 不应该仅凭字段声明就强行解析，避免把兼容性问题伪装成类型转换异常。
        if (isNumeric(type) && !(value instanceof Number)) {
            return value;
        }
        if (isTemporal(type) && value instanceof CharSequence) {
            return value;
        }
        return codecs.read(value, targetType);
    }

    private static boolean isNumeric(String type) {
        return type.contains("BIGINT")
                || type.equals("INT")
                || type.startsWith("INT(")
                || type.contains("INTEGER")
                || type.contains("DECIMAL")
                || type.contains("NUMERIC")
                || type.contains("DOUBLE")
                || type.contains("FLOAT")
                || type.contains("REAL");
    }

    private static boolean isTemporal(String type) {
        return type.contains("TIMESTAMP")
                || type.contains("DATETIME")
                || type.equals("DATE")
                || type.equals("TIME");
    }

    private static boolean isLegacyOracleBoolean(String dialectName, boolean nativeBoolean) {
        return "ORACLE".equals(normalize(dialectName)) && !nativeBoolean;
    }

    private static String normalize(String value) {
        return Objects.requireNonNull(value, "value must not be null").trim().toUpperCase(Locale.ROOT);
    }
}
