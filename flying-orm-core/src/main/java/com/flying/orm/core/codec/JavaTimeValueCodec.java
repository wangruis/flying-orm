package com.flying.orm.core.codec;

import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

/**
 * 统一处理传统 JDBC 时间对象、java.time 对象和 ISO 文本。
 * Timestamp 转带偏移的类型固定按 UTC 解释，不依赖部署机器时区。
 */
final class JavaTimeValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == LocalDate.class
                || targetType == LocalDateTime.class
                || targetType == LocalTime.class
                || targetType == Instant.class
                || targetType == OffsetDateTime.class
                || targetType == OffsetTime.class;
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        try {
            if (targetType == LocalDate.class) {
                return localDate(value);
            }
            if (targetType == LocalDateTime.class) {
                return localDateTime(value);
            }
            if (targetType == LocalTime.class) {
                return localTime(value);
            }
            if (targetType == Instant.class) {
                return instant(value);
            }
            if (targetType == OffsetDateTime.class) {
                return offsetDateTime(value);
            }
            if (targetType == OffsetTime.class) {
                return offsetTime(value);
            }
        } catch (DateTimeException failure) {
            ValueCodecTypeSupport.rethrowVirtualMachineError(failure);
            throw new IllegalArgumentException("value cannot be converted to java time type");
        }
        throw new IllegalArgumentException("java time type is not supported: " + targetType.getName());
    }

    private static LocalDate localDate(Object value) {
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.toLocalDate();
        }
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.toLocalDate();
        }
        return LocalDate.parse(ValueCodecTypeSupport.text(value));
    }

    private static LocalDateTime localDateTime(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toLocalDateTime();
        }
        if (value instanceof LocalDate date) {
            return date.atStartOfDay();
        }
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.toLocalDateTime();
        }
        return LocalDateTime.parse(ValueCodecTypeSupport.text(value));
    }

    private static LocalTime localTime(Object value) {
        if (value instanceof java.sql.Time time) {
            return time.toLocalTime();
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.toLocalTime();
        }
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.toLocalTime();
        }
        return LocalTime.parse(ValueCodecTypeSupport.text(value));
    }

    private static Instant instant(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.toInstant();
        }
        return Instant.parse(ValueCodecTypeSupport.text(value));
    }

    private static OffsetDateTime offsetDateTime(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant().atOffset(ZoneOffset.UTC);
        }
        if (value instanceof Instant instant) {
            return instant.atOffset(ZoneOffset.UTC);
        }
        return OffsetDateTime.parse(ValueCodecTypeSupport.text(value));
    }

    private static OffsetTime offsetTime(Object value) {
        if (value instanceof OffsetDateTime dateTime) {
            return dateTime.toOffsetTime();
        }
        return OffsetTime.parse(ValueCodecTypeSupport.text(value));
    }
}
