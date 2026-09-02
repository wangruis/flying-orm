package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.internal.InternalApi;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Standard JDBC legacy temporal values converted at the RDB read boundary.
 *
 * @author wangr
 * @version v1.0
 */
@InternalApi
public final class JdbcLegacyTemporalAdapter {

    private static final BiFunction<Object, Class<?>, Object> STANDARD_FALLBACK =
            JdbcLegacyTemporalAdapter::adapt;

    private JdbcLegacyTemporalAdapter() {
    }

    public static <T> T read(ValueCodecRegistry codecs, Object value, Class<T> targetType) {
        return Objects.requireNonNull(codecs, "value codec registry must not be null")
                      .read(value, targetType, STANDARD_FALLBACK);
    }

    private static Object adapt(Object value, Class<?> targetType) {
        if (value == null) {
            return null;
        }
        Class<?> safeTarget = Objects.requireNonNull(targetType, "temporal target type must not be null");
        if (value instanceof Timestamp timestamp) {
            if (safeTarget == LocalDateTime.class) {
                return timestamp.toLocalDateTime();
            }
            if (safeTarget == Instant.class || safeTarget == OffsetDateTime.class) {
                return timestamp.toInstant();
            }
            return value;
        }
        if (safeTarget == LocalDate.class && value instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (safeTarget == LocalTime.class && value instanceof Time time) {
            return time.toLocalTime();
        }
        return value;
    }
}
