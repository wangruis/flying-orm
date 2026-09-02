package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.type.DatabaseTypes;

import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Converts scalar form values at the database-driver boundary.
 *
 * <p>Type syntax is parsed once by {@link DatabaseType}. This adapter contains only genuine driver differences:
 * legacy Oracle booleans, MySQL {@code BIT(1)}, MySQL UTC timestamp binding and Oracle text-backed local time.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v1.0
 */
public final class DialectScalarValueCodec {

    private DialectScalarValueCodec() {
    }

    /** @return whether the scalar codec has an explicit contract for the declaration */
    public static boolean supports(String dataType) {
        return supports(DatabaseType.of(dataType));
    }

    /** @return whether the scalar codec has an explicit contract for the parsed type */
    public static boolean supports(DatabaseType dataType) {
        return DatabaseTypes.supportsScalar(dataType, "GENERIC");
    }

    /** @return Java type expected by the current database driver */
    public static Class<?> parameterType(String dataType, String dialectName, boolean nativeBoolean) {
        return parameterType(DatabaseType.of(dataType), dialectName, nativeBoolean);
    }

    /** @return Java type expected by the current database driver */
    public static Class<?> parameterType(DatabaseType dataType, String dialectName, boolean nativeBoolean) {
        return DatabaseTypes.parameterType(dataType, dialectName, nativeBoolean);
    }

    /** Converts one value before binding it to a statement. */
    public static Object write(Object value,
                               String dataType,
                               String dialectName,
                               boolean nativeBoolean,
                               ValueCodecRegistry valueCodecs) {
        return write(value, DatabaseType.of(dataType), dialectName, nativeBoolean, valueCodecs);
    }

    /** Converts one value before binding it to a statement without reparsing field metadata. */
    public static Object write(Object value,
                               DatabaseType dataType,
                               String dialectName,
                               boolean nativeBoolean,
                               ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        LogicalType logicalType = DatabaseTypes.effectiveLogicalType(dataType, dialectName);
        Class<?> targetType = DatabaseTypes.parameterType(dataType, dialectName, nativeBoolean);
        if (logicalType == LogicalType.INTERVAL
                && targetType != Object.class
                && targetType.isInstance(value)) {
            return value;
        }
        Object encoded = codecs.write(value);

        if (logicalType == LogicalType.BOOLEAN) {
            Boolean bool = codecs.read(encoded, Boolean.class);
            return targetType == Integer.class ? (bool ? 1 : 0) : bool;
        }
        if (logicalType == LogicalType.OFFSET_TIMESTAMP) {
            OffsetDateTime absolute = JdbcLegacyTemporalAdapter.read(
                    codecs, encoded, OffsetDateTime.class);
            return DatabaseTypes.mysqlOffsetTimestamp(dataType, dialectName)
                    ? absolute.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()
                    : absolute;
        }
        if (logicalType == LogicalType.TIME && targetType == String.class) {
            return JdbcLegacyTemporalAdapter.read(codecs, encoded, LocalTime.class).toString();
        }
        if (logicalType == LogicalType.INTERVAL && targetType != Object.class) {
            if (!targetType.isInstance(encoded)) {
                throw new IllegalArgumentException(
                        "interval value must be " + targetType.getName() + ", but was "
                                + encoded.getClass().getName());
            }
            return encoded;
        }
        return encoded;
    }

    /** Converts one scalar value returned by a database driver. */
    public static Object read(Object value, String dataType, ValueCodecRegistry valueCodecs) {
        return read(value, DatabaseType.of(dataType), valueCodecs);
    }

    /** Converts one scalar value returned by a database driver without reparsing field metadata. */
    public static Object read(Object value, DatabaseType dataType, ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        LogicalType logicalType = DatabaseTypes.effectiveLogicalType(dataType, "GENERIC");
        Class<?> targetType = DatabaseTypes.parameterType(dataType, "GENERIC", true);
        if (targetType == Object.class) {
            return value;
        }
        if (logicalType == LogicalType.BOOLEAN) {
            return codecs.read(value, Boolean.class);
        }
        if (logicalType.numeric()) {
            if (value instanceof Boolean) {
                throw new IllegalArgumentException(
                        "numeric driver value must not be Boolean; preserve the driver's numeric mapping");
            }
            if (!(value instanceof Number)) {
                return value;
            }
        }
        if (logicalType.temporal() && value instanceof CharSequence) {
            return value;
        }
        return JdbcLegacyTemporalAdapter.read(codecs, value, targetType);
    }
}
