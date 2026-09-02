package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;

import java.time.OffsetTime;
import java.util.Locale;
import java.util.Objects;

/**
 * OFFSET_TIME 保留时间里的 UTC 偏移量。数据库没有原生类型时写成文本，不能偷偷丢掉偏移量。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class OffsetTimeValueCodec {

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    private OffsetTimeValueCodec() {
    }

    public static boolean isOffsetTimeDataType(String dataType) {
        return isOffsetTimeDataType(DatabaseType.of(dataType));
    }

    public static boolean isOffsetTimeDataType(DatabaseType dataType) {
        DatabaseType safeType = Objects.requireNonNull(dataType, "database type must not be null");
        return !safeType.isArray()
                && ("OFFSET_TIME".equals(safeType.baseName())
                    || "TIME WITH TIME ZONE".equals(safeType.baseName()));
    }

    public static Class<?> parameterType(String dataType, String dialectName) {
        return parameterType(DatabaseType.of(dataType), dialectName);
    }

    public static Class<?> parameterType(DatabaseType dataType, String dialectName) {
        requireOffsetTime(dataType);
        return supportsNativeType(dialectName) ? OffsetTime.class : String.class;
    }

    public static Object write(Object value, String dataType, String dialectName) {
        return write(value, DatabaseType.of(dataType), dialectName);
    }

    public static Object write(Object value, DatabaseType dataType, String dialectName) {
        requireOffsetTime(dataType);
        if (value == null) {
            return null;
        }
        OffsetTime offsetTime = VALUE_CODECS.read(value, OffsetTime.class);
        return supportsNativeType(dialectName) ? offsetTime : offsetTime.toString();
    }

    public static OffsetTime read(Object value) {
        return value == null ? null : VALUE_CODECS.read(value, OffsetTime.class);
    }

    private static boolean supportsNativeType(String dialectName) {
        String normalized = normalize(dialectName);
        return "h2".equals(normalized) || "postgresql".equals(normalized);
    }

    private static void requireOffsetTime(DatabaseType dataType) {
        if (!isOffsetTimeDataType(dataType)) {
            throw new IllegalArgumentException("data type is not an offset-time type");
        }
    }

    private static String normalize(String value) {
        return Objects.requireNonNull(value, "value must not be null").trim().toLowerCase(Locale.ROOT);
    }
}
