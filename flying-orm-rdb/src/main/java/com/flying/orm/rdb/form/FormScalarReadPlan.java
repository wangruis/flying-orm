package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.codec.JdbcLegacyTemporalAdapter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/** 查询开始时预计算的标量读取策略；逐单元格只执行值形态保护和目标 codec。 */
record FormScalarReadPlan(Class<?> targetType,
                          boolean numeric,
                          boolean temporal,
                          boolean textBackedTemporal,
                          boolean mysqlUtc,
                          ValueCodecRegistry valueCodecs) {

    FormScalarReadPlan {
        Objects.requireNonNull(targetType, "scalar target type must not be null");
        Objects.requireNonNull(valueCodecs, "scalar value codec registry must not be null");
    }

    static FormScalarReadPlan compile(DynamicField field,
                                      String dialectName,
                                      boolean nativeBoolean,
                                      ValueCodecRegistry valueCodecs) {
        Class<?> boundType = DialectScalarValueCodec.parameterType(
                field.databaseType(), dialectName, nativeBoolean);
        if (boundType == Object.class) {
            return null;
        }
        Class<?> genericType = DialectScalarValueCodec.parameterType(field.databaseType(), "generic", true);
        Class<?> targetType = boundType == Boolean.class || genericType == Object.class ? boundType : genericType;
        boolean numeric = Number.class.isAssignableFrom(targetType);
        boolean temporal = targetType == LocalDate.class
                || targetType == LocalDateTime.class
                || targetType == LocalTime.class
                || targetType == OffsetDateTime.class;
        boolean textBackedTemporal = temporal && boundType == String.class;
        boolean mysqlUtc = "mysql".equalsIgnoreCase(dialectName)
                && targetType == OffsetDateTime.class;
        return new FormScalarReadPlan(targetType, numeric, temporal, textBackedTemporal, mysqlUtc, valueCodecs);
    }

    Object read(Object value) {
        if (value == null || targetType == Object.class) {
            return value;
        }
        // MySQL TIMESTAMP 按公开契约使用 UTC 会话并以 LocalDateTime 返回绝对时间。
        if (mysqlUtc && value instanceof LocalDateTime localDateTime) {
            return valueCodecs.read(localDateTime.toInstant(ZoneOffset.UTC), targetType);
        }
        if (numeric) {
            if (value instanceof Boolean) {
                throw new IllegalArgumentException(
                        "numeric driver value must not be Boolean; preserve the driver's numeric mapping");
            }
            if (!(value instanceof Number)) {
                return value;
            }
        }
        if (textBackedTemporal && value instanceof CharSequence) {
            return valueCodecs.read(value, targetType);
        }
        if (temporal && value instanceof CharSequence) {
            return value;
        }
        return JdbcLegacyTemporalAdapter.read(valueCodecs, value, targetType);
    }
}
