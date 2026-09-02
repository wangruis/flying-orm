package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.JdbcLegacyTemporalAdapter;
import com.flying.orm.rdb.internal.mapping.EntityEnumValueCodec;
import com.flying.orm.rdb.json.JsonValueCodec;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;

/** 把驱动值转换为实体字段值；反射写入计划只负责选择目标成员。 */
final class EntityRowValueConverter {

    private EntityRowValueConverter() {
    }

    static Object convert(Object value,
                          Class<?> targetType,
                          DatabaseType databaseType,
                          EntityEnumStorage enumStorage,
                          EntityEnumValueCodec enumValue,
                          ValueCodecRegistry valueCodecs) {
        try {
            if (enumValue != null) {
                return enumValue.read(value, valueCodecs);
            }
            if (enumStorage == EntityEnumStorage.ORDINAL && targetType.isEnum() && value != null) {
                int ordinal = valueCodecs.read(value, Integer.class);
                Object[] constants = targetType.getEnumConstants();
                if (ordinal < 0 || ordinal >= constants.length) {
                    throw new IllegalArgumentException("enum ordinal is out of range: " + ordinal);
                }
                return constants[ordinal];
            }
            // byte[] 是二进制值，不是 SQL Array；只有真正的对象/基本类型数组走数组 codec。
            if ((targetType.isArray() && targetType != byte[].class)
                    || (Collection.class.isAssignableFrom(targetType)
                    && value != null
                    && value.getClass().isArray()
                    && value.getClass() != byte[].class)) {
                return ArrayValueCodec.read(value, targetType, valueCodecs);
            }
            if (JsonValueCodec.supportsTarget(targetType)) {
                return JsonValueCodec.read(value, targetType);
            }
            return readScalar(value, targetType, databaseType, valueCodecs);
        } catch (IllegalArgumentException error) {
            throw new MappingException("row value cannot be converted to " + targetType.getName(), error);
        }
    }

    private static Object readScalar(Object value,
                                     Class<?> targetType,
                                     DatabaseType databaseType,
                                     ValueCodecRegistry valueCodecs) {
        Object normalized = absoluteTimestampCarrier(value, targetType, databaseType);
        return JdbcLegacyTemporalAdapter.read(valueCodecs, normalized, targetType);
    }

    private static Object absoluteTimestampCarrier(Object value,
                                                   Class<?> targetType,
                                                   DatabaseType databaseType) {
        if (!(value instanceof LocalDateTime localDateTime)
                || databaseType.isArray()
                || databaseType.logicalType() != LogicalType.OFFSET_TIMESTAMP) {
            return value;
        }
        if (Instant.class.equals(targetType)) {
            // 使用不同于目标类型的 UTC 载体，确保应用 codec 仍按注册顺序参与转换或拒绝。
            return localDateTime.atOffset(ZoneOffset.UTC);
        }
        if (OffsetDateTime.class.equals(targetType)) {
            return localDateTime.toInstant(ZoneOffset.UTC);
        }
        return value;
    }

    static EntityEnumValueCodec enumValueCodec(Class<?> javaType, EntityFieldMetadata field) {
        return field.enumValueMember() == null
                ? null
                : EntityEnumValueCodec.create(javaType, field.enumValueMember());
    }
}
