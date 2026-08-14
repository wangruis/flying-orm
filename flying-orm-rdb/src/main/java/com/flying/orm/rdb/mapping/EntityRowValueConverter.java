package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.internal.ReflectionFailureSupport;
import com.flying.orm.rdb.internal.mapping.EntityEnumValueCodec;
import com.flying.orm.rdb.json.JsonValueCodec;

import java.util.Collection;

/** 把驱动值转换为实体字段值；反射写入计划只负责选择目标成员。 */
final class EntityRowValueConverter {

    private EntityRowValueConverter() {
    }

    static Object convert(Object value,
                          Class<?> targetType,
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
                return ArrayValueCodec.read(value, targetType);
            }
            if (JsonValueCodec.supportsTarget(targetType)) {
                return JsonValueCodec.read(value, targetType);
            }
            return valueCodecs.read(value, targetType);
        } catch (IllegalArgumentException error) {
            ReflectionFailureSupport.rethrowVirtualMachineError(error);
            throw new MappingException("row value cannot be converted to " + targetType.getName(), error);
        }
    }

    static EntityEnumValueCodec enumValueCodec(Class<?> javaType, EntityFieldMetadata field) {
        return field.enumValueMember() == null
                ? null
                : EntityEnumValueCodec.create(javaType, field.enumValueMember());
    }
}
