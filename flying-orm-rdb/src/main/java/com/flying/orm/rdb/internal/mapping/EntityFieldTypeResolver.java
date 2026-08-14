package com.flying.orm.rdb.internal.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.util.Map;

/** 从 Java 字段类型和自有 @EnumValue 声明推断跨方言逻辑数据类型。 */
final class EntityFieldTypeResolver {

    private EntityFieldTypeResolver() {
    }

    static EntityEnumStorage enumStorage(Field field, EnumValueDefinition enumValue) {
        if (!field.getType().isEnum()) {
            return EntityEnumStorage.NONE;
        }
        if (enumValue.memberName() != null) {
            // 自有 @EnumValue 由专用 codec 读写，不再套用 name 或 ordinal 两种默认策略。
            return EntityEnumStorage.NONE;
        }
        // 没有 @EnumValue 时统一使用枚举名字，避免 ordinal 因常量重排而改变已有数据含义。
        return EntityEnumStorage.NAME;
    }

    static String dataType(Field field, EntityEnumStorage enumStorage, EnumValueDefinition enumValue) {
        Class<?> type = field.getType();
        if (enumStorage == EntityEnumStorage.ORDINAL) {
            return "INTEGER";
        }
        if (enumValue.valueType() != null) {
            return dataType(enumValue.valueType());
        }
        return dataType(type);
    }

    static EnumValueDefinition enumValue(Field field) {
        if (!field.getType().isEnum()) {
            return EnumValueDefinition.NONE;
        }
        Field selected = null;
        for (Field candidate : field.getType().getDeclaredFields()) {
            if (!candidate.isAnnotationPresent(EnumValue.class)) {
                continue;
            }
            if (candidate.isEnumConstant() || Modifier.isStatic(candidate.getModifiers())) {
                throw new MappingException("@EnumValue must mark an instance value field: " + candidate);
            }
            if (selected != null) {
                throw new MappingException("an enum may declare only one @EnumValue field: " + field.getType().getName());
            }
            selected = candidate;
        }
        return selected == null
                ? EnumValueDefinition.NONE
                : new EnumValueDefinition(selected.getName(), selected.getType());
    }

    private static String dataType(Class<?> type) {
        Class<?> safeType = wrap(type);
        if (Map.class.isAssignableFrom(safeType)
                || java.util.Collection.class.isAssignableFrom(safeType)
                || JsonNode.class.isAssignableFrom(safeType)) {
            return "JSON";
        }
        if (String.class.equals(safeType) || safeType.isEnum()) {
            return "VARCHAR";
        }
        if (Long.class.equals(safeType)) {
            return "BIGINT";
        }
        if (Integer.class.equals(safeType) || Short.class.equals(safeType) || Byte.class.equals(safeType)) {
            return "INTEGER";
        }
        if (Boolean.class.equals(safeType)) {
            return "BOOLEAN";
        }
        if (BigDecimal.class.equals(safeType)
                || BigInteger.class.equals(safeType)
                || Double.class.equals(safeType)
                || Float.class.equals(safeType)) {
            return "DECIMAL";
        }
        if (LocalDateTime.class.equals(safeType)) {
            return "TIMESTAMP";
        }
        if (LocalDate.class.equals(safeType)) {
            return "DATE";
        }
        if (LocalTime.class.equals(safeType)) {
            return "TIME";
        }
        if (OffsetTime.class.equals(safeType)) {
            return "OFFSET_TIME";
        }
        if (byte[].class.equals(type) || Byte[].class.equals(type)) {
            return "BINARY";
        }
        // 未知业务类型保守按文本交给 codec；RDB 层仍可使用动态表单显式覆盖。
        return "VARCHAR";
    }

    static Object typedValue(String value, Class<?> type) {
        Class<?> safeType = wrap(type);
        if (Boolean.class.equals(safeType)) {
            if ("1".equals(value)) {
                return true;
            }
            if ("0".equals(value)) {
                return false;
            }
            return Boolean.parseBoolean(value);
        }
        if (Long.class.equals(safeType)) {
            return Long.valueOf(value);
        }
        if (Integer.class.equals(safeType)) {
            return Integer.valueOf(value);
        }
        if (Short.class.equals(safeType)) {
            return Short.valueOf(value);
        }
        if (Byte.class.equals(safeType)) {
            return Byte.valueOf(value);
        }
        if (BigDecimal.class.equals(safeType)) {
            return new BigDecimal(value);
        }
        if (BigInteger.class.equals(safeType)) {
            return new BigInteger(value);
        }
        if (safeType.isEnum()) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Enum<?> enumValue = Enum.valueOf((Class<? extends Enum>) safeType, value);
            return enumValue;
        }
        return value;
    }

    private static Class<?> wrap(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        if (long.class.equals(type)) {
            return Long.class;
        }
        if (int.class.equals(type)) {
            return Integer.class;
        }
        if (short.class.equals(type)) {
            return Short.class;
        }
        if (byte.class.equals(type)) {
            return Byte.class;
        }
        if (boolean.class.equals(type)) {
            return Boolean.class;
        }
        if (double.class.equals(type)) {
            return Double.class;
        }
        if (float.class.equals(type)) {
            return Float.class;
        }
        return type;
    }

    /** 枚举自定义值的最小元数据，供 DDL、模型缓存和实体读写 codec 共享。 */
    record EnumValueDefinition(String memberName, Class<?> valueType) {

        private static final EnumValueDefinition NONE = new EnumValueDefinition(null, null);
    }
}
