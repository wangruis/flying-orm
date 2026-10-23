package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.rdb.mapping.EntityEnumStorage;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.math.BigInteger;

/** 解析自有枚举持久值声明及未注册类型的逻辑删除字面量。 */
final class EntityFieldTypeResolver {

    private EntityFieldTypeResolver() {
    }

    static EntityEnumStorage enumStorage(Class<?> type, EnumValueDefinition enumValue) {
        if (!type.isEnum()) {
            return EntityEnumStorage.NONE;
        }
        if (enumValue.memberName() != null) {
            // 自有 @EnumValue 由专用 codec 读写，不再套用 name 或 ordinal 两种默认策略。
            return EntityEnumStorage.NONE;
        }
        // 没有 @EnumValue 时统一使用枚举名字，避免 ordinal 因常量重排而改变已有数据含义。
        return EntityEnumStorage.NAME;
    }

    static EnumValueDefinition enumValue(Class<?> type) {
        if (!type.isEnum()) {
            return EnumValueDefinition.NONE;
        }
        Field selected = null;
        for (Field candidate : type.getDeclaredFields()) {
            if (!candidate.isAnnotationPresent(EnumValue.class)) {
                continue;
            }
            if (candidate.isEnumConstant() || Modifier.isStatic(candidate.getModifiers())) {
                throw new MappingException("@EnumValue must mark an instance value field: " + candidate);
            }
            if (selected != null) {
                throw new MappingException("an enum may declare only one @EnumValue field: " + type.getName());
            }
            selected = candidate;
        }
        return selected == null
                ? EnumValueDefinition.NONE
                : new EnumValueDefinition(selected.getName(), selected.getType());
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
            if ("true".equalsIgnoreCase(value)) {
                return true;
            }
            if ("false".equalsIgnoreCase(value)) {
                return false;
            }
            throw new IllegalArgumentException("boolean logic delete value must be true, false, 1 or 0");
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
        if (Double.class.equals(safeType)) {
            double parsed = Double.parseDouble(value);
            if (!Double.isFinite(parsed)) {
                throw new IllegalArgumentException("double logic delete value must be finite");
            }
            return parsed;
        }
        if (Float.class.equals(safeType)) {
            float parsed = Float.parseFloat(value);
            if (!Float.isFinite(parsed)) {
                throw new IllegalArgumentException("float logic delete value must be finite");
            }
            return parsed;
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
