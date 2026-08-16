package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.ReflectionFailureSupport;
import com.flying.orm.rdb.mapping.MappingException;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 预编译一个枚举的 @EnumValue 成员读写规则。
 *
 * <p>创建时一次性读取全部常量并检查 null、重复值；真正映射每行数据时只做一次 codec 转换和 Map 查找，
 * 不再扫描字段或枚举常量。实例发布后只读，可以被实体映射缓存并发复用。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
@InternalApi
public final class EntityEnumValueCodec {

    private final Class<?> enumType;
    private final Field member;
    private final Class<?> valueType;
    private final Map<Object, Enum<?>> constantsByValue;

    private EntityEnumValueCodec(Class<?> enumType,
                                 Field member,
                                 Map<Object, Enum<?>> constantsByValue) {
        this.enumType = enumType;
        this.member = member;
        this.valueType = member.getType();
        this.constantsByValue = Map.copyOf(constantsByValue);
    }

    /** 根据元数据中的成员名创建访问计划；空成员名表示该字段不使用自定义枚举值。 */
    public static EntityEnumValueCodec create(Class<?> enumType, String memberName) {
        Class<?> safeType = Objects.requireNonNull(enumType, "enum type must not be null");
        if (!safeType.isEnum()) {
            throw new MappingException("@EnumValue mapping needs an enum type: " + safeType.getName());
        }
        String safeMember = requireText(memberName, "enum value member");
        try {
            Field member = safeType.getDeclaredField(safeMember);
            if (!member.trySetAccessible()) {
                throw new MappingException("enum value member is not accessible: " + member);
            }
            Map<Object, Enum<?>> values = new LinkedHashMap<>();
            for (Object constant : safeType.getEnumConstants()) {
                Enum<?> enumValue = (Enum<?>) constant;
                Object databaseValue = member.get(enumValue);
                if (databaseValue == null) {
                    throw new MappingException("@EnumValue must not be null: "
                                                       + safeType.getName() + "." + enumValue.name());
                }
                Enum<?> previous = values.putIfAbsent(databaseValue, enumValue);
                if (previous != null) {
                    throw new MappingException("duplicate @EnumValue in " + safeType.getName() + ": "
                                                       + previous.name() + " and " + enumValue.name());
                }
            }
            return new EntityEnumValueCodec(safeType, member, values);
        } catch (NoSuchFieldException | IllegalAccessException error) {
            throw new MappingException("enum value member cannot be compiled: "
                                               + safeType.getName() + "." + safeMember, error);
        }
    }

    /** 把枚举常量转换成真正绑定到 SQL 的成员值。 */
    public Object write(Object value) {
        if (value == null) {
            return null;
        }
        if (!enumType.isInstance(value)) {
            throw new MappingException("enum value does not belong to " + enumType.getName());
        }
        try {
            return member.get(value);
        } catch (IllegalAccessException error) {
            throw new MappingException("enum value member cannot be read: " + member, error);
        }
    }

    /** 把驱动返回值精确转换为成员类型，再查回枚举常量。 */
    public Object read(Object value, ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        Object converted;
        try {
            converted = Objects.requireNonNull(valueCodecs, "value codec registry must not be null")
                               .read(value, valueType);
        } catch (IllegalArgumentException error) {
            ReflectionFailureSupport.rethrowVirtualMachineError(error);
            throw new MappingException("enum database value cannot be converted to " + valueType.getName(), error);
        }
        Enum<?> result = constantsByValue.get(converted);
        if (result == null) {
            throw new MappingException("unknown @EnumValue for " + enumType.getName());
        }
        return result;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new MappingException(name + " must not be blank");
        }
        return value.trim();
    }
}
