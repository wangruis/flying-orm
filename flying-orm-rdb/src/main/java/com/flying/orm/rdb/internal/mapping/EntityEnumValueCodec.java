package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.JdbcLegacyTemporalAdapter;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.json.JsonValueCodec;
import tools.jackson.databind.JsonNode;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collection;
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
    private final boolean absoluteTimestamp;

    private EntityEnumValueCodec(Class<?> enumType,
                                 Field member,
                                 Map<Object, Enum<?>> constantsByValue,
                                 boolean absoluteTimestamp) {
        this.enumType = enumType;
        this.member = member;
        this.valueType = member.getType();
        this.constantsByValue = constantsByValue;
        this.absoluteTimestamp = absoluteTimestamp;
    }

    /** 根据元数据中的成员名创建访问计划；空成员名表示该字段不使用自定义枚举值。 */
    public static EntityEnumValueCodec create(Class<?> enumType, String memberName) {
        return create(enumType, memberName, DatabaseType.of("VARCHAR"));
    }

    /** 字段的存储语义决定绝对时间身份；显式文本映射仍保留原偏移量。 */
    public static EntityEnumValueCodec create(Class<?> enumType, String memberName, DatabaseType databaseType) {
        Class<?> safeType = Objects.requireNonNull(enumType, "enum type must not be null");
        if (!safeType.isEnum()) {
            throw new MappingException("@EnumValue mapping needs an enum type: " + safeType.getName());
        }
        String safeMember = requireText(memberName, "enum value member");
        boolean absoluteTimestamp = !databaseType.isArray()
                && databaseType.logicalType() == LogicalType.OFFSET_TIMESTAMP;
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
                Enum<?> previous = values.putIfAbsent(
                        databaseIdentity(databaseValue, true, absoluteTimestamp), enumValue);
                if (previous != null) {
                    throw new MappingException("duplicate @EnumValue in " + safeType.getName() + ": "
                                                       + previous.name() + " and " + enumValue.name());
                }
            }
            return new EntityEnumValueCodec(safeType, member, values, absoluteTimestamp);
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

    /** 供实体行转换复用成员的目标类型与字段数据库语义，无需再次反射解析。 */
    public Class<?> valueType() {
        return valueType;
    }

    /** 把驱动返回值精确转换为成员类型，再查回枚举常量。 */
    public Object read(Object value, ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        Object converted;
        try {
            converted = JsonValueCodec.supportsTarget(valueType)
                    ? valueCodecs.read(value, valueType, JsonValueCodec::read)
                    : JdbcLegacyTemporalAdapter.read(valueCodecs, value, valueType);
        } catch (IllegalArgumentException error) {
            throw new MappingException("enum database value cannot be converted to " + valueType.getName(), error);
        }
        Enum<?> result = constantsByValue.get(databaseIdentity(converted, false, absoluteTimestamp));
        if (result == null) {
            throw new MappingException("unknown @EnumValue for " + enumType.getName());
        }
        return result;
    }

    /** 按内容建立稳定键；编译期冻结可变值，查找期二进制仅借用当前行值，不遍历枚举。 */
    private static Object databaseIdentity(Object value, boolean snapshot, boolean absoluteTimestamp) {
        if (absoluteTimestamp && value instanceof OffsetDateTime timestamp) {
            return timestamp.toInstant();
        }
        if (value instanceof Map<?, ?> || value instanceof Collection<?> || value instanceof JsonNode) {
            return jsonIdentity(value);
        }
        if (value instanceof char[] characters) {
            return new String(characters);
        }
        if (value instanceof CharSequence characters) {
            return characters.toString();
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.stripTrailingZeros();
        }
        if (value instanceof byte[] bytes) {
            return ByteBuffer.wrap(snapshot ? bytes.clone() : bytes);
        }
        if (value instanceof Byte[] bytes) {
            return Arrays.asList(snapshot ? bytes.clone() : bytes);
        }
        return value;
    }

    /** JSON 不保留 Java 数字包装类型；只在声明结构化枚举值时构造内容键。 */
    private static Object jsonIdentity(Object value) {
        if (value instanceof JsonNode node) {
            if (node.isObject()) {
                Map<String, Object> identity = new LinkedHashMap<>(node.size());
                node.properties().forEach(entry -> identity.put(entry.getKey(), jsonIdentity(entry.getValue())));
                return identity;
            }
            if (node.isArray()) {
                return node.valueStream().map(EntityEnumValueCodec::jsonIdentity).toList();
            }
            if (node.isNumber()) {
                return node.decimalValue().stripTrailingZeros();
            }
            if (node.isTextual()) {
                return node.asString();
            }
            if (node.isBoolean()) {
                return node.booleanValue();
            }
            return node.isNull() ? null : node;
        }
        if (value instanceof Number number) {
            return new BigDecimal(number.toString()).stripTrailingZeros();
        }
        if (value instanceof Map<?, ?> object) {
            Map<Object, Object> identity = new LinkedHashMap<>(object.size());
            object.forEach((key, item) -> identity.put(key, jsonIdentity(item)));
            return identity;
        }
        if (value instanceof Collection<?> items) {
            return items.stream().map(EntityEnumValueCodec::jsonIdentity).toList();
        }
        return value;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new MappingException(name + " must not be blank");
        }
        return value.trim();
    }
}
