package com.flying.orm.rdb.json;

import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * JSON 字段的值转换器。ObjectMapper 完成初始化后只读复用，可以安全地被并发请求共享。
 *
 * <p>这里没有开启默认类型或多态反序列化，前端 JSON 只能还原成普通 Map、List 或 JsonNode，
 * 不会根据输入里的类名创建任意 Java 对象。</p>
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public final class JsonValueCodec {

    private static final String ORACLE_JSON_VALUE = "oracle.sql.json.OracleJsonValue";

    private static final ObjectMapper MAPPER = createMapper();

    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private static final TypeReference<List<Object>> LIST_TYPE = new TypeReference<>() {
    };

    /**
     * PostgreSQL R2DBC 会用自己的 Json 包装类返回 JSONB。主模块不应该强依赖某个具体驱动，
     * 所以按运行时类型查找一次公开的 asString()，后面的同类对象直接复用 MethodHandle。
     */
    private static final ClassValue<Optional<MethodHandle>> DRIVER_TEXT_ACCESSORS = new ClassValue<>() {
        @Override
        protected Optional<MethodHandle> computeValue(Class<?> type) {
            // 具体实现可能是驱动包里的非公开内部类。沿父类向上找公开契约，虚方法调用仍会落到真实实现。
            for (Class<?> candidate = type; candidate != null; candidate = candidate.getSuperclass()) {
                if (!Modifier.isPublic(candidate.getModifiers())) {
                    continue;
                }
                try {
                    MethodHandle accessor = MethodHandles.publicLookup()
                                                         .findVirtual(candidate,
                                                                      "asString",
                                                                      MethodType.methodType(String.class))
                                                         .asType(MethodType.methodType(String.class, Object.class));
                    return Optional.of(accessor);
                } catch (NoSuchMethodException | IllegalAccessException ignored) {
                    // 当前公开父类没有文本契约，继续向上找；最终找不到时按不支持类型处理。
                }
            }
            return Optional.empty();
        }
    };

    /** Oracle JSON is optional, so detect its public interface without adding a driver dependency. */
    private static final ClassValue<Boolean> ORACLE_JSON_VALUES = new ClassValue<>() {
        @Override
        protected Boolean computeValue(Class<?> type) {
            return implementsNamedType(type, ORACLE_JSON_VALUE);
        }
    };

    private JsonValueCodec() {
    }

    private static ObjectMapper createMapper() {
        return JsonMapper.builder().build();
    }

    /**
     * 把 JSON 字段值变成紧凑 JSON 文本。字符串也会先解析，坏 JSON 不会被写进数据库。
     */
    public static String write(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof CharSequence text) {
                return MAPPER.writeValueAsString(MAPPER.readTree(text.toString()));
            }
            if (value instanceof byte[] bytes) {
                return MAPPER.writeValueAsString(MAPPER.readTree(bytes));
            }
            return MAPPER.writeValueAsString(value);
        } catch (JacksonException error) {
            throw new IllegalArgumentException("json value cannot be serialized", error);
        }
    }

    /**
     * 条件里的单个 JSON 元素按普通值编码。和 write 不同，字符串在这里是字符串值，不是整段 JSON 文本。
     */
    static String writeLiteral(Object value) {
        Object safeValue = Objects.requireNonNull(value, "json literal must not be null");
        try {
            return MAPPER.writeValueAsString(safeValue);
        } catch (JacksonException error) {
            throw new IllegalArgumentException("json literal cannot be serialized", error);
        }
    }

    /**
     * 判断实体字段是不是可以直接接收结构化 JSON。
     */
    public static boolean supportsTarget(Class<?> targetType) {
        Class<?> safeType = Objects.requireNonNull(targetType, "json target type must not be null");
        return Map.class.isAssignableFrom(safeType)
                || Collection.class.isAssignableFrom(safeType)
                || JsonNode.class.isAssignableFrom(safeType);
    }

    /**
     * 动态表单用逻辑类型名描述 JSON，数据库反读时也可能直接给出 JSONB。
     */
    public static boolean isJsonDataType(String dataType) {
        DatabaseType type = DatabaseType.of(
                Objects.requireNonNull(dataType, "json data type must not be null"));
        return !type.isArray() && type.logicalType() == LogicalType.JSON;
    }

    /**
     * 把驱动返回的 JSON 文本还原成实体字段需要的 Map、List 或 JsonNode。
     */
    public static Object read(Object value, Class<?> targetType) {
        Class<?> safeType = Objects.requireNonNull(targetType, "json target type must not be null");
        if (value == null || (safeType.isInstance(value) && !isOracleJsonValue(value))) {
            return value;
        }
        String json = jsonText(value);
        try {
            if (JsonNode.class.isAssignableFrom(safeType)) {
                return MAPPER.readTree(json);
            }
            if (safeType == Map.class) {
                return MAPPER.readValue(json, MAP_TYPE);
            }
            if (safeType == List.class || safeType == Collection.class) {
                return MAPPER.readValue(json, LIST_TYPE);
            }
            return MAPPER.readValue(json, safeType);
        } catch (JacksonException error) {
            throw new IllegalArgumentException("json value cannot be converted to " + safeType.getName(), error);
        }
    }

    /**
     * 动态表单没有固定 Java 类型，按 JSON 自身结构还原成 Map、List 或标量值。
     */
    public static Object read(Object value) {
        if (value == null) {
            return null;
        }
        if (!isOracleJsonValue(value)
                && (value instanceof Map<?, ?> || value instanceof Collection<?> || value instanceof JsonNode)) {
            return value;
        }
        try {
            return MAPPER.readValue(jsonText(value), Object.class);
        } catch (JacksonException error) {
            throw new IllegalArgumentException("json value cannot be decoded", error);
        }
    }

    private static String jsonText(Object value) {
        if (value instanceof CharSequence text) {
            return text.toString();
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (isOracleJsonValue(value)) {
            return value.toString();
        }
        Optional<MethodHandle> accessor = DRIVER_TEXT_ACCESSORS.get(value.getClass());
        if (accessor.isPresent()) {
            try {
                return (String) accessor.get().invokeExact(value);
            } catch (RuntimeException | Error error) {
                // 内存耗尽、链接错误等 JVM 级故障不能伪装成普通 JSON 参数错误。
                throw error;
            } catch (Throwable error) {
                throw new IllegalArgumentException("json database value text accessor failed", error);
            }
        }
        throw new IllegalArgumentException("json database value must be text or bytes, but was "
                                                   + value.getClass().getName());
    }

    private static boolean isOracleJsonValue(Object value) {
        return ORACLE_JSON_VALUES.get(value.getClass());
    }

    private static boolean implementsNamedType(Class<?> type, String qualifiedName) {
        if (qualifiedName.equals(type.getName())) {
            return true;
        }
        for (Class<?> contract : type.getInterfaces()) {
            if (implementsNamedType(contract, qualifiedName)) {
                return true;
            }
        }
        Class<?> parent = type.getSuperclass();
        return parent != null && implementsNamedType(parent, qualifiedName);
    }

}
