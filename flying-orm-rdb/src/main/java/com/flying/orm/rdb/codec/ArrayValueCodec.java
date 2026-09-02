package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * SQL 一维数组和 Java 数组/Collection 之间的字段感知转换器。
 * 写库时交给驱动真正的强类型数组，不生成 PostgreSQL array literal，也不拼接元素内容。
 *
 * <p>当前只支持一维数组；嵌套容器明确拒绝，避免不同驱动把多维数组解释成不同形状。元素转换复用 core codec，
 * 因此数字、UUID、时间和枚举规则与普通字段一致。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ArrayValueCodec {

    private static final ValueCodecRegistry STANDARD_CODECS = ValueCodecRegistry.standard();

    private ArrayValueCodec() {
    }

    public static boolean isArrayDataType(String dataType) {
        return isArrayDataType(DatabaseType.of(dataType));
    }

    public static boolean isArrayDataType(DatabaseType dataType) {
        return Objects.requireNonNull(dataType, "array data type must not be null").isArray();
    }

    /**
     * 批量绑定 null 时执行器需要知道准确类型，所以这里返回 Long[]、String[] 这类数组 Class。
     */
    public static Class<?> parameterType(String dataType) {
        return parameterType(DatabaseType.of(dataType));
    }

    public static Class<?> parameterType(DatabaseType dataType) {
        return Array.newInstance(elementJavaType(dataType), 0).getClass();
    }

    public static Object write(Object value, String dataType) {
        return write(value, DatabaseType.of(dataType), STANDARD_CODECS);
    }

    public static Object write(Object value, DatabaseType dataType) {
        return write(value, dataType, STANDARD_CODECS);
    }

    public static Object write(Object value, String dataType, ValueCodecRegistry valueCodecs) {
        return write(value, DatabaseType.of(dataType), valueCodecs);
    }

    public static Object write(Object value, DatabaseType dataType, ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        Class<?> elementType = elementJavaType(dataType);
        if (value instanceof Collection<?> collection) {
            int expectedSize = collection.size();
            Object array = Array.newInstance(elementType, expectedSize);
            int index = 0;
            for (Object item : collection) {
                rejectNested(item);
                Array.set(array, index++, writeElement(item, elementType, codecs));
            }
            return array;
        }
        Class<?> valueType = value.getClass();
        if (!valueType.isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        int length = Array.getLength(value);
        // 创建准确组件类型的数组，R2DBC 驱动才能知道 null 元素和数据库 array element type。
        Object array = Array.newInstance(elementType, length);
        for (int index = 0; index < length; index++) {
            Object item = Array.get(value, index);
            rejectNested(item);
            Array.set(array, index, writeElement(item, elementType, codecs));
        }
        return array;
    }

    /** `? = any(array_column)` 只绑定一个元素，也必须按数组元素类型转换。 */
    public static Object writeElement(Object value, String dataType) {
        return writeElement(value, DatabaseType.of(dataType), STANDARD_CODECS);
    }

    public static Object writeElement(Object value, DatabaseType dataType) {
        return writeElement(value, dataType, STANDARD_CODECS);
    }

    public static Object writeElement(Object value,
                                      DatabaseType dataType,
                                      ValueCodecRegistry valueCodecs) {
        if (value == null) {
            throw new IllegalArgumentException("array condition element must not be null");
        }
        rejectNested(value);
        return writeElement(value, elementJavaType(dataType),
                            Objects.requireNonNull(valueCodecs, "value codec registry must not be null"));
    }

    /** 动态表单没有 Java 泛型信息，统一返回只读 List。 */
    public static List<Object> read(Object value) {
        return readList(value, Object.class, STANDARD_CODECS);
    }

    /** 实体字段可以直接声明成数组或 List/Collection。 */
    public static Object read(Object value, Class<?> targetType) {
        return read(value, targetType, STANDARD_CODECS);
    }

    public static Object read(Object value, Class<?> targetType, ValueCodecRegistry valueCodecs) {
        Class<?> safeTarget = Objects.requireNonNull(targetType, "array target type must not be null");
        ValueCodecRegistry codecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        if (value == null) {
            return null;
        }
        if (safeTarget.isArray()) {
            Class<?> componentType = safeTarget.getComponentType();
            return readArray(value, componentType, codecs);
        }
        if (safeTarget == List.class || safeTarget == Collection.class) {
            return read(value);
        }
        throw new IllegalArgumentException("array value cannot be converted to " + safeTarget.getName());
    }

    /** Package-local conversion into the final read-only list. */
    static List<Object> readList(Object value,
                                 Class<?> elementType,
                                 ValueCodecRegistry valueCodecs) {
        if (value == null) {
            return null;
        }
        Class<?> safeElementType = Objects.requireNonNull(
                elementType, "array element type must not be null");
        ValueCodecRegistry codecs = Objects.requireNonNull(
                valueCodecs, "value codec registry must not be null");
        if (value instanceof Collection<?> collection) {
            List<Object> target = new ArrayList<>(collection.size());
            for (Object item : collection) {
                rejectNested(item);
                target.add(readElement(item, safeElementType, codecs));
            }
            return Collections.unmodifiableList(target);
        }
        Class<?> valueType = value.getClass();
        if (!valueType.isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        int length = Array.getLength(value);
        List<Object> target = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            Object item = Array.get(value, index);
            rejectNested(item);
            target.add(readElement(item, safeElementType, codecs));
        }
        return Collections.unmodifiableList(target);
    }

    private static Object readArray(Object value,
                                    Class<?> componentType,
                                    ValueCodecRegistry codecs) {
        if (value instanceof Collection<?> collection) {
            int length = collection.size();
            Object target = Array.newInstance(componentType, length);
            int index = 0;
            for (Object item : collection) {
                if (index >= length) {
                    throw new IllegalArgumentException("array Collection size changed during conversion");
                }
                rejectNested(item);
                Array.set(target, index++, readElement(item, componentType, codecs));
            }
            if (index != length) {
                throw new IllegalArgumentException("array Collection size changed during conversion");
            }
            return target;
        }
        Class<?> valueType = value.getClass();
        if (!valueType.isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        int length = Array.getLength(value);
        Object target = Array.newInstance(componentType, length);
        for (int index = 0; index < length; index++) {
            Object item = Array.get(value, index);
            rejectNested(item);
            Array.set(target, index, readElement(item, componentType, codecs));
        }
        return target;
    }

    private static void rejectNested(Object value) {
        if (value instanceof Collection<?> || value != null && value.getClass().isArray()) {
            throw new IllegalArgumentException("nested SQL arrays are not supported yet");
        }
    }

    private static Object writeElement(Object value, Class<?> targetType, ValueCodecRegistry valueCodecs) {
        if (value == null || targetType == Object.class) {
            return value;
        }
        Object encoded = valueCodecs.write(value);
        return targetType.isInstance(encoded) ? encoded : valueCodecs.read(encoded, targetType);
    }

    private static Object readElement(Object value, Class<?> targetType, ValueCodecRegistry valueCodecs) {
        return value == null || targetType == Object.class
                ? value : JdbcLegacyTemporalAdapter.read(valueCodecs, value, targetType);
    }

    private static Class<?> elementJavaType(DatabaseType dataType) {
        DatabaseType type = Objects.requireNonNull(dataType, "array data type must not be null");
        if (type.arrayDimensions() != 1) {
            throw new IllegalArgumentException("only one-dimensional SQL arrays are supported");
        }
        LogicalType logicalType = type.logicalType();
        return switch (logicalType) {
            case BIG_INTEGER -> Long.class;
            case SMALL_INTEGER -> "SMALLINT".equals(type.baseName()) || "INT2".equals(type.baseName())
                    ? Short.class : Integer.class;
            case INTEGER -> Integer.class;
            case DECIMAL -> BigDecimal.class;
            case FLOAT -> "REAL".equals(type.baseName()) || "FLOAT4".equals(type.baseName())
                    ? Float.class : Double.class;
            case BOOLEAN -> Boolean.class;
            case DATE -> LocalDate.class;
            case TIME -> LocalTime.class;
            case OFFSET_TIME -> OffsetTime.class;
            case TIMESTAMP -> LocalDateTime.class;
            case OFFSET_TIMESTAMP -> OffsetDateTime.class;
            case UUID -> UUID.class;
            case TEXT -> String.class;
            default -> Object.class;
        };
    }
}
