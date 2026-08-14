package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;

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
import java.util.Locale;
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

    private static final ValueCodecRegistry VALUE_CODECS = ValueCodecRegistry.standard();

    private ArrayValueCodec() {
    }

    public static boolean isArrayDataType(String dataType) {
        return Objects.requireNonNull(dataType, "array data type must not be null").trim().endsWith("[]");
    }

    /**
     * 批量绑定 null 时执行器需要知道准确类型，所以这里返回 Long[]、String[] 这类数组 Class。
     */
    public static Class<?> parameterType(String dataType) {
        return Array.newInstance(elementJavaType(dataType), 0).getClass();
    }

    public static Object write(Object value, String dataType) {
        if (value == null) {
            return null;
        }
        List<Object> values = values(value);
        Class<?> elementType = elementJavaType(dataType);
        // 创建准确组件类型的数组，R2DBC 驱动才能知道 null 元素和数据库 array element type。
        Object array = Array.newInstance(elementType, values.size());
        for (int index = 0; index < values.size(); index++) {
            Object item = values.get(index);
            rejectNested(item);
            Array.set(array, index, convert(item, elementType));
        }
        return array;
    }

    /** `? = any(array_column)` 只绑定一个元素，也必须按数组元素类型转换。 */
    public static Object writeElement(Object value, String dataType) {
        if (value == null) {
            throw new IllegalArgumentException("array condition element must not be null");
        }
        rejectNested(value);
        return convert(value, elementJavaType(dataType));
    }

    /** 动态表单没有 Java 泛型信息，统一返回只读 List。 */
    public static List<Object> read(Object value) {
        if (value == null) {
            return null;
        }
        List<Object> values = values(value);
        values.forEach(ArrayValueCodec::rejectNested);
        return Collections.unmodifiableList(values);
    }

    /** 实体字段可以直接声明成数组或 List/Collection。 */
    public static Object read(Object value, Class<?> targetType) {
        Class<?> safeTarget = Objects.requireNonNull(targetType, "array target type must not be null");
        if (value == null) {
            return null;
        }
        if (safeTarget.isArray()) {
            List<Object> values = values(value);
            Class<?> componentType = safeTarget.getComponentType();
            Object target = Array.newInstance(componentType, values.size());
            for (int index = 0; index < values.size(); index++) {
                Object item = values.get(index);
                rejectNested(item);
                Array.set(target, index, convert(item, componentType));
            }
            return target;
        }
        if (safeTarget == List.class || safeTarget == Collection.class) {
            return read(value);
        }
        throw new IllegalArgumentException("array value cannot be converted to " + safeTarget.getName());
    }

    private static List<Object> values(Object value) {
        if (value instanceof Collection<?> collection) {
            return new ArrayList<>(collection);
        }
        Class<?> valueType = value.getClass();
        if (!valueType.isArray()) {
            throw new IllegalArgumentException("array value must be a Java array or Collection");
        }
        int length = Array.getLength(value);
        List<Object> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(Array.get(value, index));
        }
        return values;
    }

    private static void rejectNested(Object value) {
        if (value instanceof Collection<?> || value != null && value.getClass().isArray()) {
            throw new IllegalArgumentException("nested SQL arrays are not supported yet");
        }
    }

    private static Object convert(Object value, Class<?> targetType) {
        return value == null || targetType == Object.class ? value : VALUE_CODECS.read(value, targetType);
    }

    private static Class<?> elementJavaType(String dataType) {
        // 去掉 varchar(64)[] 之类的类型参数后再映射元素 Java 类型。
        String type = Objects.requireNonNull(dataType, "array data type must not be null").trim();
        if (!type.endsWith("[]")) {
            throw new IllegalArgumentException("SQL array type must end with []");
        }
        String elementType = type.substring(0, type.length() - 2).trim().toUpperCase(Locale.ROOT);
        int arguments = elementType.indexOf('(');
        if (arguments >= 0) {
            elementType = elementType.substring(0, arguments).trim();
        }
        return switch (elementType) {
            case "BIGINT", "INT8", "BIGSERIAL" -> Long.class;
            case "SMALLINT", "INT2" -> Short.class;
            case "INTEGER", "INT", "INT4", "SERIAL" -> Integer.class;
            case "DECIMAL", "NUMERIC" -> BigDecimal.class;
            case "REAL", "FLOAT4" -> Float.class;
            case "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT8" -> Double.class;
            case "BOOLEAN", "BOOL" -> Boolean.class;
            case "DATE" -> LocalDate.class;
            case "TIME", "TIME WITHOUT TIME ZONE" -> LocalTime.class;
            case "TIME WITH TIME ZONE", "TIMETZ" -> OffsetTime.class;
            case "TIMESTAMP", "DATETIME", "TIMESTAMP WITHOUT TIME ZONE" -> LocalDateTime.class;
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> OffsetDateTime.class;
            case "UUID" -> UUID.class;
            case "VARCHAR", "CHAR", "CHARACTER", "CHARACTER VARYING", "BPCHAR", "TEXT" -> String.class;
            default -> Object.class;
        };
    }
}
