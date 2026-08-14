package com.flying.orm.rdb.vector;

import com.flying.orm.rdb.internal.ReflectionFailureSupport;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * 把动态表单传入的数组或集合整理成 PostgreSQL R2DBC 驱动原生支持的 {@code float[]}。
 *
 * <p>这里不生成 vector literal，也不把向量值拼到 SQL。所有元素必须是有限数字，维度不符会在拿连接前失败。
 * 回读时兼容驱动直接返回的 {@code float[]} 和它自己的 Vector 包装对象，但主项目不因此依赖具体驱动类。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class VectorValueCodec {

    /** pgvector 的 vector 类型最多允许 16000 维。 */
    public static final int MAX_DIMENSIONS = 16_000;

    private static final String DRIVER_VECTOR = "io.r2dbc.postgresql.codec.Vector";

    private VectorValueCodec() {
    }

    public static boolean isVectorDataType(String dataType) {
        String type = Objects.requireNonNull(dataType, "vector data type must not be null")
                             .trim()
                             .toUpperCase(Locale.ROOT);
        return "VECTOR".equals(type) || type.startsWith("VECTOR(");
    }

    /** null 参数也要给执行器准确的 Java 类型，才能调用 bindNull(float[].class)。 */
    public static Class<?> parameterType() {
        return float[].class;
    }

    public static float[] write(Object value, Integer expectedDimensions) {
        if (value == null) {
            return null;
        }
        List<?> values = values(value);
        int dimensions = values.size();
        if (dimensions < 1 || dimensions > MAX_DIMENSIONS) {
            throw new IllegalArgumentException("vector dimensions must be between 1 and " + MAX_DIMENSIONS);
        }
        if (expectedDimensions != null && dimensions != expectedDimensions) {
            throw new IllegalArgumentException("vector dimensions must be " + expectedDimensions + " but were "
                                                       + dimensions);
        }
        float[] vector = new float[dimensions];
        for (int index = 0; index < dimensions; index++) {
            Object item = values.get(index);
            if (!(item instanceof Number number)) {
                throw new IllegalArgumentException("vector element at index " + index + " must be a number");
            }
            float element = number.floatValue();
            if (!Float.isFinite(element)) {
                throw new IllegalArgumentException("vector element at index " + index + " must be finite");
            }
            vector[index] = element;
        }
        return vector;
    }

    /**
     * 动态表单统一返回新的 float[]，不把驱动对象或驱动内部数组泄漏给业务代码。
     */
    public static float[] read(Object value, Integer expectedDimensions) {
        if (value == null) {
            return null;
        }
        Object unwrapped = unwrapDriverVector(value);
        return write(unwrapped, expectedDimensions);
    }

    private static List<?> values(Object value) {
        if (value instanceof Collection<?> collection) {
            int declaredSize = collection.size();
            requireSupportedDimensions(declaredSize);
            List<Object> values = new ArrayList<>(declaredSize);
            for (Object item : collection) {
                if (values.size() == MAX_DIMENSIONS) {
                    throw new IllegalArgumentException(
                            "vector dimensions must be between 1 and " + MAX_DIMENSIONS);
                }
                values.add(item);
            }
            return values;
        }
        Class<?> type = value.getClass();
        if (!type.isArray()) {
            throw new IllegalArgumentException("vector value must be a Java array or Collection");
        }
        int length = Array.getLength(value);
        requireSupportedDimensions(length);
        List<Object> values = new ArrayList<>(length);
        for (int index = 0; index < length; index++) {
            values.add(Array.get(value, index));
        }
        return values;
    }

    private static void requireSupportedDimensions(int dimensions) {
        if (dimensions < 1 || dimensions > MAX_DIMENSIONS) {
            throw new IllegalArgumentException("vector dimensions must be between 1 and " + MAX_DIMENSIONS);
        }
    }

    private static Object unwrapDriverVector(Object value) {
        if (!DRIVER_VECTOR.equals(value.getClass().getName())) {
            return value;
        }
        try {
            Method accessor = value.getClass().getMethod("getVector");
            return accessor.invoke(value);
        } catch (NoSuchMethodException | IllegalAccessException error) {
            throw new IllegalArgumentException("PostgreSQL vector driver value cannot be read", error);
        } catch (InvocationTargetException error) {
            ReflectionFailureSupport.rethrowVirtualMachineError(error);
            Throwable cause = error.getCause() == null ? error : error.getCause();
            throw new IllegalArgumentException("PostgreSQL vector driver value cannot be read", cause);
        }
    }
}
