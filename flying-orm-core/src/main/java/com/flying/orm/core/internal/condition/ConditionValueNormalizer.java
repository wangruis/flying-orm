package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;

import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * 集中整理条件值，让所有查询入口使用同一套空值、集合、区间和标量规则。
 *
 * <p>它只处理 Java 值，不接触 SQL、字段名或数据库。调用方可以传入 {@link ScalarConverter} 完成
 * 字段类型转换，但转换器只能返回标量，不能借机把集合或对象塞回条件 AST。类本身无状态、无锁，
 * 所有返回集合都会复制成不可变集合。</p>
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public final class ConditionValueNormalizer {
    /** 类型形状与具体值无关；随 Class 卸载，避免集合热路径重复执行接口类型检查。 */
    private static final ClassValue<ValueType> VALUE_TYPES = new ClassValue<>() {
        @Override
        protected ValueType computeValue(Class<?> type) {
            return new ValueType(CharSequence.class.isAssignableFrom(type),
                    Iterable.class.isAssignableFrom(type) || Map.class.isAssignableFrom(type),
                    type.isArray() && !type.getComponentType().isPrimitive());
        }
    };

    private ConditionValueNormalizer() {
    }

    /**
     * 整理值时顺手做字段类型转换。{@code index == -1} 表示单值，集合和区间从 0 开始。
     *
     * @param shape     term 声明的值形状
     * @param value     原始输入
     * @param policy    空值处理策略
     * @param converter 标量转换器
     * @param maxCollectionSize 调用方指定的集合元素上限
     * @param maxStringLength 调用方指定的文本长度上限
     * @return 可直接放入条件 AST 的结果；{@link Result#present()} 为 false 时应跳过该条件
     */
    public static Result normalize(ConditionValueShape shape,
                                   Object value,
                                   ConditionValuePolicy policy,
                                   ScalarConverter converter,
                                   int maxCollectionSize,
                                   int maxStringLength) {
        ConditionValueShape safeShape = Objects.requireNonNull(shape, "condition value shape must not be null");
        ConditionValuePolicy safePolicy = Objects.requireNonNull(policy, "condition value policy must not be null");
        ScalarConverter safeConverter = Objects.requireNonNull(converter, "condition scalar converter must not be null");
        if (maxCollectionSize < 1) {
            throw new IllegalArgumentException("condition max collection size must be positive");
        }
        if (maxStringLength < 1) {
            throw new IllegalArgumentException("condition max string length must be positive");
        }
        return switch (safeShape) {
            case NONE -> normalizeNone(value);
            case SCALAR -> normalizeScalarValue(value, safePolicy, safeConverter, maxStringLength);
            case COLLECTION -> normalizeCollection(value,
                                                   safePolicy,
                                                   safeConverter,
                                                   maxCollectionSize,
                                                   maxStringLength);
            case RANGE -> normalizeRange(value,
                                         safePolicy,
                                         safeConverter,
                                         maxCollectionSize,
                                         maxStringLength);
            case SCALAR_OR_COLLECTION ->
                    normalizeScalarOrCollection(value,
                                                safePolicy,
                                                safeConverter,
                                                maxCollectionSize,
                                                maxStringLength);
        };
    }

    private static Result normalizeNone(Object value) {
        if (value != null) {
            throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                        "condition does not accept a value");
        }
        return Result.of(null);
    }

    private static Result normalizeScalarValue(Object value,
                                               ConditionValuePolicy policy,
                                               ScalarConverter converter,
                                               int maxStringLength) {
        Object scalar = normalizeScalar(value, converter, -1, maxStringLength, true);
        if (scalar instanceof Empty empty) {
            return handleEmpty(policy, empty.error());
        }
        return Result.of(scalar);
    }

    private static Result normalizeCollection(Object value,
                                              ConditionValuePolicy policy,
                                              ScalarConverter converter,
                                              int maxCollectionSize,
                                              int maxStringLength) {
        if (value == null) {
            return handleEmpty(policy, ConditionValueException.Error.NULL_VALUE);
        }
        Iterable<?> source = collectionValues(value, maxCollectionSize);
        Stream.Builder<Object> cleaned = Stream.builder();
        Class<?> previousClass = null;
        ValueType previousType = null;
        int index = 0;
        for (Object item : source) {
            if (index >= maxCollectionSize) {
                throw error(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                            "condition collection exceeds limit: " + maxCollectionSize);
            }
            if (item != null && item.getClass() != previousClass) {
                previousClass = item.getClass();
                previousType = VALUE_TYPES.get(previousClass);
            }
            Object scalar = normalizeScalar(item, converter, index, maxStringLength, false, previousType);
            if (scalar instanceof Empty empty) {
                if (policy == ConditionValuePolicy.REJECT_EMPTY) {
                    return handleEmpty(policy, empty.error());
                }
                index++;
                continue;
            }
            cleaned.add(scalar);
            index++;
        }
        List<Object> values = cleaned.build().toList();
        if (values.isEmpty()) {
            return handleEmpty(policy, ConditionValueException.Error.COLLECTION_EMPTY);
        }
        return Result.of(values);
    }

    /** 区间保留两个非空端点及其输入顺序；比较规则由数据库决定，不使用 Java 排序预判。 */
    private static Result normalizeRange(Object value,
                                         ConditionValuePolicy policy,
                                         ScalarConverter converter,
                                         int maxCollectionSize,
                                         int maxStringLength) {
        if (value == null) {
            return handleEmpty(policy, ConditionValueException.Error.NULL_VALUE);
        }
        Iterator<?> iterator = collectionValues(value, maxCollectionSize).iterator();
        if (!iterator.hasNext()) {
            throw error(ConditionValueException.Error.RANGE_SIZE_INVALID,
                        "range condition needs exactly two values");
        }
        Object sourceStart = iterator.next();
        if (!iterator.hasNext()) {
            throw error(ConditionValueException.Error.RANGE_SIZE_INVALID,
                        "range condition needs exactly two values");
        }
        if (maxCollectionSize < 2) {
            throw error(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                        "condition collection exceeds limit: " + maxCollectionSize);
        }
        Object sourceEnd = iterator.next();
        if (iterator.hasNext()) {
            iterator.next();
            throw error(ConditionValueException.Error.RANGE_SIZE_INVALID,
                        "range condition needs exactly two values");
        }
        Object startValue = normalizeScalar(sourceStart, converter, 0, maxStringLength, false);
        Object endValue = normalizeScalar(sourceEnd, converter, 1, maxStringLength, false);
        if (startValue instanceof Empty empty) {
            return handleEmpty(policy, empty.error());
        }
        if (endValue instanceof Empty empty) {
            return handleEmpty(policy, empty.error());
        }
        return Result.of(List.of(startValue, endValue));
    }

    /** 只有明确声明该形状的 term 才能同时接收单值和集合。 */
    private static Result normalizeScalarOrCollection(Object value,
                                                       ConditionValuePolicy policy,
                                                       ScalarConverter converter,
                                                       int maxCollectionSize,
                                                       int maxStringLength) {
        if (value instanceof Iterable<?> || value != null && value.getClass().isArray()) {
            return normalizeCollection(value, policy, converter, maxCollectionSize, maxStringLength);
        }
        return normalizeScalarValue(value, policy, converter, maxStringLength);
    }

    private static Object normalizeScalar(Object value,
                                          ScalarConverter converter,
                                          int index,
                                          int maxStringLength,
                                          boolean allowsArray) {
        return normalizeScalar(value, converter, index, maxStringLength, allowsArray,
                               value == null ? null : VALUE_TYPES.get(value.getClass()));
    }

    private static Object normalizeScalar(Object value,
                                          ScalarConverter converter,
                                          int index,
                                          int maxStringLength,
                                          boolean allowsArray,
                                          ValueType inputType) {
        if (value == null) {
            return Empty.NULL;
        }
        if (inputType.text()) {
            String text = value.toString();
            if (text.isBlank()) {
                return Empty.BLANK;
            }
            requireStringLength(text, maxStringLength);
        }
        if (inputType.isNonScalarContainer(allowsArray)) {
            throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                        "scalar condition does not accept a collection or object value");
        }
        Object converted = converter.convert(value, index);
        if (converted == null) {
            return Empty.NULL;
        }
        ValueType convertedType = converted == value ? inputType : VALUE_TYPES.get(converted.getClass());
        if (convertedType.text()) {
            String text = converted.toString();
            if (text.isBlank()) {
                return Empty.BLANK;
            }
            requireStringLength(text, maxStringLength);
            return converted;
        }
        if (convertedType.isNonScalarContainer(allowsArray)) {
            throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                        "condition value converter must return a scalar value");
        }
        return converted;
    }

    private static void requireStringLength(String value, int maxStringLength) {
        if (value.length() > maxStringLength) {
            throw error(ConditionValueException.Error.STRING_TOO_LONG,
                        "condition string exceeds limit: " + maxStringLength);
        }
    }

    /** 标量 term 可以绑定任意 Java 数组；集合中的 primitive array 是二进制/驱动数组标量。 */
    private record ValueType(boolean text, boolean container, boolean referenceArray) {
        private boolean isNonScalarContainer(boolean allowsArray) {
            return container || !allowsArray && referenceArray;
        }
    }

    private static Iterable<?> collectionValues(Object value, int maxCollectionSize) {
        if (value instanceof Iterable<?> iterable) {
            return iterable;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (length > maxCollectionSize) {
                throw error(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                            "condition collection exceeds limit: " + maxCollectionSize);
            }
            return new ArrayValues(value, length);
        }
        throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                    "condition needs a collection or array value");
    }

    private static Result handleEmpty(ConditionValuePolicy policy, ConditionValueException.Error error) {
        if (policy == ConditionValuePolicy.IGNORE_EMPTY) {
            return Result.ignored();
        }
        throw error(error, "condition value must not be empty");
    }

    private static ConditionValueException error(ConditionValueException.Error error, String message) {
        return new ConditionValueException(error, message);
    }

    /**
     * 条件值整理结果。
     *
     * @param present false 表示命中了 IGNORE_EMPTY，调用方不应把条件加入 AST
     * @param value   整理后的标量或不可变集合；被忽略时为 null
     */
    public record Result(boolean present, Object value) {
        private static Result of(Object value) {
            return new Result(true, value);
        }
        private static Result ignored() {
            return new Result(false, null);
        }
    }

    @FunctionalInterface
    public interface ScalarConverter {
        /**
         * 把单个输入转成字段真正需要的类型。单值的 index 是 -1，集合和区间从 0 开始。
         * 转换结果仍必须是标量，集合和 Map 会被 normalizer 拒绝。
         *
         * @param value 保留原始文本和领域类型的标量
         * @param index 标量在集合中的位置，单值为 -1
         * @return 转换后的标量
         */
        Object convert(Object value, int index);
    }

    /** 空值使用私有标记；有效元素直接传递，不为集合中的每个值创建临时包装。 */
    private record Empty(ConditionValueException.Error error) {
        private static final Empty NULL = new Empty(ConditionValueException.Error.NULL_VALUE);
        private static final Empty BLANK = new Empty(ConditionValueException.Error.BLANK_VALUE);
    }

    private static final class ArrayValues extends AbstractList<Object> {
        private final Object values;
        private final int size;
        private ArrayValues(Object values, int size) {
            this.values = values;
            this.size = size;
        }
        @Override
        public Object get(int index) {
            Objects.checkIndex(index, size);
            return Array.get(values, index);
        }
        @Override
        public int size() {
            return size;
        }
    }
}
