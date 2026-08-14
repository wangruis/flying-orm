package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private static final int DEFAULT_MAX_COLLECTION_SIZE = 1_000;

    private static final int DEFAULT_MAX_STRING_LENGTH = 4_096;

    private ConditionValueNormalizer() {
    }

    /**
     * 使用原值作为标量结果进行整理。
     *
     * @param shape  term 需要的值形状
     * @param value  调用方传入的原始值
     * @param policy 空值是忽略还是拒绝
     * @return 是否应保留条件以及整理后的值
     */
    public static Result normalize(ConditionValueShape shape, Object value, ConditionValuePolicy policy) {
        return normalize(shape, value, policy, (scalar, index) -> scalar);
    }

    /**
     * 整理值时顺手做字段类型转换。{@code index == -1} 表示单值，集合和区间从 0 开始。
     *
     * @param shape     term 声明的值形状
     * @param value     原始输入
     * @param policy    空值处理策略
     * @param converter 标量转换器
     * @return 可直接放入条件 AST 的结果；{@link Result#present()} 为 false 时应跳过该条件
     */
    public static Result normalize(ConditionValueShape shape,
                                   Object value,
                                   ConditionValuePolicy policy,
                                   ScalarConverter converter) {
        return normalize(shape,
                         value,
                         policy,
                         converter,
                         DEFAULT_MAX_COLLECTION_SIZE,
                         DEFAULT_MAX_STRING_LENGTH);
    }

    /**
     * 结构化条件可以给每次请求设置更小的集合上限。这个重载只供 core 内部安全编译器使用，
     * 公开是因为编译器位于另一个包；整个类型仍放在 internal 命名空间，不属于业务 API。
     */
    public static Result normalize(ConditionValueShape shape,
                                   Object value,
                                   ConditionValuePolicy policy,
                                   ScalarConverter converter,
                                   int maxCollectionSize) {
        return normalize(shape,
                         value,
                         policy,
                         converter,
                         maxCollectionSize,
                         DEFAULT_MAX_STRING_LENGTH);
    }

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
        if (maxCollectionSize > DEFAULT_MAX_COLLECTION_SIZE) {
            throw new IllegalArgumentException(
                    "condition max collection size must not exceed " + DEFAULT_MAX_COLLECTION_SIZE);
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
        Scalar scalar = normalizeScalar(value, converter, -1, maxStringLength, true);
        if (scalar.empty()) {
            return handleEmpty(policy, scalar.error());
        }
        return Result.of(scalar.value());
    }

    private static Result normalizeCollection(Object value,
                                              ConditionValuePolicy policy,
                                              ScalarConverter converter,
                                              int maxCollectionSize,
                                              int maxStringLength) {
        if (value == null) {
            return handleEmpty(policy, ConditionValueException.Error.NULL_VALUE);
        }
        List<Object> source = asList(value, maxCollectionSize);
        List<Object> cleaned = new ArrayList<>(source.size());
        for (int index = 0; index < source.size(); index++) {
            Scalar scalar = normalizeScalar(source.get(index), converter, index, maxStringLength, false);
            if (!scalar.empty()) {
                cleaned.add(scalar.value());
            }
        }
        if (cleaned.isEmpty()) {
            return handleEmpty(policy, ConditionValueException.Error.COLLECTION_EMPTY);
        }
        return Result.of(List.copyOf(cleaned));
    }

    /**
     * 区间固定要求两个同类型、可比较的非空值，并在这里检查起点不能大于终点。
     * 这样 between 一类 term 到渲染阶段时只需要负责占位符，不必再次解释值语义。
     */
    private static Result normalizeRange(Object value,
                                         ConditionValuePolicy policy,
                                         ScalarConverter converter,
                                         int maxCollectionSize,
                                         int maxStringLength) {
        if (value == null) {
            return handleEmpty(policy, ConditionValueException.Error.NULL_VALUE);
        }
        List<Object> source = asList(value, maxCollectionSize);
        if (source.size() != 2) {
            throw error(ConditionValueException.Error.RANGE_SIZE_INVALID,
                        "range condition needs exactly two values");
        }
        Scalar start = normalizeScalar(source.get(0), converter, 0, maxStringLength, false);
        Scalar end = normalizeScalar(source.get(1), converter, 1, maxStringLength, false);
        if (start.empty() || end.empty()) {
            ConditionValueException.Error emptyError = start.empty() ? start.error() : end.error();
            return handleEmpty(policy, emptyError);
        }
        Object startValue = start.value();
        Object endValue = end.value();
        if (!startValue.getClass().equals(endValue.getClass()) || !(startValue instanceof Comparable<?> comparable)) {
            throw error(ConditionValueException.Error.RANGE_TYPE_MISMATCH,
                        "range values must use the same comparable type");
        }
        @SuppressWarnings("unchecked")
        int compared = ((Comparable<Object>) comparable).compareTo(endValue);
        if (compared > 0) {
            throw error(ConditionValueException.Error.RANGE_ORDER_INVALID,
                        "range start must not be greater than range end");
        }
        return Result.of(List.of(startValue, endValue));
    }

    /**
     * 只有明确声明该形状的 term 才能同时接收单值和集合，未知 term 不会走到这里。
     */
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

    private static Scalar normalizeScalar(Object value,
                                          ScalarConverter converter,
                                          int index,
                                          int maxStringLength,
                                          boolean allowsArray) {
        if (value == null) {
            return Scalar.empty(ConditionValueException.Error.NULL_VALUE);
        }
        Object cleaned = value;
        if (value instanceof CharSequence text) {
            String stripped = text.toString().strip();
            if (stripped.isEmpty()) {
                return Scalar.empty(ConditionValueException.Error.BLANK_VALUE);
            }
            requireStringLength(stripped, maxStringLength);
            cleaned = stripped;
        }
        if (isNonScalarContainer(value, allowsArray)) {
            throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                        "scalar condition does not accept a collection or object value");
        }
        Object converted = converter.convert(cleaned, index);
        if (converted == null) {
            return Scalar.empty(ConditionValueException.Error.NULL_VALUE);
        }
        if (converted instanceof CharSequence text) {
            String stripped = text.toString().strip();
            if (stripped.isEmpty()) {
                return Scalar.empty(ConditionValueException.Error.BLANK_VALUE);
            }
            requireStringLength(stripped, maxStringLength);
            return Scalar.of(stripped);
        }
        if (isNonScalarContainer(converted, allowsArray)) {
            throw error(ConditionValueException.Error.SHAPE_NOT_ALLOWED,
                        "condition value converter must return a scalar value");
        }
        return Scalar.of(converted);
    }

    private static void requireStringLength(String value, int maxStringLength) {
        if (value.length() > maxStringLength) {
            throw error(ConditionValueException.Error.STRING_TOO_LONG,
                        "condition string exceeds limit: " + maxStringLength);
        }
    }

    /** 标量 term 可以绑定任意 Java 数组；集合和范围中的数组仍是禁止的嵌套容器。 */
    private static boolean isNonScalarContainer(Object value, boolean allowsArray) {
        return value instanceof Iterable<?>
                || value instanceof Map<?, ?>
                || !allowsArray && value.getClass().isArray();
    }

    private static List<Object> asList(Object value, int maxCollectionSize) {
        if (value instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            for (Object item : iterable) {
                if (values.size() >= maxCollectionSize) {
                    throw error(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                                "condition collection exceeds limit: " + maxCollectionSize);
                }
                values.add(item);
            }
            return values;
        }
        if (value.getClass().isArray()) {
            int length = Array.getLength(value);
            if (length > maxCollectionSize) {
                throw error(ConditionValueException.Error.COLLECTION_TOO_LARGE,
                            "condition collection exceeds limit: " + maxCollectionSize);
            }
            List<Object> values = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                values.add(Array.get(value, index));
            }
            return values;
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

        public static Result of(Object value) {
            return new Result(true, value);
        }

        public static Result ignored() {
            return new Result(false, null);
        }
    }

    @FunctionalInterface
    public interface ScalarConverter {

        /**
         * 把单个输入转成字段真正需要的类型。单值的 index 是 -1，集合和区间从 0 开始。
         * 转换结果仍必须是标量，集合和 Map 会被 normalizer 拒绝。
         *
         * @param value 已去掉字符串首尾空白的标量
         * @param index 标量在集合中的位置，单值为 -1
         * @return 转换后的标量
         */
        Object convert(Object value, int index);
    }

    private record Scalar(boolean empty, Object value, ConditionValueException.Error error) {

        private static Scalar of(Object value) {
            return new Scalar(false, value, null);
        }

        private static Scalar empty(ConditionValueException.Error error) {
            return new Scalar(true, null, error);
        }
    }
}
