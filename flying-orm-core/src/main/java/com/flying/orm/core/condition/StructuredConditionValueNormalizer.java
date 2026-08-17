package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 条件值的单次有界整理器。
 *
 * <p>集合最多快照一次，随后在同一份稳定数据上做大小检查和类型转换。这样 Iterable 不会被重复消费，
 * 也不会让集合、字符串或嵌套对象绕过条件策略。</p>
 */
final class StructuredConditionValueNormalizer {

    private final ValueCodecRegistry valueCodecs;

    StructuredConditionValueNormalizer(ValueCodecRegistry valueCodecs) {
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
    }

    Object normalize(Object value,
                     DynamicField field,
                     StructuredConditionPolicy policy,
                     String path,
                     String operator) {
        ConditionValueShape shape = policy.valueShape(operator);
        try {
            requireTextFieldForLike(operator, field, path);
            Object stableValue = snapshotIterable(value, shape, field, policy, path);
            validateValueLimits(stableValue, shape, field, policy, path, false);
            ConditionValueNormalizer.ScalarConverter converter = !policy.usesFieldValue(operator)
                    || shape == ConditionValueShape.NONE
                    ? (scalar, index) -> scalar
                    : (scalar, index) -> normalizeScalar(scalar,
                                                         field,
                                                         ConditionCompilationBudget.valuePath(path, index));
            return ConditionValueNormalizer.normalize(shape,
                                                      stableValue,
                                                      ConditionValuePolicy.REJECT_EMPTY,
                                                      converter,
                                                      policy.maxCollectionSize(),
                                                      policy.maxStringLength())
                                           .value();
        } catch (ConditionValueException error) {
            throw structuredValueError(error.error(), path, field.name());
        } catch (StructuredConditionException error) {
            throw error;
        } catch (RuntimeException failure) {
            rethrowVirtualMachineError(failure);
            throw valueTypeMismatch(field, path);
        }
    }

    private Object snapshotIterable(Object value,
                                    ConditionValueShape shape,
                                    DynamicField field,
                                    StructuredConditionPolicy policy,
                                    String path) {
        if (!(value instanceof Iterable<?> iterable)
                || shape != ConditionValueShape.COLLECTION
                && shape != ConditionValueShape.RANGE
                && shape != ConditionValueShape.SCALAR_OR_COLLECTION) {
            return value;
        }
        ArrayList<Object> snapshot = new ArrayList<>();
        for (Object item : iterable) {
            if (snapshot.size() >= policy.maxCollectionSize()) {
                throw collectionTooLarge(field, path);
            }
            snapshot.add(item);
        }
        // null 元素交给已有 normalizer 按策略清理或拒绝，快照阶段不抢先改变错误语义。
        return Collections.unmodifiableList(snapshot);
    }

    private void validateValueLimits(Object value,
                                     ConditionValueShape shape,
                                     DynamicField field,
                                     StructuredConditionPolicy policy,
                                     String path,
                                     boolean collectionElement) {
        if (value == null) {
            return;
        }
        if (value instanceof CharSequence text) {
            if (text.length() > policy.maxStringLength()) {
                throw StructuredConditionException.field(StructuredConditionErrorCode.VALUE_TOO_LONG,
                                                         path,
                                                         field.name(),
                                                         "structured condition string value exceeds limit at " + path);
            }
            return;
        }
        if (shape == ConditionValueShape.SCALAR && value.getClass().isArray()) {
            return;
        }
        if (value instanceof Iterable<?> iterable) {
            if (collectionElement) {
                throw valueShapeNotAllowed(field, path);
            }
            int index = 0;
            for (Object item : iterable) {
                if (index >= policy.maxCollectionSize()) {
                    throw collectionTooLarge(field, path);
                }
                // 只允许一层集合，因此自引用 List 也只会在这里被拒绝，不会递归进入自身。
                validateValueLimits(item,
                                    shape,
                                    field,
                                    policy,
                                    ConditionCompilationBudget.valuePath(path, index),
                                    true);
                index++;
            }
            return;
        }
        if (value.getClass().isArray()) {
            if (collectionElement) {
                throw valueShapeNotAllowed(field, path);
            }
            int length = Array.getLength(value);
            if (length > policy.maxCollectionSize()) {
                throw collectionTooLarge(field, path);
            }
            for (int index = 0; index < length; index++) {
                validateValueLimits(Array.get(value, index),
                                    shape,
                                    field,
                                    policy,
                                    ConditionCompilationBudget.valuePath(path, index),
                                    true);
            }
            return;
        }
        if (value instanceof Map<?, ?> || value instanceof StructuredConditionInput) {
            throw valueShapeNotAllowed(field, path);
        }
    }

    private StructuredConditionException structuredValueError(ConditionValueException.Error error,
                                                               String path,
                                                               String field) {
        StructuredConditionErrorCode code = switch (error) {
            case NULL_VALUE -> StructuredConditionErrorCode.VALUE_NULL;
            case BLANK_VALUE -> StructuredConditionErrorCode.VALUE_BLANK;
            case COLLECTION_EMPTY -> StructuredConditionErrorCode.VALUE_COLLECTION_EMPTY;
            case COLLECTION_TOO_LARGE -> StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE;
            case STRING_TOO_LONG -> StructuredConditionErrorCode.VALUE_TOO_LONG;
            case SHAPE_NOT_ALLOWED -> StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED;
            case RANGE_SIZE_INVALID -> StructuredConditionErrorCode.VALUE_RANGE_SIZE_INVALID;
            case RANGE_TYPE_MISMATCH -> StructuredConditionErrorCode.VALUE_RANGE_TYPE_MISMATCH;
            case RANGE_ORDER_INVALID -> StructuredConditionErrorCode.VALUE_RANGE_ORDER_INVALID;
        };
        return StructuredConditionException.field(code,
                                                  path,
                                                  field,
                                                  "structured condition value is invalid at " + path);
    }

    private Object normalizeScalar(Object value, DynamicField field, String path) {
        Class<?> targetType = targetType(field);
        try {
            return targetType == null ? valueCodecs.write(value) : valueCodecs.read(value, targetType);
        } catch (RuntimeException failure) {
            rethrowVirtualMachineError(failure);
            throw valueTypeMismatch(field, path);
        }
    }

    private static void requireTextFieldForLike(String operator, DynamicField field, String path) {
        String normalized = operator.trim().toLowerCase(Locale.ROOT);
        if ((normalized.equals("like")
                || normalized.equals("not-like")
                || normalized.equals("like-ignore-case")
                || normalized.equals("not-like-ignore-case"))
                && targetType(field) != String.class) {
            throw valueTypeMismatch(field, path);
        }
    }

    /** 扩展异常可包装或交叉引用底层失败；按对象身份遍历，fatal 必须保持原对象出站。 */
    static void rethrowVirtualMachineError(Throwable failure) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(failure);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                throw fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            Throwable[] suppressed = current.getSuppressed();
            for (int index = suppressed.length - 1; index >= 0; index--) {
                pending.addFirst(suppressed[index]);
            }
        }
    }

    private static StructuredConditionException valueTypeMismatch(DynamicField field, String path) {
        return StructuredConditionException.field(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH,
                                                  path,
                                                  field.name(),
                                                  "structured condition value cannot be converted for field ["
                                                          + field.name() + "] at " + path);
    }

    private static StructuredConditionException valueShapeNotAllowed(DynamicField field, String path) {
        return StructuredConditionException.field(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                                  path,
                                                  field.name(),
                                                  "structured condition value shape is not allowed at " + path);
    }

    private static StructuredConditionException collectionTooLarge(DynamicField field, String path) {
        return StructuredConditionException.field(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE,
                                                  path,
                                                  field.name(),
                                                  "structured condition collection value exceeds limit at " + path);
    }

    /** 根据跨方言逻辑类型选择稳定的 Java 类型，数据库专有类型仍交给 codec 的通用写转换。 */
    private static Class<?> targetType(DynamicField field) {
        LogicalDataType dataType = logicalDataType(field.dataType());
        return switch (dataType.name()) {
            case "BIGINT", "BIGSERIAL", "INT8" -> dataType.unsigned() ? BigInteger.class : Long.class;
            case "INT", "INTEGER", "INT4" -> dataType.unsigned() ? Long.class : Integer.class;
            case "INT2", "SMALLINT", "TINYINT", "MEDIUMINT" -> Integer.class;
            case "DEC", "DECIMAL", "NUMERIC", "NUMBER", "DOUBLE", "DOUBLE PRECISION", "FLOAT", "FLOAT4",
                    "FLOAT8", "REAL", "BINARY_FLOAT", "BINARY_DOUBLE", "MONEY", "SMALLMONEY" -> BigDecimal.class;
            case "BOOL", "BOOLEAN" -> Boolean.class;
            case "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ" -> OffsetDateTime.class;
            case "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "DATETIME",
                    "DATETIME2" -> LocalDateTime.class;
            case "TIME WITH TIME ZONE", "TIMETZ" -> OffsetTime.class;
            case "TIME", "TIME WITHOUT TIME ZONE" -> LocalTime.class;
            case "DATE" -> LocalDate.class;
            case "CHAR", "CHARACTER", "CHARACTER VARYING", "VARCHAR", "VARCHAR2", "NCHAR", "NVARCHAR",
                    "NVARCHAR2", "TEXT", "CLOB", "NCLOB", "JSON", "JSONB", "BPCHAR" -> String.class;
            default -> null;
        };
    }

    /** 去掉精度和长度修饰，同时保留 WITH TIME ZONE 等决定 Java 语义的类型词。 */
    private static LogicalDataType logicalDataType(String source) {
        String value = source.trim().toUpperCase(Locale.ROOT);
        StringBuilder normalized = new StringBuilder(value.length());
        int parentheses = 0;
        boolean pendingSpace = false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current == '(') {
                parentheses++;
                continue;
            }
            if (current == ')' && parentheses > 0) {
                parentheses--;
                continue;
            }
            if (parentheses > 0) {
                continue;
            }
            if (Character.isWhitespace(current)) {
                pendingSpace = normalized.length() > 0;
                continue;
            }
            if (pendingSpace) {
                normalized.append(' ');
                pendingSpace = false;
            }
            normalized.append(current);
        }
        return stripNumericModifiers(normalized.toString());
    }

    private static LogicalDataType stripNumericModifiers(String dataType) {
        String result = dataType;
        boolean unsigned = false;
        boolean stripped;
        do {
            stripped = false;
            if (result.endsWith(" ZEROFILL")) {
                result = result.substring(0, result.length() - " ZEROFILL".length()).stripTrailing();
                unsigned = true;
                stripped = true;
            } else if (result.endsWith(" UNSIGNED")) {
                result = result.substring(0, result.length() - " UNSIGNED".length()).stripTrailing();
                unsigned = true;
                stripped = true;
            }
        } while (stripped);
        return new LogicalDataType(result, unsigned);
    }

    private record LogicalDataType(String name, boolean unsigned) {
    }
}
