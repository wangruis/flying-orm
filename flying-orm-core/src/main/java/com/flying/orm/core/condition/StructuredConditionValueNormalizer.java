package com.flying.orm.core.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Objects;

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
        if (value instanceof StructuredConditionInput) {
            throw StructuredConditionException.field(StructuredConditionErrorCode.VALUE_SHAPE_NOT_ALLOWED,
                                                       path,
                                                       field.name(),
                                                       "structured condition value is invalid at " + path);
        }
        ConditionValueShape shape = policy.valueShape(operator);
        try {
            requireTextFieldForLike(operator, field, path);
            ConditionValueNormalizer.ScalarConverter converter = !policy.usesFieldValue(operator)
                    || shape == ConditionValueShape.NONE
                    ? (scalar, index) -> scalar
                    : (scalar, index) -> normalizeScalar(scalar,
                                                         field,
                                                         ConditionCompilationBudget.valuePath(path, index));
            return ConditionValueNormalizer.normalize(shape,
                                                      value,
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
            throw valueTypeMismatch(field, path);
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
            throw valueConversionFailed(field, path, failure);
        }
    }

    private static void requireTextFieldForLike(String operator, DynamicField field, String path) {
        if ((operator.equals("like")
                || operator.equals("not-like")
                || operator.equals("like-ignore-case")
                || operator.equals("not-like-ignore-case"))
                && targetType(field) != String.class) {
            throw valueTypeMismatch(field, path);
        }
    }

    private static StructuredConditionException valueTypeMismatch(DynamicField field, String path) {
        return StructuredConditionException.field(StructuredConditionErrorCode.VALUE_TYPE_MISMATCH,
                                                  path,
                                                  field.name(),
                                                  "structured condition value cannot be converted for field ["
                                                          + field.name() + "] at " + path);
    }

    private static StructuredConditionException valueConversionFailed(DynamicField field,
                                                                       String path,
                                                                       RuntimeException cause) {
        return StructuredConditionException.cause(StructuredConditionErrorCode.VALUE_CONVERSION_FAILED,
                                                  path,
                                                  field.name(),
                                                  null,
                                                  "structured condition value conversion failed for field ["
                                                          + field.name() + "] at " + path,
                                                  cause);
    }

    /** 根据跨方言逻辑类型选择稳定的 Java 类型，数据库专有类型仍交给 codec 的通用写转换。 */
    private static Class<?> targetType(DynamicField field) {
        DatabaseType dataType = field.databaseType();
        if (dataType.isArray()) {
            return null;
        }
        LogicalType logicalType = dataType.logicalType();
        return switch (logicalType) {
            case BIG_INTEGER -> dataType.unsigned() ? BigInteger.class : Long.class;
            case INTEGER -> dataType.unsigned() ? Long.class : Integer.class;
            case SMALL_INTEGER -> Integer.class;
            case DECIMAL, FLOAT -> BigDecimal.class;
            case BOOLEAN -> Boolean.class;
            case OFFSET_TIMESTAMP -> OffsetDateTime.class;
            case TIMESTAMP -> LocalDateTime.class;
            case OFFSET_TIME -> OffsetTime.class;
            case TIME -> LocalTime.class;
            case DATE -> LocalDate.class;
            case UUID -> java.util.UUID.class;
            case TEXT, JSON -> String.class;
            default -> null;
        };
    }
}
