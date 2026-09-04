package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.form.FormAggregateReadSupport;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.UUID;

/** 聚合字段与返回类型的内部契约校验；不生成 SQL，也不访问数据库。 */
final class AggregateTypeSupport {

    private AggregateTypeSupport() {
    }

    static void rejectEncryptedSelections(AggregateSpec spec, DynamicForm form) {
        for (GroupSelection group : spec.groups()) {
            requireUnencrypted(
                    form, form.field(group.field()),
                    "encrypted field must not be used for aggregate grouping");
        }
        for (AggregateExpression<?> aggregate : spec.aggregates()) {
            requireUnencrypted(
                    form, form.field(aggregate.sourceField()),
                    "encrypted field must not be used as an aggregate source");
        }
    }

    static void requireAggregateContract(AggregateExpression<?> aggregate,
                                         DynamicField field,
                                         FormAggregateReadSupport reads) {
        switch (aggregate.function()) {
            case COUNT -> {
                // COUNT(field) is defined for every scalar SQL type and preserves null semantics.
            }
            case COUNT_DISTINCT -> requireGroupable(field);
            case SUM, AVG -> {
                if (!field.databaseType().isNumeric()) {
                    throw new IllegalArgumentException("SUM and AVG require a numeric source field");
                }
            }
            case MIN, MAX -> {
                requireGroupable(field);
                reads.requireStableOffsetTimeOrdering(field);
                if (aggregate.resultLogicalType() != field.databaseType().logicalType()) {
                    throw new IllegalArgumentException(
                            "MIN and MAX logical result type must match the source field");
                }
                requireCompatibleJavaType(field, aggregate.javaType(), reads);
            }
        }
    }

    static void requireGroupable(DynamicField field) {
        LogicalType logicalType = field.databaseType().logicalType();
        boolean groupable = !field.databaseType().isArray()
                && !LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())
                && switch (logicalType) {
                    case SMALL_INTEGER, INTEGER, BIG_INTEGER, DECIMAL, FLOAT, BOOLEAN,
                            TEXT, DATE, TIME, OFFSET_TIME, TIMESTAMP, OFFSET_TIMESTAMP, UUID -> true;
                    case BINARY, JSON, XML, VECTOR, INTERVAL, OTHER -> false;
                };
        if (!groupable) {
            throw new IllegalArgumentException(
                    "aggregate grouping, DISTINCT, MIN and MAX require a comparable scalar field");
        }
    }

    private static void requireCompatibleJavaType(DynamicField field,
                                                  Class<?> javaType,
                                                  FormAggregateReadSupport reads) {
        Class<?> customType = reads.customJavaType(field);
        if (customType != null) {
            if (customType != javaType) {
                throw new IllegalArgumentException(
                        "MIN and MAX Java result type must match the source field codec");
            }
            return;
        }
        LogicalType logicalType = field.databaseType().logicalType();
        boolean compatible = switch (logicalType) {
            case SMALL_INTEGER, INTEGER -> javaType == Byte.class
                    || javaType == Short.class || javaType == Integer.class;
            case BIG_INTEGER -> javaType == Long.class || javaType == BigInteger.class;
            case DECIMAL -> javaType == BigDecimal.class || javaType == BigInteger.class
                    || javaType == Double.class || javaType == Float.class;
            case FLOAT -> javaType == Double.class || javaType == Float.class || javaType == BigDecimal.class;
            case TEXT -> javaType == String.class;
            case DATE -> javaType == LocalDate.class;
            case TIME -> javaType == LocalTime.class;
            case OFFSET_TIME -> javaType == OffsetTime.class;
            case TIMESTAMP -> javaType == LocalDateTime.class;
            case OFFSET_TIMESTAMP -> javaType == Instant.class || javaType == OffsetDateTime.class;
            case UUID -> javaType == UUID.class;
            case BOOLEAN, BINARY, JSON, XML, VECTOR, INTERVAL, OTHER -> false;
        };
        if (!compatible) {
            throw new IllegalArgumentException(
                    "MIN and MAX Java result type does not match the source field codec");
        }
    }

    private static void requireUnencrypted(DynamicForm form,
                                           DynamicField field,
                                           String message) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            throw new IllegalArgumentException(message);
        }
    }
}
