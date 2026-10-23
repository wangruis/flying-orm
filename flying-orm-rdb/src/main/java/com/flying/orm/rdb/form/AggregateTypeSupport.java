package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;

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

    static void rejectEncryptedSelections(AggregateSpec spec) {
        // 只在来源声明了加密时预检，且必须早于密钥准备；普通聚合不遍历表达式做空检查。
        if (spec.joinQuery().isEmpty()
                ? spec.query().form().protections().encryptedFields().isEmpty()
                : spec.joinQuery().orElseThrow().sources().stream()
                        .allMatch(source -> source.form().protections().encryptedFields().isEmpty())) {
            return;
        }
        for (GroupSelection group : spec.groups()) {
            if (group.field() != null) {
                rejectEncryptedSelection(spec, spec.source(group.alias()), group.field(),
                        "encrypted field must not be used for aggregate grouping");
            } else {
                group.expression().forEachField(field -> rejectEncryptedSelection(spec,
                        field.source() == null ? spec.source(group.alias()) : field.source(), field.identity().name(),
                        "encrypted field must not be used for aggregate grouping"));
            }
        }
        for (AggregateExpression<?> aggregate : spec.aggregates()) {
            if (aggregate.operand() != null) {
                aggregate.operand().forEachField(field -> rejectEncryptedSelection(spec,
                        field.source() == null ? spec.source(aggregate.alias()) : field.source(),
                        field.identity().name(), "encrypted field must not be used as an aggregate source"));
            }
        }
    }

    private static void rejectEncryptedSelection(AggregateSpec spec, JoinSource source, String field, String message) {
        DynamicForm form = source == null ? spec.query().form() : source.form();
        if (form.protections().encrypted(field).isPresent()) {
            throw new IllegalArgumentException(message);
        }
    }

    static void requireAggregateContract(AggregateExpression<?> aggregate,
                                         AggregateInput input,
                                         FormAggregateReadSupport reads) {
        DynamicField field = input.valueField();
        switch (aggregate.function()) {
            case REGISTERED -> {
                if (aggregate.resultLogicalType() != field.databaseType().logicalType()) {
                    throw new IllegalArgumentException("registered aggregate result type must match its function");
                }
            }
            case COUNT_ROWS -> {
                // COUNT(*) consumes rows, not a field; no source type contract applies.
            }
            case COUNT -> {
                // COUNT(field) is defined for every scalar SQL type and preserves null semantics.
            }
            case COUNT_DISTINCT -> requireComparableInput(input);
            case SUM, AVG, STDDEV_POP, STDDEV_SAMPLE, VARIANCE_POP, VARIANCE_SAMPLE -> {
                if (!field.databaseType().isNumeric()) {
                    throw new IllegalArgumentException("numeric aggregate requires a numeric source field");
                }
            }
            case MIN, MAX -> {
                requireComparableInput(input);
                reads.renderer.requireStableOffsetTimeOrdering(field);
                if (aggregate.resultLogicalType() != field.databaseType().logicalType()) {
                    throw new IllegalArgumentException(
                            "MIN and MAX logical result type must match the source field");
                }
                requireCompatibleJavaType(field, aggregate.javaType(), reads);
            }
        }
    }

    static void requireComparableInput(AggregateInput input) {
        requireGroupable(input.valueField());
        // TEXT 仅描述逻辑族，不能证明函数把 CLOB 转成了可比较的 VARCHAR 标量。
        if (input.valueField().databaseType().logicalType() == LogicalType.TEXT
                && input.sources().stream().anyMatch(source ->
                    LargeObjectValueCodec.isLargeObjectDataType(source.field().databaseType()))) {
            throw new IllegalArgumentException("computed LOB text requires a comparable scalar result contract");
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
            case SMALL_INTEGER -> javaType == Byte.class
                    || javaType == Short.class || javaType == Integer.class;
            case INTEGER -> javaType == Byte.class || javaType == Short.class
                    || javaType == Integer.class
                    || field.databaseType().unsigned() && javaType == Long.class;
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

}
