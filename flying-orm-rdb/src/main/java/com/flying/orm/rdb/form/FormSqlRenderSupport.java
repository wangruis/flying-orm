package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.internal.plan.ConditionStructurePlan;
import com.flying.orm.rdb.internal.plan.SqlPlanSpec;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import com.flying.orm.rdb.internal.plan.SqlStructurePlan;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.vector.VectorValueCodec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * 动态表单各类 SQL 渲染器共用的只读基础能力。
 *
 * <p>这里集中保留条件 AST 编译、SQL 计划缓存、字段识别、参数类型和 codec 转换规则，
 * 查询、单条写入与批量写入因此始终走同一套逻辑。实例只在 {@link FormDataSqlRenderer}
 * 创建时构造一次，内部没有请求级可变状态，可以被并发复用。</p>
 */
final class FormSqlRenderSupport {

    final SqlRenderer conditionRenderer;
    final ValueCodecRegistry valueCodecs;
    final String dialectName;
    final boolean nativeBoolean;

    private final SqlRenderer normalizedConditionRenderer;
    private final JsonDialect jsonDialect;
    private final UnaryOperator<String> identifierRenderer;
    private final StructuralPlanCaches planCaches;
    private final FormConditionValueNormalizer conditionValues;

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches) {
        this.conditionRenderer = Objects.requireNonNull(conditionRenderer, "sql renderer must not be null");
        this.valueCodecs = conditionRenderer.valueCodecs();
        this.normalizedConditionRenderer = conditionRenderer.withValueCodecs(
                FormEncodedConditionValue.registerWith(valueCodecs));
        this.jsonDialect = Objects.requireNonNull(jsonDialect, "json dialect must not be null");
        this.dialectName = Objects.requireNonNull(dialectName, "dialect name must not be null");
        this.nativeBoolean = nativeBoolean;
        this.identifierRenderer = Objects.requireNonNull(identifierRenderer,
                                                         "sql identifier renderer must not be null");
        this.planCaches = Objects.requireNonNull(planCaches, "structural plan caches must not be null");
        this.conditionValues = new FormConditionValueNormalizer(this);
    }

    FormSqlRenderSupport withPlanCaches(StructuralPlanCaches caches) {
        return new FormSqlRenderSupport(conditionRenderer,
                                        jsonDialect,
                                        dialectName,
                                        nativeBoolean,
                                        identifierRenderer,
                                        Objects.requireNonNull(caches, "structural plan caches must not be null"));
    }

    TermRegistry conditionTerms() {
        return conditionRenderer.terms();
    }

    SqlRenderer normalizedConditionRenderer() {
        return normalizedConditionRenderer;
    }

    ConditionSql condition(DynamicForm form, ConditionGroup where) {
        ConditionGroup normalized = normalizeCondition(form, where);
        ConditionStructurePlan plan = planCaches.condition(
                dialectName,
                normalized,
                normalizedConditionRenderer);
        return new ConditionSql(plan.plan().sql(), plan.parameters(), plan.shape(), plan.cacheable());
    }

    ConditionGroup normalizeCondition(DynamicForm form, ConditionGroup where) {
        return conditionValues.normalize(form, where);
    }

    ConditionSql requiredWhere(DynamicForm form, ConditionGroup where, String operation) {
        ConditionSql fragment = condition(form, where);
        if (fragment.sql().isBlank()) {
            throw new IllegalArgumentException(operation + " where condition must not be empty");
        }
        return fragment;
    }

    SqlRequest request(String operation,
                       DynamicForm form,
                       List<String> fields,
                       ConditionSql condition,
                       String groupShape,
                       String sortShape,
                       String pageShape,
                       List<Object> parameters,
                       Supplier<String> sqlCompiler) {
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql request parameters must not be null");
        Supplier<String> safeCompiler = Objects.requireNonNull(sqlCompiler, "sql compiler must not be null");
        if (!condition.cacheable()) {
            return new SqlRequest(
                    SqlStatementCompiler.compile(
                            safeCompiler.get(),
                            safeParameters.size(),
                            SqlBindMarkerStyle.CANONICAL,
                            dialectName),
                    safeParameters);
        }
        SqlPlanSpec spec = new SqlPlanSpec(dialectName,
                                           SqlBindMarkerStyle.CANONICAL,
                                           form.structureFingerprint(),
                                           form.table(),
                                           operation,
                                           fields,
                                           condition.shape(),
                                           groupShape,
                                           sortShape,
                                           pageShape);
        SqlStructurePlan plan = planCaches.sqlPlan(spec, () -> SqlStructurePlan.sequential(
                safeCompiler.get(), dialectName, SqlBindMarkerStyle.CANONICAL,
                operation, form.table(), safeParameters.size()));
        if (plan.parameterCount() != safeParameters.size()) {
            throw new IllegalStateException("cached SQL plan parameter count does not match current request");
        }
        return new SqlRequest(plan.statement(), safeParameters);
    }

    /** 为不适合结构缓存的 ORM SQL 创建已经校验和方言编译的计划。 */
    SqlRequest compiledRequest(String sql, List<Object> parameters) {
        List<Object> safeParameters = Objects.requireNonNull(
                parameters, "sql request parameters must not be null");
        return new SqlRequest(
                SqlStatementCompiler.compile(
                        sql,
                        safeParameters.size(),
                        SqlBindMarkerStyle.CANONICAL,
                        dialectName),
                safeParameters);
    }

    SqlStatementPlan compiledStatement(String sql,
                                       int parameterCount,
                                       SqlBindMarkerStyle bindMarkerStyle) {
        return SqlStatementCompiler.compile(
                sql, parameterCount, bindMarkerStyle, dialectName);
    }

    DynamicField field(DynamicForm form, String fieldName) {
        return form.findField(fieldName)
                   .orElseThrow(() -> new IllegalArgumentException(
                           "dynamic field does not exist"));
    }

    List<String> fieldNames(List<DynamicField> fields) {
        List<DynamicField> safeFields = Objects.requireNonNull(fields, "dynamic fields must not be null");
        List<String> names = new ArrayList<>(safeFields.size());
        for (DynamicField field : safeFields) {
            names.add(Objects.requireNonNull(field, "dynamic field must not be null").name());
        }
        return names;
    }

    List<FieldValue> writeFields(DynamicForm form, Map<String, Object> values, Long batchRowIndex) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("dynamic form values must not be empty");
        }
        List<FieldValue> fieldValues = new ArrayList<>(safeValues.size());
        Map<String, String> sourceNames = new HashMap<>(Math.max(16, safeValues.size() * 2));
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            DynamicField field = field(form, entry.getKey());
            if (entry.getValue() instanceof UpdateDelta) {
                if (batchRowIndex != null) {
                    throw new IllegalArgumentException("batch write row [" + batchRowIndex + "] field ["
                                                               + field.name() + "] does not allow update delta");
                }
                throw new IllegalArgumentException("update delta is only valid in an update SET clause: "
                                                           + field.name());
            }
            if ("sqlserver".equalsIgnoreCase(dialectName)
                    && field.generation().strategy() == ValueGeneration.Strategy.IDENTITY) {
                throw new IllegalArgumentException("SQL Server identity field must be omitted from write values: "
                                                           + field.name());
            }
            String previousName = sourceNames.putIfAbsent(field.normalizedName(), entry.getKey());
            if (previousName != null) {
                throw new IllegalArgumentException("duplicate normalized dynamic write field");
            }
            fieldValues.add(new FieldValue(field, writeValue(field, entry.getValue())));
        }
        return fieldValues;
    }

    Object writeValue(DynamicField field, Object value) {
        if (field.databaseType().isArray()) {
            return ArrayValueCodec.write(value, field.databaseType(), valueCodecs);
        }
        if (field.databaseType().logicalType() == LogicalType.VECTOR) {
            if (!"postgresql".equalsIgnoreCase(dialectName)) {
                throw new IllegalArgumentException("VECTOR fields are only supported by PostgreSQL");
            }
            return VectorValueCodec.write(value, field.length());
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())) {
            return OffsetTimeValueCodec.write(value, field.databaseType(), dialectName);
        }
        if (isJson(field)) {
            return JsonValueCodec.write(value);
        }
        if (LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return LargeObjectValueCodec.write(value, field.databaseType(), dialectName);
        }
        if (scalarParameterType(field) != Object.class) {
            return DialectScalarValueCodec.write(value,
                                                 field.databaseType(),
                                                 dialectName,
                                                 nativeBoolean,
                                                 valueCodecs);
        }
        Object encoded = valueCodecs.write(value);
        if (encoded instanceof UUID uuid && parameterType(field) == String.class) {
            // 应用 codec 优先；只有 codec 仍保留 UUID 时才执行跨方言 VARCHAR 默认回退。
            return uuid.toString();
        }
        return encoded;
    }

    Object writeConditionValue(DynamicField field, Object value) {
        DynamicField safeField = Objects.requireNonNull(field, "condition dynamic field must not be null");
        if (isJson(safeField) && !"?".equals(valueExpression(safeField))) {
            throw new IllegalArgumentException(
                    "JSON equality and ordering require a registered JSON condition term for this dialect");
        }
        if (safeField.databaseType().logicalType() == LogicalType.VECTOR) {
            throw new IllegalArgumentException("VECTOR comparison requires a registered vector condition term");
        }
        if (!requiresFieldAwareConditionEncoding(safeField)) {
            return value;
        }
        return new FormEncodedConditionValue(writeValue(safeField, value));
    }

    /** 普通标量留给通用渲染器编码一次；只有驱动形态依赖字段/方言时才提前转换并做不重复编码标记。 */
    boolean requiresFieldAwareConditionEncoding(DynamicField field) {
        if (field.databaseType().isArray()
                || OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())
                || isJson(field)
                || LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return true;
        }
        LogicalType logicalType = field.databaseType().logicalType();
        Class<?> targetType = scalarParameterType(field);
        return logicalType == LogicalType.BOOLEAN
                || logicalType == LogicalType.OFFSET_TIMESTAMP
                || logicalType == LogicalType.INTERVAL && targetType != Object.class
                || logicalType == LogicalType.TIME && targetType == String.class
                || logicalType == LogicalType.UUID && parameterType(field) == String.class;
    }

    /** 文本可无损保存偏移量，但 ISO 文本顺序不等于 OffsetTime 的 UTC 时间线顺序。 */
    void requireStableOffsetTimeComparison(DynamicField field, String operator) {
        boolean timelineOperator = switch (operator) {
            case ">", ">=", "<", "<=", "between", "not-between" -> true;
            default -> false;
        };
        if (timelineOperator && isTextBackedOffsetTime(field)) {
            throw new IllegalArgumentException(
                    "text-backed OFFSET_TIME range comparison is not supported by dialect "
                            + dialectName + ": " + field.name());
        }
    }

    void requireStableOffsetTimeOrdering(DynamicField field) {
        if (isTextBackedOffsetTime(field)) {
            throw new IllegalArgumentException(
                    "text-backed OFFSET_TIME ordering is not supported by dialect "
                            + dialectName + ": " + field.name());
        }
    }

    Class<?> parameterType(DynamicField field) {
        if (field.databaseType().isArray()) {
            return ArrayValueCodec.parameterType(field.databaseType());
        }
        if (field.databaseType().logicalType() == LogicalType.VECTOR) {
            return VectorValueCodec.parameterType();
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())) {
            return OffsetTimeValueCodec.parameterType(field.databaseType(), dialectName);
        }
        if (isJson(field)) {
            return String.class;
        }
        Class<?> scalarType = scalarParameterType(field);
        if (scalarType != Object.class) {
            return scalarType;
        }
        if (field.databaseType().isBinary()) {
            return byte[].class;
        }
        if (field.databaseType().isTextual()) {
            return String.class;
        }
        return Object.class;
    }

    String valueExpression(DynamicField field) {
        return isJson(field) ? jsonDialect.valueExpression("?") : "?";
    }

    String identifier(String value) {
        return Objects.requireNonNull(
                identifierRenderer.apply(value), "rendered identifier must not be null");
    }

    String columns(List<DynamicField> fields) {
        return fields.stream().map(field -> identifier(field.name()))
                     .collect(java.util.stream.Collectors.joining(", "));
    }

    String identifierColumns(List<String> fields) {
        return fields.stream().map(this::identifier).collect(java.util.stream.Collectors.joining(", "));
    }

    boolean needsScalarDecoding(DynamicField field) {
        return scalarParameterType(field) != Object.class;
    }

    FormScalarReadPlan scalarReadPlan(DynamicField field) {
        return FormScalarReadPlan.compile(field, dialectName, nativeBoolean, valueCodecs);
    }

    private Class<?> scalarParameterType(DynamicField field) {
        return DialectScalarValueCodec.parameterType(field.databaseType(), dialectName, nativeBoolean);
    }

    Object readScalarValue(DynamicField field, Object value) {
        FormScalarReadPlan plan = scalarReadPlan(field);
        return plan == null ? DialectScalarValueCodec.read(value, field.databaseType(), valueCodecs) : plan.read(value);
    }

    private static boolean isJson(DynamicField field) {
        return !field.databaseType().isArray() && field.databaseType().logicalType() == LogicalType.JSON;
    }

    private boolean isTextBackedOffsetTime(DynamicField field) {
        return OffsetTimeValueCodec.isOffsetTimeDataType(field.databaseType())
                && ("mysql".equalsIgnoreCase(dialectName)
                    || "oracle".equalsIgnoreCase(dialectName)
                    || "sqlserver".equalsIgnoreCase(dialectName));
    }

    record FieldValue(DynamicField field, Object value) {
    }

    record ConditionSql(String sql, List<Object> parameters, String shape, boolean cacheable) {

        static ConditionSql none() {
            return new ConditionSql("", List.of(), "none", true);
        }
    }

}
