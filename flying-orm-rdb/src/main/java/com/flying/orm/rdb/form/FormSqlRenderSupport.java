package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.DialectScalarValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.internal.plan.ConditionStructurePlan;
import com.flying.orm.rdb.internal.plan.SqlPlanSpec;
import com.flying.orm.rdb.internal.plan.SqlStructurePlan;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.json.JsonValueCodec;
import com.flying.orm.rdb.vector.VectorValueCodec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormSqlRenderSupport {

    final SqlRenderer conditionRenderer;
    final ValueCodecRegistry valueCodecs;
    final String dialectName;
    final boolean nativeBoolean;

    private final JsonDialect jsonDialect;
    private final UnaryOperator<String> identifierRenderer;
    private final StructuralPlanCaches planCaches;

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches) {
        this.conditionRenderer = Objects.requireNonNull(conditionRenderer, "sql renderer must not be null");
        this.valueCodecs = conditionRenderer.valueCodecs();
        this.jsonDialect = Objects.requireNonNull(jsonDialect, "json dialect must not be null");
        this.dialectName = Objects.requireNonNull(dialectName, "dialect name must not be null");
        this.nativeBoolean = nativeBoolean;
        this.identifierRenderer = Objects.requireNonNull(identifierRenderer,
                                                         "sql identifier renderer must not be null");
        this.planCaches = Objects.requireNonNull(planCaches, "structural plan caches must not be null");
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

    ConditionSql condition(ConditionGroup where) {
        ConditionStructurePlan plan = planCaches.condition(
                dialectName,
                Objects.requireNonNull(where, "where condition must not be null"),
                conditionRenderer);
        return new ConditionSql(plan.plan().sql(), plan.parameters(), plan.shape(), plan.cacheable());
    }

    ConditionSql requiredWhere(ConditionGroup where, String operation) {
        ConditionSql fragment = condition(where);
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
                       List<String> projections,
                       Supplier<String> sqlCompiler) {
        List<Object> safeParameters = Objects.requireNonNull(parameters, "sql request parameters must not be null");
        Supplier<String> safeCompiler = Objects.requireNonNull(sqlCompiler, "sql compiler must not be null");
        if (!condition.cacheable()) {
            return new SqlRequest(safeCompiler.get(), safeParameters, SqlBindMarkerStyle.CANONICAL);
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
                safeCompiler.get(), operation, form.table(), projections, safeParameters.size()));
        if (plan.parameterCount() != safeParameters.size()) {
            throw new IllegalStateException("cached SQL plan parameter count does not match current request");
        }
        return new SqlRequest(plan.sql(), safeParameters, SqlBindMarkerStyle.CANONICAL);
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
        if (ArrayValueCodec.isArrayDataType(field.dataType())) {
            return ArrayValueCodec.write(value, field.dataType());
        }
        if (VectorValueCodec.isVectorDataType(field.dataType())) {
            if (!"postgresql".equalsIgnoreCase(dialectName)) {
                throw new IllegalArgumentException("VECTOR fields are only supported by PostgreSQL");
            }
            return VectorValueCodec.write(value, field.length());
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(field.dataType())) {
            return OffsetTimeValueCodec.write(value, field.dataType(), dialectName);
        }
        if (isJson(field)) {
            return JsonValueCodec.write(value);
        }
        if (LargeObjectValueCodec.isLargeObjectDataType(field.dataType())) {
            return LargeObjectValueCodec.write(value, field.dataType(), dialectName);
        }
        if (DialectScalarValueCodec.supports(field.dataType())) {
            return DialectScalarValueCodec.write(value,
                                                 field.dataType(),
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

    Class<?> parameterType(DynamicField field) {
        String dataType = field.dataType().toUpperCase(Locale.ROOT);
        if (ArrayValueCodec.isArrayDataType(dataType)) {
            return ArrayValueCodec.parameterType(dataType);
        }
        if (VectorValueCodec.isVectorDataType(dataType)) {
            return VectorValueCodec.parameterType();
        }
        if (OffsetTimeValueCodec.isOffsetTimeDataType(dataType)) {
            return OffsetTimeValueCodec.parameterType(dataType, dialectName);
        }
        if (isJson(field)) {
            return String.class;
        }
        if (DialectScalarValueCodec.supports(dataType)) {
            return DialectScalarValueCodec.parameterType(dataType, dialectName, nativeBoolean);
        }
        if (dataType.contains("BLOB") || dataType.contains("BINARY") || dataType.contains("BYTEA")) {
            return byte[].class;
        }
        if (dataType.contains("CHAR") || dataType.contains("TEXT") || dataType.contains("CLOB")) {
            return String.class;
        }
        return Object.class;
    }

    String valueExpression(DynamicField field) {
        return isJson(field) ? jsonDialect.valueExpression("?") : "?";
    }

    String identifier(String value) {
        return identifierRenderer.apply(SqlIdentifiers.requireIdentifier(value, "identifier"));
    }

    String columns(List<DynamicField> fields) {
        return fields.stream().map(field -> identifier(field.name()))
                     .collect(java.util.stream.Collectors.joining(", "));
    }

    String identifierColumns(List<String> fields) {
        return fields.stream().map(this::identifier).collect(java.util.stream.Collectors.joining(", "));
    }

    boolean needsScalarDecoding(DynamicField field) {
        return DialectScalarValueCodec.supports(field.dataType());
    }

    Object readScalarValue(DynamicField field, Object value) {
        return DialectScalarValueCodec.read(value, field.dataType(), valueCodecs);
    }

    private static boolean isJson(DynamicField field) {
        return JsonValueCodec.isJsonDataType(field.dataType());
    }

    record FieldValue(DynamicField field, Object value) {
    }

    record ConditionSql(String sql, List<Object> parameters, String shape, boolean cacheable) {

        static ConditionSql none() {
            return new ConditionSql("", List.of(), "none", true);
        }
    }
}
