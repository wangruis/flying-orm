package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.codec.OffsetTimeValueCodec;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.internal.plan.ConditionStructurePlan;
import com.flying.orm.rdb.internal.plan.SqlPlanSpec;
import com.flying.orm.rdb.internal.plan.SqlStatementCompiler;
import com.flying.orm.rdb.internal.plan.SqlStructurePlan;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.json.JsonDialect;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.vector.VectorValueCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
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
    final DialectCapabilities dialectCapabilities;

    private final SqlRenderer normalizedConditionRenderer;
    private final JsonDialect jsonDialect;
    private final UnaryOperator<String> identifierRenderer;
    private final Function<RelationIdentity, String> relationIdentifierRenderer;
    private final StructuralPlanCaches planCaches;
    private final FormConditionValueNormalizer conditionValues;
    final Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs;

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches) {
        this(conditionRenderer, jsonDialect, dialectName, nativeBoolean,
             identifierRenderer, planCaches, Map.of(), DialectCapabilities.empty());
    }

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches,
                         Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs) {
        this(conditionRenderer, jsonDialect, dialectName, nativeBoolean, identifierRenderer,
             planCaches, customFieldCodecs, DialectCapabilities.empty());
    }

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches,
                         Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs,
                         DialectCapabilities dialectCapabilities) {
        this(conditionRenderer, jsonDialect, dialectName, nativeBoolean, identifierRenderer,
             planCaches, customFieldCodecs, dialectCapabilities,
             identity -> FormRelationIdentifierSupport.render(identifierRenderer, identity));
    }

    FormSqlRenderSupport(SqlRenderer conditionRenderer,
                         JsonDialect jsonDialect,
                         String dialectName,
                         boolean nativeBoolean,
                         UnaryOperator<String> identifierRenderer,
                         StructuralPlanCaches planCaches,
                         Map<DynamicField, EntityTypeMappingRegistry.Mapping> customFieldCodecs,
                         DialectCapabilities dialectCapabilities,
                         Function<RelationIdentity, String> relationIdentifierRenderer) {
        this.conditionRenderer = Objects.requireNonNull(conditionRenderer, "sql renderer must not be null");
        this.valueCodecs = conditionRenderer.valueCodecs();
        this.normalizedConditionRenderer = conditionRenderer.withValueCodecs(
                FormEncodedConditionValue.registerWith(valueCodecs));
        this.jsonDialect = Objects.requireNonNull(jsonDialect, "json dialect must not be null");
        this.dialectName = Objects.requireNonNull(dialectName, "dialect name must not be null");
        this.nativeBoolean = nativeBoolean;
        this.dialectCapabilities = Objects.requireNonNull(
                dialectCapabilities, "dialect capabilities must not be null");
        this.identifierRenderer = Objects.requireNonNull(identifierRenderer,
                                                         "sql identifier renderer must not be null");
        this.relationIdentifierRenderer = Objects.requireNonNull(
                relationIdentifierRenderer, "relation identifier renderer must not be null");
        this.planCaches = Objects.requireNonNull(planCaches, "structural plan caches must not be null");
        this.customFieldCodecs = Objects.requireNonNull(
                customFieldCodecs, "custom entity field codecs must not be null");
        this.conditionValues = new FormConditionValueNormalizer(this);
    }

    FormSqlRenderSupport withPlanCaches(StructuralPlanCaches caches) {
        return new FormSqlRenderSupport(conditionRenderer, jsonDialect, dialectName, nativeBoolean,
                                        identifierRenderer,
                                        Objects.requireNonNull(caches, "structural plan caches must not be null"),
                                        customFieldCodecs,
                                        dialectCapabilities,
                                        relationIdentifierRenderer);
    }

    FormSqlRenderSupport withCustomFieldCodecs(
            Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings) {
        return new FormSqlRenderSupport(conditionRenderer, jsonDialect, dialectName, nativeBoolean,
                                        identifierRenderer, planCaches,
                                        Objects.requireNonNull(mappings,
                                                               "custom entity field codecs must not be null"),
                                        dialectCapabilities,
                                        relationIdentifierRenderer);
    }

    TermRegistry conditionTerms() {
        return conditionRenderer.terms();
    }

    SqlRenderer normalizedConditionRenderer() {
        return normalizedConditionRenderer;
    }

    ConditionSql condition(DynamicForm form, ConditionGroup where) {
        ConditionGroup normalized = normalizeCondition(form, where);
        if (normalizedConditionRenderer.hasCorrelatedTerms()
                && !normalized.executionView().cacheable(normalizedConditionRenderer.standardConditionTermMask())) {
            String qualifier = identifier(form);
            SqlFragment fragment = normalizedConditionRenderer.renderWhere(
                    normalized, name -> qualifier + "." + identifier(form.field(name).name()),
                    name -> qualifier);
            return new ConditionSql(fragment.sql(), fragment.parameters(), "relation-condition", false);
        }
        ConditionStructurePlan plan = planCaches.condition(
                dialectName, normalized, normalizedConditionRenderer);
        return new ConditionSql(plan.plan().sql(), plan.parameters(), plan.shape(), plan.cacheable());
    }

    /**
     * 聚合 HAVING 复用相同的字段感知值规范化，但把已验证别名渲染为对应分组或聚合表达式。
     * 表达式只来自聚合 planner，不进入普通条件结构缓存。
     */
    ConditionSql condition(DynamicForm form, ConditionGroup where,
                           UnaryOperator<String> fieldIdentifierRenderer) {
        return condition(form, where, fieldIdentifierRenderer, null, null);
    }

    ConditionSql condition(DynamicForm form, ConditionGroup where,
                           UnaryOperator<String> fieldIdentifierRenderer,
                           UnaryOperator<String> correlatedFieldRenderer,
                           UnaryOperator<String> outerQualifierRenderer) {
        ConditionGroup normalized = normalizeCondition(form, where);
        SqlRenderer renderer = normalizedConditionRenderer.withFieldIdentifierRenderer(Objects.requireNonNull(
                        fieldIdentifierRenderer, "condition field identifier renderer must not be null"));
        SqlFragment fragment = correlatedFieldRenderer == null
                ? renderer.renderWhere(normalized)
                : renderer.renderWhere(normalized, correlatedFieldRenderer, outerQualifierRenderer);
        return new ConditionSql(fragment.sql(), fragment.parameters(), "aggregate-having", false);
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

    SqlRequest request(String operation, DynamicForm form,
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
        return FormFieldValueSupport.writeFields(this, form, values, batchRowIndex);
    }

    Object writeValue(DynamicField field, Object value) {
        return FormFieldValueSupport.writeValue(this, field, value);
    }

    EntityTypeMappingRegistry.Mapping customFieldMapping(DynamicField field) {
        return customFieldCodecs.get(field);
    }

    Object writeConditionValue(DynamicField field, Object value) {
        DynamicField safeField = Objects.requireNonNull(field, "condition dynamic field must not be null");
        if (FormFieldValueSupport.isJson(safeField) && !"?".equals(valueExpression(safeField))) {
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
                || FormFieldValueSupport.isJson(field)
                || LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return true;
        }
        LogicalType logicalType = field.databaseType().logicalType();
        Class<?> targetType = FormFieldValueSupport.scalarParameterType(this, field);
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
        if (timelineOperator && FormFieldValueSupport.isTextBackedOffsetTime(this, field)) {
            throw new IllegalArgumentException(
                    "text-backed OFFSET_TIME range comparison is not supported by dialect "
                            + dialectName + ": " + field.name());
        }
    }

    void requireStableOffsetTimeOrdering(DynamicField field) {
        if (FormFieldValueSupport.isTextBackedOffsetTime(this, field)) {
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
        if (FormFieldValueSupport.isJson(field)) {
            return String.class;
        }
        Class<?> scalarType = FormFieldValueSupport.scalarParameterType(this, field);
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
        return FormFieldValueSupport.isJson(field) ? jsonDialect.valueExpression("?") : "?";
    }

    String identifier(String value) {
        return Objects.requireNonNull(
                identifierRenderer.apply(value), "rendered identifier must not be null");
    }

    String identifier(DynamicForm form) {
        return FormRelationIdentifierSupport.identifier(
                form, identifierRenderer, relationIdentifierRenderer);
    }

    String derivedRelationIdentifier(DynamicForm owner, String localTable) {
        return FormRelationIdentifierSupport.derivedIdentifier(
                owner, localTable, identifierRenderer, relationIdentifierRenderer);
    }

    String columns(List<DynamicField> fields) {
        return fields.stream().map(field -> identifier(field.name()))
                     .collect(java.util.stream.Collectors.joining(", "));
    }

    String identifierColumns(List<String> fields) {
        return fields.stream().map(this::identifier).collect(java.util.stream.Collectors.joining(", "));
    }

    record FieldValue(DynamicField field, Object value) {
    }

    record ConditionSql(String sql, List<Object> parameters, String shape, boolean cacheable) {

        static ConditionSql none() {
            return new ConditionSql("", List.of(), "none", true);
        }
    }

}
