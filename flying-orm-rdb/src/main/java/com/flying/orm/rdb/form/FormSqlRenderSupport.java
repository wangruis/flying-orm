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
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.expression.SqlExpressionCompiler;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.codec.LargeObjectValueCodec;
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
    final boolean sqlPlansEnabled;
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
        this.sqlPlansEnabled = planCaches.sqlPlansEnabled();
        this.customFieldCodecs = Objects.requireNonNull(
                customFieldCodecs, "custom entity field codecs must not be null");
        this.conditionValues = new FormConditionValueNormalizer(this);
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

    SqlExpressionCompiler.Operand expression(ValueExpression expression,
            Function<ValueExpression.Field, SqlExpressionCompiler.Operand> fields) {
        return SqlExpressionCompiler.compile(expression, fields,
                (value, nullType) -> expressionParameter(value.value(), nullType),
                conditionRenderer.expressionFunctions(), dialectName);
    }

    private SqlExpressionCompiler.Operand expressionParameter(Object source, LogicalType nullType) {
        if (source instanceof ExpressionFieldValue bound) {
            DynamicField field = bound.field();
            Object encoded = writeValue(field, bound.value());
            Object parameter = encoded == null ? new SqlNullParameter(parameterType(field)) : encoded;
            LogicalType type = field.databaseType().isArray() ? LogicalType.OTHER : field.databaseType().logicalType();
            return new SqlExpressionCompiler.Operand(new SqlFragment(valueExpression(field), List.of(parameter)), type);
        }
        Object value = valueCodecs.write(source);
        LogicalType type;
        if (value == null) type = null;
        else if (value instanceof Byte || value instanceof Short || value instanceof Integer) type = LogicalType.INTEGER;
        else if (value instanceof Long) type = LogicalType.BIG_INTEGER;
        else if (value instanceof Float || value instanceof Double) type = LogicalType.FLOAT;
        else if (value instanceof Number) type = LogicalType.DECIMAL;
        else if (value instanceof CharSequence || value instanceof Character) type = LogicalType.TEXT;
        else if (value instanceof Boolean) type = LogicalType.BOOLEAN;
        else type = LogicalType.OTHER;
        LogicalType bindingType = nullType == null ? LogicalType.DECIMAL : nullType;
        String sql = value == null && bindingType == LogicalType.DECIMAL ? "cast(? as decimal)" : "?";
        String declaration = switch (bindingType) {
            case SMALL_INTEGER -> "SMALLINT";
            case BIG_INTEGER -> "BIGINT";
            case OFFSET_TIMESTAMP -> "TIMESTAMP WITH TIME ZONE";
            default -> bindingType.name();
        };
        Object parameter = value == null
                ? new SqlNullParameter(parameterType(DynamicField.of("expression_parameter", declaration))) : value;
        return new SqlExpressionCompiler.Operand(new SqlFragment(sql, List.of(parameter)), type);
    }

    ConditionSql condition(DynamicForm form, ConditionGroup where) {
        ConditionGroup normalized = normalizeCondition(form, where);
        boolean standard = !form.hasFieldFeatures()
                && normalized.executionView().cacheable(normalizedConditionRenderer.standardConditionTermMask());
        SqlRenderer renderer = standard ? normalizedConditionRenderer
                : normalizedConditionRenderer.withFields(name -> form.findField(name).orElse(null));
        if (renderer.hasCorrelatedTerms() && !standard) {
            String qualifier = identifier(form);
            SqlFragment fragment = renderer.renderWhere(
                    normalized, name -> qualifier + "." + identifier(correlatedField(form, name)),
                    name -> qualifier);
            return new ConditionSql(fragment.sql(),
                    OwnedBindableValues.ownedValues(fragment.parameters()),
                    "relation-condition", false);
        }
        ConditionStructurePlan plan = planCaches.condition(
                dialectName, normalized, renderer);
        return new ConditionSql(plan.plan().sql(), plan.parameters(), plan.shape(), plan.cacheable());
    }

    /**
     * 聚合 HAVING 复用相同的字段感知值规范化，但把已验证别名渲染为对应分组或聚合表达式。
     * 表达式只来自聚合 planner，不进入普通条件结构缓存。
     */
    ConditionSql condition(DynamicForm form, ConditionGroup where,
                           UnaryOperator<String> fieldIdentifierRenderer,
                           UnaryOperator<String> correlatedFieldRenderer,
                           UnaryOperator<String> outerQualifierRenderer,
                           Function<String, DynamicField> valueFieldResolver) {
        ConditionGroup normalized = conditionValues.normalize(form, where, valueFieldResolver);
        SqlRenderer renderer = normalizedConditionRenderer.withFieldIdentifierRenderer(Objects.requireNonNull(
                        fieldIdentifierRenderer, "condition field identifier renderer must not be null"), form::field);
        SqlFragment fragment = correlatedFieldRenderer == null
                ? renderer.renderWhere(normalized)
                : renderer.renderWhere(normalized, name -> {
                    correlatedField(form, name);
                    return correlatedFieldRenderer.apply(name);
                }, outerQualifierRenderer);
        return new ConditionSql(fragment.sql(),
                OwnedBindableValues.ownedValues(fragment.parameters()),
                "aggregate-having", false);
    }

    ConditionGroup normalizeCondition(DynamicForm form, ConditionGroup where) {
        return conditionValues.normalize(form, where);
    }

    /** 关联键不经过值编码；不能把隐藏密文或检索摘要当作普通字段进行跨表比较。 */
    static String correlatedField(DynamicForm form, String name) {
        DynamicField field = form.field(name);
        if (form.protections().encrypted(field.name()).isPresent()
                || com.flying.orm.rdb.protection.ProtectedFormLayout.isCiphertextType(field.databaseType())
                || com.flying.orm.rdb.protection.ProtectedFormLayout.isHashType(field.databaseType())) {
            throw new IllegalArgumentException("encrypted field must not be used as a correlation key: " + name);
        }
        return field.name();
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
        return new SqlRequest(statement(operation, form, fields, condition, groupShape, sortShape,
                                        pageShape, safeParameters.size(), () -> SqlStatementCompiler.compile(
                                                sqlCompiler.get(), safeParameters.size(),
                                                SqlBindMarkerStyle.CANONICAL, dialectName)), safeParameters);
    }

    /** 单条与批量写入共享结构计划；这里只接收槽位数量，不持有请求参数。 */
    SqlStatementPlan statement(String operation, DynamicForm form,
                               List<String> fields, ConditionSql condition,
                               String groupShape, String sortShape, String pageShape,
                               int parameterCount, Supplier<SqlStatementPlan> sqlCompiler) {
        Supplier<SqlStatementPlan> safeCompiler = Objects.requireNonNull(sqlCompiler, "sql compiler must not be null");
        if (!sqlPlansEnabled || !condition.cacheable()) {
            return safeCompiler.get();
        }
        SqlPlanSpec spec = new SqlPlanSpec(dialectName,
                                           SqlBindMarkerStyle.CANONICAL,
                                           form.structureFingerprint(),
                                           form.relationIdentity(),
                                           operation,
                                           fields,
                                           condition.shape(),
                                           groupShape,
                                           sortShape,
                                           pageShape);
        SqlStructurePlan plan = planCaches.sqlPlan(spec, () -> new SqlStructurePlan(
                safeCompiler.get(), operation, form.relationIdentity()));
        if (plan.parameterCount() != parameterCount) {
            throw new IllegalStateException("cached SQL plan parameter count does not match current request");
        }
        return plan.statement();
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

    /** 校验每个字段；名称未变化时复用不可变列表，不保留调用方的可变列表。 */
    static List<String> canonicalFieldNames(DynamicForm form, List<String> fields) {
        Objects.requireNonNull(form, "dynamic form must not be null");
        if (fields.size() == form.fields().size() && fields == form.fieldNames()) return fields;
        List<String> normalized = null;
        int index = 0;
        for (String name : fields) {
            String canonical = form.field(name).name();
            if (normalized == null && !canonical.equals(name)) {
                normalized = new ArrayList<>(fields);
            }
            if (normalized != null) {
                normalized.set(index, canonical);
            }
            index++;
        }
        return List.copyOf(normalized == null ? fields : normalized);
    }

    List<String> fieldNames(List<DynamicField> fields) {
        List<DynamicField> safeFields = Objects.requireNonNull(fields, "dynamic fields must not be null");
        List<String> names = new ArrayList<>(safeFields.size());
        for (DynamicField field : safeFields) {
            names.add(Objects.requireNonNull(field, "dynamic field must not be null").name());
        }
        return names;
    }

    WriteFields writeFields(DynamicForm form, Map<String, Object> values, Long batchRowIndex) {
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

    /** 普通标量留给通用渲染器；显式字段 codec 与方言转换提前编码一次，避免被运行时值类型覆盖。 */
    boolean requiresFieldAwareConditionEncoding(DynamicField field) {
        if (!customFieldCodecs.isEmpty() && customFieldCodecs.containsKey(field)) {
            return true;
        }
        if (field.databaseType().isArray()
                || FormFieldValueSupport.isJson(field)
                || LargeObjectValueCodec.isLargeObjectDataType(field.databaseType())) {
            return true;
        }
        LogicalType logicalType = field.databaseType().logicalType();
        Class<?> targetType = FormFieldValueSupport.scalarParameterType(this, field);
        return logicalType == LogicalType.BOOLEAN
                || logicalType == LogicalType.OFFSET_TIME
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

    /** 已编码参数的类型；空值携带的声明不能被内部包装类型替代。 */
    static Class<?> boundParameterType(Object value) {
        return value instanceof SqlNullParameter nil ? nil.javaType() : value == null ? Object.class : value.getClass();
    }

    String valueExpression(DynamicField field) {
        return FormFieldValueSupport.isJson(field) ? jsonDialect.valueExpression("?") : "?";
    }

    String identifier(String value) {
        return Objects.requireNonNull(
                identifierRenderer.apply(value), "rendered identifier must not be null");
    }

    String identifier(DynamicForm form) {
        return identifier(form.relationIdentity());
    }

    String derivedRelationIdentifier(DynamicForm owner, String localTable) {
        return identifier(owner.relationIdentity().withTable(localTable));
    }

    private String identifier(RelationIdentity relation) {
        return Objects.requireNonNull(
                relationIdentifierRenderer.apply(relation), "rendered relation identifier must not be null");
    }

    String identifierColumns(List<String> fields) {
        return fields.stream().map(this::identifier).collect(java.util.stream.Collectors.joining(", "));
    }

    /** 字段布局只读使用，编码值由本次请求独占；发布时交给请求或批量计划，不进入结构缓存。 */
    record WriteFields(List<DynamicField> fields, Object[] parameters, List<String> expressions, int[] offsets) {
        int parameterIndex(int column) { return offsets == null ? column : offsets[column]; }

        String expression(int column, FormSqlRenderSupport support) {
            return expressions.isEmpty() ? support.valueExpression(fields.get(column)) : expressions.get(column);
        }

        List<String> expressions(FormSqlRenderSupport support) {
            return expressions.isEmpty() ? fields.stream().map(support::valueExpression).toList() : expressions;
        }

        List<Class<?>> parameterTypes(FormSqlRenderSupport support) {
            List<Class<?>> types = new ArrayList<>(parameters.length);
            for (int column = 0; column < fields.size(); column++) {
                Class<?> type = support.parameterType(fields.get(column));
                for (int index = parameterIndex(column); index < parameterIndex(column + 1); index++) {
                    Object value = parameters[index];
                    types.add(offsets == null || value == null ? type : boundParameterType(value));
                }
            }
            return types;
        }

        /** UPSERT 阶段或后续 Map 重排只改变布局，不重复执行字段 codec。 */
        WriteFields select(List<DynamicField> selected) {
            if (fields.equals(selected)) return this;
            Map<DynamicField, Integer> indexes = new java.util.IdentityHashMap<>(fields.size());
            for (int index = 0; index < fields.size(); index++) indexes.put(fields.get(index), index);
            int count = 0;
            for (DynamicField field : selected) {
                int index = indexes.get(field);
                count += parameterIndex(index + 1) - parameterIndex(index);
            }
            Object[] bindings = new Object[count];
            int[] selectedOffsets = offsets == null ? null : new int[selected.size() + 1];
            List<String> selectedExpressions = offsets == null ? List.of() : new ArrayList<>(selected.size());
            int position = 0;
            for (int column = 0; column < selected.size(); column++) {
                int index = indexes.get(selected.get(column));
                int start = parameterIndex(index);
                int length = parameterIndex(index + 1) - start;
                System.arraycopy(parameters, start, bindings, position, length);
                if (selectedOffsets != null) {
                    selectedOffsets[column] = position;
                    selectedExpressions.add(expressions.get(index));
                }
                position += length;
            }
            if (selectedOffsets != null) selectedOffsets[selected.size()] = position;
            return new WriteFields(selected, bindings, selectedExpressions, selectedOffsets);
        }
    }

    record ConditionSql(String sql, List<Object> parameters, String shape, boolean cacheable) {

        static ConditionSql none() {
            return new ConditionSql("", List.of(), "none", true);
        }
    }

}
