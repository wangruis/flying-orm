package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormAggregateReadSupport;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * 把类型化聚合规格编译成 JDBC/R2DBC 共用的参数化 SQL、结果布局和解码事实。
 *
 * <p>一次规划同时完成 Scope/逻辑删除/保护条件、字段用途、查询形状和参数预算。函数、字段、
 * 别名及 HAVING 引用均来自已验证模型，业务值只进入 {@link SqlRequest#parameters()}。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class FormAggregatePlanner {

    private final FormAggregateReadSupport reads;
    private final SqlExecutionOptions defaultExecutionOptions;
    private final FieldUsePolicy fieldUsePolicy;
    private final QueryShapeLimits shapeLimits;
    private final boolean sqlServerDialect;
    private final boolean postgresqlDialect;

    public FormAggregatePlanner(FormDataSqlRenderer renderer,
                                StructuredConditionResolver resolver,
                                DataScope defaultDataScope,
                                SqlExecutionOptions defaultExecutionOptions,
                                FieldUsePolicy fieldUsePolicy,
                                QueryShapeLimits shapeLimits) {
        this.reads = new FormAggregateReadSupport(renderer, resolver, defaultDataScope);
        this.defaultExecutionOptions = Objects.requireNonNull(
                defaultExecutionOptions, "default aggregate execution options must not be null");
        this.fieldUsePolicy = Objects.requireNonNull(
                fieldUsePolicy, "aggregate field use policy must not be null");
        this.shapeLimits = Objects.requireNonNull(
                shapeLimits, "aggregate query shape limits must not be null");
        this.sqlServerDialect = reads.sqlServerDialect();
        this.postgresqlDialect = reads.postgresqlDialect();
    }

    /** 完成 SQL 前校验并返回可由同步/响应式执行器直接消费的不可变计划。 */
    public Plan plan(AggregateSpec spec) {
        return plan(spec, false);
    }

    /** 客户端传递显式治理状态；不会关闭字段策略或形状预算本身要求的审批。 */
    @InternalApi
    public Plan plan(AggregateSpec spec, boolean explicitGovernance) {
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        QuerySpec query = safeSpec.query();
        if (!query.projections().isEmpty() || !query.groups().isEmpty()) {
            throw new IllegalArgumentException(
                    "aggregate query projections and groups must be declared by AggregateSpec");
        }
        AggregateTypeSupport.rejectEncryptedSelections(safeSpec, query.form());

        boolean governed = explicitGovernance || fieldUsePolicy != FieldUsePolicy.unrestricted()
                || shapeLimits != QueryShapeLimits.defaults();
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        FormAggregateReadSupport.PreparedRead read;
        if (governed) {
            FormAggregateReadSupport.GovernedPreparedRead prepared = reads.prepareGoverned(query);
            read = prepared.read();
            collectCondition(prepared.businessWhere(), FieldUse.FILTER, requirements, null, true, reads);
        } else {
            read = reads.prepare(query);
            collectCallerFilters(query, requirements, false, reads);
        }
        AggregateRowLayout layout = AggregateRowLayout.of(safeSpec.groups(), safeSpec.aggregates());

        List<DynamicField> groupFields = new ArrayList<>(safeSpec.groups().size());
        List<DynamicField> aggregateFields = new ArrayList<>(safeSpec.aggregates().size());
        Map<String, ResultTarget> targets = new LinkedHashMap<>(Math.max(4, layout.size() * 2));
        DynamicForm.Builder resultForm = read.physicalForm().relationIdentity()
                .map(identity -> DynamicForm.relationalBuilder("aggregate_result", identity))
                .orElseGet(() -> DynamicForm.builder("aggregate_result", read.physicalForm().table()));
        StringJoiner select = new StringJoiner(", ");
        StringJoiner groupBy = new StringJoiner(", ");
        boolean correlatedHaving = safeSpec.having().isPresent() && reads.hasCorrelatedTerms();
        String outerQualifier = correlatedHaving ? reads.identifier(read.physicalForm()) : null;
        Map<String, String> correlatedExpressions = correlatedHaving ? new LinkedHashMap<>() : null;

        for (GroupSelection group : safeSpec.groups()) {
            DynamicField field = read.readableForm().field(group.field());
            AggregateTypeSupport.requireGroupable(field);
            String expression = reads.identifier(field.name());
            String alias = reads.identifier(group.alias());
            select.add(expression + " as " + alias);
            groupBy.add(expression);
            groupFields.add(field);
            DynamicField result = DynamicField.of(group.alias(), field.databaseType());
            resultForm.addField(result);
            targets.put(FieldIdentity.of(group.alias()).key(),
                        new ResultTarget(field, result, expression, field));
            if (correlatedHaving) {
                correlatedExpressions.put(FieldIdentity.of(group.alias()).key(),
                                          outerQualifier + "." + reads.identifier(field.name()));
            }
            requirements.require(field.name(), FieldUse.GROUP)
                        .require(field.name(), FieldUse.PROJECT);
        }

        for (AggregateExpression<?> aggregate : safeSpec.aggregates()) {
            DynamicField field = read.readableForm().field(aggregate.sourceField());
            AggregateTypeSupport.requireAggregateContract(aggregate, field, reads);
            String expression = renderFunction(
                    aggregate.function(), reads.identifier(field.name()),
                    field.databaseType().logicalType());
            select.add(expression + " as " + reads.identifier(aggregate.alias()));
            aggregateFields.add(field);
            DynamicField result = resultField(aggregate, field);
            DynamicField valueField = switch (aggregate.function()) {
                case MIN, MAX -> field;
                case COUNT, COUNT_DISTINCT, SUM, AVG -> result;
            };
            resultForm.addField(result);
            targets.put(FieldIdentity.of(aggregate.alias()).key(),
                        new ResultTarget(field, result, expression, valueField));
            if (correlatedHaving) {
                correlatedExpressions.put(FieldIdentity.of(aggregate.alias()).key(),
                        renderFunction(
                                aggregate.function(), outerQualifier + "." + reads.identifier(field.name()),
                                field.databaseType().logicalType()));
            }
            requirements.require(field.name(), FieldUse.AGGREGATE);
        }

        int havingNodes = safeSpec.having()
                .map(having -> collectHaving(
                        having.condition(), targets, requirements, governed, reads))
                .orElse(0);
        collectSorts(query.sorts(), targets, requirements, reads);
        if (governed) {
            requireCount(layout.size(), shapeLimits.maxProjectionCount(), "query projection count");
            requireCount(safeSpec.groups().size(), shapeLimits.maxGroupCount(), "query group count");
            requireCount(safeSpec.aggregates().size(), shapeLimits.maxAggregateCount(), "query aggregate count");
            requireCount(havingNodes, shapeLimits.maxHavingCount(), "query having count");
            requireCount(query.sorts().size(), shapeLimits.maxSortCount(), "query sort count");
        }

        SqlFragment where = reads.renderWhere(read.physicalForm(), read.where());
        DynamicForm aliases = resultForm.build();
        Map<String, String> expressions = new LinkedHashMap<>(targets.size());
        targets.forEach((alias, target) -> expressions.put(alias, target.expression()));
        SqlFragment having = safeSpec.having()
                .map(value -> reads.renderHaving(
                        aliases, value.condition(), expressions, correlatedExpressions, outerQualifier,
                        alias -> targets.get(FieldIdentity.of(alias).key()).valueField()))
                .orElseGet(() -> new SqlFragment("", List.of()));

        StringBuilder sql = new StringBuilder("select ")
                .append(select)
                .append(" from ")
                .append(reads.identifier(read.physicalForm()));
        appendClause(sql, " where ", where.sql());
        if (!safeSpec.groups().isEmpty()) {
            sql.append(" group by ").append(groupBy);
        }
        appendClause(sql, " having ", having.sql());
        appendOrder(sql, query.sorts(), aliases);

        List<Object> whereParameters = OwnedBindableValues.ownedValues(where.parameters());
        List<Object> havingParameters = OwnedBindableValues.ownedValues(having.parameters());
        List<Object> parameters = new ArrayList<>(whereParameters.size() + havingParameters.size());
        parameters.addAll(whereParameters);
        parameters.addAll(havingParameters);
        SqlRequest request = new SqlRequest(sql.toString(), parameters);
        FieldUseRequirements approvedRequirements = requirements.build();
        FieldUseSnapshot fieldUse = reads.approveAggregate(
                read.logicalForm().id(), approvedRequirements, read.scope().fields(), request,
                fieldUsePolicy, shapeLimits,
                governed ? 0 : layout.size(),
                governed ? 0 : safeSpec.groups().size(),
                governed ? 0 : safeSpec.aggregates().size(),
                governed ? 0 : havingNodes,
                governed ? 0 : query.sorts().size());
        validateResultVisibility(safeSpec.aggregates(), aggregateFields, fieldUse);
        return new Plan(
                request,
                query.executionOptions().orElse(defaultExecutionOptions),
                layout,
                reads,
                read.logicalForm(),
                groupFields,
                aggregateFields,
                fieldUsePolicy.isUnrestricted() ? query.sensitiveDisplayMode() : SensitiveDisplayMode.FULL,
                fieldUse);
    }

    private static void requireCount(int count, int limit, String name) {
        if (count > limit) {
            throw new IllegalArgumentException(name + " exceeds configured limit " + limit);
        }
    }

    private static void collectCallerFilters(QuerySpec query,
                                             FieldUseRequirements.Builder requirements,
                                             boolean governed,
                                             FormAggregateReadSupport reads) {
        if (query.structuredInput().isPresent()) {
            collectStructured(query.structuredInput().orElseThrow(), requirements);
        }
        collectCondition(query.where(), FieldUse.FILTER, requirements, null, governed, reads);
    }

    private static void collectStructured(StructuredConditionInput input,
                                          FieldUseRequirements.Builder requirements) {
        StructuredConditionInput safeInput = Objects.requireNonNull(
                input, "structured aggregate condition must not be null");
        for (StructuredConditionInput current : ConditionNodes.preorder(safeInput)) {
            if (current.field() != null && !current.field().isBlank()) {
                requirements.require(current.field(), FieldUse.FILTER);
            }
        }
    }

    private static int collectHaving(ConditionNode node,
                                     Map<String, ResultTarget> targets,
                                     FieldUseRequirements.Builder requirements,
                                     boolean governed,
                                     FormAggregateReadSupport reads) {
        return collectCondition(node, FieldUse.HAVING, requirements, targets, governed, reads);
    }

    private static int collectCondition(ConditionNode node,
                                        FieldUse use,
                                        FieldUseRequirements.Builder requirements,
                                        Map<String, ResultTarget> targets,
                                        boolean governed,
                                        FormAggregateReadSupport reads) {
        ConditionNode safeNode = Objects.requireNonNull(node, "aggregate condition node must not be null");
        int nodes = 0;
        for (ConditionNode current : ConditionNodes.preorder(safeNode)) {
            nodes = Math.addExact(nodes, 1);
            if (!(current instanceof TermCondition term)) continue;
            String field = term.field();
            if (targets == null) {
                requirements.require(field, use);
            } else {
                ResultTarget target = targets.get(FieldIdentity.of(field).key());
                if (target == null) {
                    throw new IllegalArgumentException(
                            "HAVING may reference only declared group or aggregate aliases");
                }
                requirements.require(target.source().name(), use);
            }
            if (governed) {
                reads.approveTermExtension(term, use);
            }
        }
        return nodes;
    }

    private static void collectSorts(List<PageSort> sorts,
                                     Map<String, ResultTarget> targets,
                                     FieldUseRequirements.Builder requirements,
                                     FormAggregateReadSupport reads) {
        for (PageSort sort : sorts) {
            ResultTarget target = targets.get(FieldIdentity.of(sort.field()).key());
            if (target == null) {
                throw new IllegalArgumentException(
                        "aggregate ordering may reference only declared result aliases");
            }
            reads.requireStableOffsetTimeOrdering(target.result());
            requirements.require(target.source().name(), FieldUse.SORT);
        }
    }

    private void appendOrder(StringBuilder sql,
                             List<PageSort> sorts,
                             DynamicForm aliases) {
        if (sorts.isEmpty()) {
            return;
        }
        StringJoiner order = new StringJoiner(", ", " order by ", "");
        for (PageSort sort : sorts) {
            order.add(reads.identifier(aliases.field(sort.field()).name()) + " " + sort.sqlKeyword());
        }
        sql.append(order);
    }

    private static void appendClause(StringBuilder sql, String prefix, String fragment) {
        if (!fragment.isBlank()) {
            sql.append(prefix).append(fragment);
        }
    }

    private static DynamicField resultField(AggregateExpression<?> aggregate, DynamicField source) {
        return switch (aggregate.function()) {
            case COUNT, COUNT_DISTINCT -> DynamicField.of(aggregate.alias(), "BIGINT");
            case SUM, AVG -> DynamicField.of(aggregate.alias(), "DECIMAL");
            case MIN, MAX -> DynamicField.of(aggregate.alias(), source.databaseType());
        };
    }

    /** 渲染已经通过类型契约校验的聚合表达式。 */
    private String renderFunction(AggregateFunction function,
                                  String field,
                                  LogicalType sourceType) {
        if (postgresqlDialect && sourceType == LogicalType.UUID
                && (function == AggregateFunction.MIN || function == AggregateFunction.MAX)) {
            // PostgreSQL 无内置 UUID MIN/MAX；规范十六进制文本按 C 排序等价于 UUID 字节序。
            // 转回 UUID，保持结果类型与 HAVING 参数比较一致；NULL 仍由聚合原生忽略。
            String name = function == AggregateFunction.MIN ? "min" : "max";
            return "cast(" + name + "(cast(" + field + " as text) collate \"C\") as uuid)";
        }
        return switch (function) {
            case COUNT -> (sqlServerDialect ? "count_big" : "count") + "(" + field + ")";
            case COUNT_DISTINCT -> (sqlServerDialect ? "count_big" : "count") + "(distinct " + field + ")";
            // SQL Server 会把整数 SUM/AVG 保留在源整数类型族。聚合前提升输入，
            // 同时避免 SUM 溢出和 AVG 整除截断；聚合后再 cast 已无法恢复丢失的事实。
            case SUM -> "sum(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case AVG -> "avg(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case MIN -> "min(" + field + ")";
            case MAX -> "max(" + field + ")";
        };
    }

    private static String stableDecimalInput(String field,
                                             LogicalType sourceType,
                                             boolean sqlServerDialect) {
        boolean integerSource = switch (sourceType) {
            case SMALL_INTEGER, INTEGER, BIG_INTEGER -> true;
            default -> false;
        };
        return sqlServerDialect && integerSource
                ? "cast(" + field + " as decimal(38,10))"
                : field;
    }

    /** COUNT 的结果固定为 Long，不能继承源文本字段的脱敏器。 */
    private static void validateResultVisibility(List<AggregateExpression<?>> aggregates,
                                                 List<DynamicField> fields,
                                                 FieldUseSnapshot fieldUse) {
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index).function();
            if ((function == AggregateFunction.COUNT || function == AggregateFunction.COUNT_DISTINCT)
                    && fieldUse.visibility(fields.get(index).name()) == FieldVisibility.MASKED) {
                throw new IllegalArgumentException(
                        "COUNT output cannot inherit MASKED visibility from its source field");
            }
        }
    }

    private record ResultTarget(DynamicField source, DynamicField result, String expression,
                                DynamicField valueField) {
        private ResultTarget {
            source = Objects.requireNonNull(source, "aggregate result source must not be null");
            result = Objects.requireNonNull(result, "aggregate result field must not be null");
            expression = Objects.requireNonNull(expression, "aggregate result expression must not be null");
            valueField = Objects.requireNonNull(valueField, "aggregate value source field must not be null");
        }
    }

    /** JDBC/R2DBC 共用的 SQL、布局和解码事实。 */
    public static final class Plan {

        private final SqlRequest request;
        private final SqlExecutionOptions options;
        private final AggregateRowLayout layout;
        private final FormAggregateReadSupport reads;
        private final DynamicForm form;
        private final List<DynamicField> groupFields;
        private final List<DynamicField> aggregateFields;
        private final SensitiveDisplayMode displayMode;
        private final FieldUseSnapshot fieldUse;

        private Plan(SqlRequest request,
                     SqlExecutionOptions options,
                     AggregateRowLayout layout,
                     FormAggregateReadSupport reads,
                     DynamicForm form,
                     List<DynamicField> groupFields,
                     List<DynamicField> aggregateFields,
                     SensitiveDisplayMode displayMode,
                     FieldUseSnapshot fieldUse) {
            this.request = Objects.requireNonNull(request, "aggregate SQL request must not be null");
            this.options = Objects.requireNonNull(options, "aggregate execution options must not be null");
            this.layout = Objects.requireNonNull(layout, "aggregate row layout must not be null");
            this.reads = Objects.requireNonNull(reads, "aggregate read support must not be null");
            this.form = Objects.requireNonNull(form, "aggregate form must not be null");
            this.groupFields = List.copyOf(groupFields);
            this.aggregateFields = List.copyOf(aggregateFields);
            this.displayMode = Objects.requireNonNull(displayMode, "aggregate display mode must not be null");
            this.fieldUse = Objects.requireNonNull(fieldUse, "aggregate field use snapshot must not be null");
        }

        public SqlRequest request() {
            return request;
        }

        public SqlExecutionOptions options() {
            return options;
        }

        public AggregateRowLayout layout() {
            return layout;
        }

        public FieldUseSnapshot fieldUse() {
            return fieldUse;
        }

        FormAggregateReadSupport reads() {
            return reads;
        }

        DynamicForm form() {
            return form;
        }

        List<DynamicField> groupFields() {
            return groupFields;
        }

        List<DynamicField> aggregateFields() {
            return aggregateFields;
        }

        SensitiveDisplayMode displayMode() {
            return displayMode;
        }
    }
}
