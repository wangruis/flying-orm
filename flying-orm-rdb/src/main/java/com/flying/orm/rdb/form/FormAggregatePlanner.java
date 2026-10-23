package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateFunction;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.aggregate.AggregateRowLayout;

import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
final class FormAggregatePlanner {

    private static final DatabaseType COUNT_RESULT_TYPE = DatabaseType.of("BIGINT");
    private static final DatabaseType DECIMAL_RESULT_TYPE = DatabaseType.of("DECIMAL");

    private final FormAggregateReadSupport reads;
    private final SqlExecutionOptions defaultExecutionOptions;
    private final FieldUsePolicy fieldUsePolicy;
    private final QueryShapeLimits shapeLimits;
    private final boolean sqlServerDialect;
    private final boolean postgresqlDialect;

    FormAggregatePlanner(FormDataSqlRenderer renderer,
                                FormScopeSupport scopes,
                                SqlExecutionOptions defaultExecutionOptions,
                                FieldUsePolicy fieldUsePolicy,
                                QueryShapeLimits shapeLimits) {
        this.reads = new FormAggregateReadSupport(renderer, scopes);
        this.defaultExecutionOptions = Objects.requireNonNull(
                defaultExecutionOptions, "default aggregate execution options must not be null");
        this.fieldUsePolicy = Objects.requireNonNull(
                fieldUsePolicy, "aggregate field use policy must not be null");
        this.shapeLimits = Objects.requireNonNull(
                shapeLimits, "aggregate query shape limits must not be null");
        this.sqlServerDialect = reads.renderer.sqlServerDialect();
        this.postgresqlDialect = reads.renderer.postgresqlDialect();
    }

    /** 完成 SQL 前校验并返回可由同步/响应式执行器直接消费的不可变计划。 */
    Plan plan(AggregateSpec spec) {
        return plan(spec, false);
    }

    /** 客户端传递显式治理状态；不会关闭字段策略或形状预算本身要求的审批。 */
    Plan plan(AggregateSpec spec, boolean explicitGovernance) {
        return compile(spec, explicitGovernance, null);
    }

    /** 同一次规划生成最终分组总数与数据库分页请求，保留 HAVING 和字段审批。 */
    Plan planPage(AggregateSpec spec, PageQuery page, boolean explicitGovernance) {
        return compile(spec, explicitGovernance,
                Objects.requireNonNull(page, "aggregate page must not be null"));
    }

    private Plan compile(AggregateSpec spec, boolean explicitGovernance, PageQuery page) {
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        boolean joined = safeSpec.joinQuery().isPresent();
        QuerySpec query = joined ? null : safeSpec.query();
        DynamicForm rootForm = joined ? safeSpec.joinQuery().orElseThrow().root().form() : query.form();
        List<PageSort> sorts = page == null || page.sorts().isEmpty() ? safeSpec.sorts() : page.sorts();
        Map<String, ValueExpression> sortExpressions = page == null || page.sorts().isEmpty()
                ? safeSpec.sortExpressions() : Map.of();
        if (query != null && (!query.projections().isEmpty() || !query.groups().isEmpty())) {
            throw new IllegalArgumentException(
                    "aggregate query projections and groups must be declared by AggregateSpec");
        }
        AggregateTypeSupport.rejectEncryptedSelections(safeSpec);

        boolean governed = explicitGovernance || fieldUsePolicy != FieldUsePolicy.unrestricted()
                || !shapeLimits.equals(QueryShapeLimits.defaults());
        FieldUseRequirements.Builder requirements;
        FormAggregateReadSupport.PreparedRead read = null;
        FormAggregateReadSupport.JoinedRead joinRead = null;
        if (joined) {
            requirements = FieldUseRequirements.builder();
            joinRead = reads.prepareJoin(safeSpec.joinQuery().orElseThrow(), requirements, shapeLimits, governed);
        } else if (governed) {
            requirements = FieldUseRequirements.builder();
            FormAggregateReadSupport.GovernedPreparedRead prepared = reads.prepareGoverned(query);
            read = prepared.read();
            collectCondition(prepared.businessWhere(), FieldUse.FILTER, requirements, null, true, reads);
        } else {
            read = reads.prepare(query);
            // 双向无限制的默认单表审批直接返回共享快照，无需构建随后丢弃的用途集合。
            requirements = read.scope().fields().unrestrictedRead() && read.scope().fields().unrestrictedWrite()
                    ? null : FieldUseRequirements.builder();
            if (requirements != null) {
                collectCallerFilters(query, requirements, false, reads);
            }
        }
        AggregateRowLayout layout = safeSpec.layout();

        List<AggregateInput> groupFields = new ArrayList<>(safeSpec.groups().size());
        List<AggregateInput> aggregateFields = new ArrayList<>(safeSpec.aggregates().size());
        List<Object> selectParameters = new ArrayList<>();
        boolean computedGroups = safeSpec.groups().stream().anyMatch(GroupSelection::computed);
        StringJoiner inputColumns = computedGroups ? new StringJoiner(", ") : null;
        Map<AggregateField, String> inputFields = computedGroups ? new LinkedHashMap<>() : null;
        List<Object> groupParameters = computedGroups ? new ArrayList<>() : List.of();
        boolean havingDeclared = safeSpec.having().isPresent();
        Map<String, AggregateResultTarget> targets = havingDeclared
                ? new LinkedHashMap<>(layout.size()) : new LinkedHashMap<>(sorts.size());
        if (!havingDeclared) {
            // 无 HAVING 时仅排序需要别名目标；未知排序别名仍由 collectSorts 统一拒绝。
            for (PageSort sort : sorts) {
                ValueExpression expression = sortExpressions.get(sort.field());
                if (expression == null) targets.put(FieldIdentity.of(sort.field()).key(), null);
                else expression.forEachField(field -> targets.put(field.identity().key(), null));
            }
        }
        StringJoiner select = new StringJoiner(", ");
        StringJoiner groupBy = new StringJoiner(", ");
        boolean correlatedHaving = safeSpec.having()
                .map(condition -> reads.hasCorrelatedTerms(rootForm, condition)).orElse(false);
        boolean resultHaving = computedGroups && havingDeclared || correlatedHaving && (joined || safeSpec.aggregates().stream()
                .anyMatch(aggregate -> aggregate.function() == AggregateFunction.COUNT_ROWS))
                || safeSpec.having().isPresent() && safeSpec.aggregates().stream().anyMatch(aggregate -> aggregate.operand() != null
                        && !(aggregate.operand() instanceof ValueExpression.Field));
        String outerQualifier = resultHaving ? reads.renderer.conditionRenderer().identifier("flying_aggregate_result")
                : correlatedHaving ? reads.renderer.relationIdentifier(read.physicalForm()) : null;
        Map<String, String> correlatedExpressions = correlatedHaving ? new LinkedHashMap<>() : null;

        for (GroupSelection group : safeSpec.groups()) {
            AggregateInput input = group.field() != null
                    ? AggregateInput.field(source(safeSpec.source(group.alias()), group.field(), read, joinRead))
                    : input(safeSpec, group.alias(), group.expression(), read, joinRead, null, null);
            DynamicField field = input.valueField();
            AggregateTypeSupport.requireComparableInput(input);
            String expression = input.fragment().sql();
            if (computedGroups) {
                String column = reads.renderer.conditionRenderer().identifier("__fo_group_" + groupFields.size());
                inputColumns.add(expression + " as " + column);
                groupParameters.addAll(input.fragment().parameters());
                expression = reads.renderer.conditionRenderer().identifier("flying_group_input") + "." + column;
            }
            String alias = reads.renderer.conditionRenderer().identifier(group.alias());
            select.add(expression + " as " + alias);
            groupBy.add(expression);
            groupFields.add(input);
            String targetKey = FieldIdentity.of(group.alias()).key();
            if (havingDeclared || targets.containsKey(targetKey)) {
                DynamicField result = DynamicField.of(group.alias(), field.databaseType());
                targets.put(targetKey, new AggregateResultTarget(input.sources(), result, expression, field, List.of()));
            }
            if (correlatedHaving) {
                correlatedExpressions.put(FieldIdentity.of(group.alias()).key(),
                        outerQualifier + "." + reads.renderer.conditionRenderer().identifier(resultHaving ? group.alias() : field.name()));
            }
            for (AggregateField source : input.sources()) {
                source.requireUse(requirements, FieldUse.GROUP);
                source.requireUse(requirements, FieldUse.PROJECT);
            }
        }

        for (AggregateExpression<?> aggregate : safeSpec.aggregates()) {
            AggregateInput input = aggregate.operand() == null
                    ? AggregateInput.field(null)
                    : input(safeSpec, aggregate.alias(), aggregate.operand(), read, joinRead, inputFields, inputColumns);
            DynamicField field = input.valueField();
            AggregateTypeSupport.requireAggregateContract(aggregate, input, reads);
            String expression = renderFunction(aggregate.function(), input.fragment().sql(),
                    field == null ? null : field.databaseType().logicalType());
            selectParameters.addAll(input.fragment().parameters());
            select.add(expression + " as " + reads.renderer.conditionRenderer().identifier(aggregate.alias()));
            aggregateFields.add(input);
            String targetKey = FieldIdentity.of(aggregate.alias()).key();
            if (havingDeclared || targets.containsKey(targetKey)) {
                DynamicField result = resultField(aggregate, field);
                DynamicField valueField = switch (aggregate.function()) {
                    case MIN, MAX, REGISTERED -> field;
                    case COUNT_ROWS, COUNT, COUNT_DISTINCT, SUM, AVG,
                            STDDEV_POP, STDDEV_SAMPLE, VARIANCE_POP, VARIANCE_SAMPLE -> result;
                };
                targets.put(targetKey, new AggregateResultTarget(input.sources(), result, expression, valueField, input.fragment().parameters()));
            }
            if (correlatedHaving) {
                correlatedExpressions.put(FieldIdentity.of(aggregate.alias()).key(),
                        resultHaving ? outerQualifier + "." + reads.renderer.conditionRenderer().identifier(aggregate.alias())
                                : renderAggregate(aggregate.function(), field, outerQualifier));
            }
            for (AggregateField source : input.sources()) {
                source.requireUse(requirements, FieldUse.AGGREGATE);
            }
        }

        int havingNodes = safeSpec.having()
                .map(having -> collectCondition(
                        having, FieldUse.HAVING, requirements, targets, governed, reads))
                .orElse(0);
        if (governed) {
            requireCount(layout.size(), shapeLimits.maxProjectionCount(), "query projection count");
            requireCount(safeSpec.groups().size(), shapeLimits.maxGroupCount(), "query group count");
            requireCount(safeSpec.aggregates().size(), shapeLimits.maxAggregateCount(), "query aggregate count");
            requireCount(havingNodes, shapeLimits.maxHavingCount(), "query having count");
            requireCount(sorts.size(), shapeLimits.maxSortCount(), "query sort count");
        }

        SqlFragment relation;
        FormSqlRenderSupport.ConditionSql condition = null;
        if (joinRead != null) {
            relation = joinRead.relation();
        } else {
            condition = reads.renderer.support.condition(read.physicalForm(), read.where());
            relation = new SqlFragment("from " + reads.renderer.relationIdentifier(read.physicalForm())
                    + (condition.sql().isBlank() ? "" : " where " + condition.sql()),
                    OwnedBindableValues.ownedValues(condition.parameters()));
        }
        if (computedGroups) {
            // 分组表达式只绑定一次；外层引用派生列，不依赖方言的 GROUP BY 别名或参数等价推断。
            groupParameters.addAll(relation.parameters());
            relation = new SqlFragment("from (select " + inputColumns + " " + relation.sql() + ") "
                    + reads.renderer.conditionRenderer().identifier("flying_group_input"), groupParameters);
        }
        SqlFragment having = safeSpec.having()
                .map(value -> {
                    DynamicForm.Builder aliases = DynamicForm.builder("aggregate_result", "aggregate_result")
                            .features(rootForm.features());
                    Map<String, String> expressions = new LinkedHashMap<>(targets.size());
                    targets.forEach((alias, target) -> {
                        aliases.addField(target.result());
                        expressions.put(alias, resultHaving
                                ? outerQualifier + "." + reads.renderer.conditionRenderer().identifier(target.result().name()) : target.expression());
                    });
                    return reads.renderHaving(aliases.build(), value, expressions,
                            correlatedExpressions, outerQualifier,
                            alias -> targets.get(FieldIdentity.of(alias).key()).valueField());
                })
                .orElseGet(() -> new SqlFragment("", List.of()));

        String projectionSql = select.toString();
        String groupSql = groupBy.toString();
        StringBuilder sql = new StringBuilder("select ")
                .append(projectionSql)
                .append(' ').append(relation.sql());
        if (!safeSpec.groups().isEmpty()) {
            sql.append(" group by ").append(groupSql);
        }
        if (resultHaving) {
            // 无源字段的 COUNT(*) 不能搬进关联子查询，否则它会统计内层关系。
            // 将 HAVING 施加于同一 SQL 内的聚合结果，外层引用统一使用结果别名。
            sql.insert(0, "select * from (").append(") ").append(outerQualifier);
            appendClause(sql, " where ", having.sql());
        } else {
            appendClause(sql, " having ", having.sql());
        }
        String countSourceSql = page == null ? null : sql.toString();
        int orderStart = sql.length();
        List<Object> orderParameters = AggregateResultTarget.appendOrder(sql, sorts, sortExpressions, targets, reads, requirements,
                resultHaving ? outerQualifier : null);

        List<Object> parameters = OwnedBindableValues.ownedValues(relation.parameters());
        List<Object> havingParameters = OwnedBindableValues.ownedValues(having.parameters());
        if (!selectParameters.isEmpty() || !havingParameters.isEmpty()) {
            selectParameters.addAll(parameters);
            selectParameters.addAll(havingParameters);
            parameters = OwnedBindableValues.publishOwned(selectParameters);
        }
        SqlRequest request;
        SqlRequest countRequest = null;
        if (page != null) {
            countRequest = reads.renderer.compileRequest("select " + (sqlServerDialect ? "count_big" : "count")
                    + "(*) as " + reads.renderer.conditionRenderer().identifier("total") + " from (" + countSourceSql + ") "
                    + reads.renderer.conditionRenderer().identifier("flying_aggregate_count"), parameters);
            FieldUseGuard.accountRenderedRequest(new QueryShapeBudget(shapeLimits), countRequest);
        }
        if (!orderParameters.isEmpty()) {
            List<Object> combined = new ArrayList<>(parameters.size() + orderParameters.size());
            combined.addAll(parameters);
            combined.addAll(orderParameters);
            parameters = OwnedBindableValues.publishOwned(combined);
        }
        if (page == null && !havingDeclared && !computedGroups && condition != null
                && reads.renderer.support.sqlPlansEnabled && condition.cacheable()) {
            // 只复用 SQL 结构，当前参数、字段审批与结果解码仍由本次规划负责。
            request = reads.renderer.support.request("aggregate", read.physicalForm(), List.of(projectionSql),
                    condition, groupSql, sql.substring(orderStart), "", parameters, sql::toString);
        } else {
            request = page == null ? reads.renderer.compileRequest(sql.toString(), parameters)
                    : reads.renderer.compileRequest(reads.renderer.paginate(new SqlRequest(sql.toString(), parameters), page));
        }
        FieldUseRequirements approvedRequirements = requirements == null
                ? FieldUseRequirements.empty() : requirements.build();
        FieldUseSnapshot fieldUse;
        if (joinRead != null) {
            FieldUseGuard.accountRenderedRequest(new QueryShapeBudget(shapeLimits), request);
            fieldUse = joinRead.approve(approvedRequirements, fieldUsePolicy);
        } else {
            fieldUse = FieldUseGuard.approveAggregate(
                read.logicalForm().id(), approvedRequirements, read.scope().fields(), request,
                fieldUsePolicy, shapeLimits,
                governed ? 0 : layout.size(),
                governed ? 0 : safeSpec.groups().size(),
                governed ? 0 : safeSpec.aggregates().size(),
                governed ? 0 : havingNodes,
                governed ? 0 : sorts.size());
        }
        SensitiveDisplayMode displayMode = fieldUsePolicy.isUnrestricted()
                ? safeSpec.sensitiveDisplayMode() : SensitiveDisplayMode.FULL;
        validateResultVisibility(safeSpec.aggregates(), aggregateFields, fieldUse, displayMode);
        for (int index = 0; index < safeSpec.groups().size(); index++) {
            if (safeSpec.groups().get(index).computed()) {
                groupFields.get(index).requireVisibility(fieldUse, displayMode, true, true);
            }
        }
        return new Plan(
                request,
                countRequest,
                safeSpec.executionOptions().orElse(defaultExecutionOptions),
                layout,
                reads,
                groupFields,
                aggregateFields,
                displayMode,
                fieldUse);
    }

    private AggregateInput input(AggregateSpec spec, String alias, ValueExpression operand,
                                   FormAggregateReadSupport.PreparedRead read,
                                   FormAggregateReadSupport.JoinedRead joinRead,
                                   Map<AggregateField, String> inputFields, StringJoiner inputColumns) {
        return AggregateInput.computed(operand, field -> {
            JoinSource owner = field.source() == null ? spec.source(alias) : field.source();
            if (field.source() != null && (joinRead == null || !spec.joinQuery().orElseThrow().sources().contains(owner))) {
                throw new IllegalArgumentException("expression source must belong to the aggregate relation");
            }
            AggregateField source = source(owner, field.identity().name(), read, joinRead);
            if (inputFields == null) return source;
            String column = inputFields.computeIfAbsent(source, key -> {
                String name = reads.renderer.conditionRenderer().identifier("__fo_field_" + inputFields.size());
                inputColumns.add(key.expression() + " as " + name);
                return reads.renderer.conditionRenderer().identifier("flying_group_input") + "." + name;
            });
            return new AggregateField(source.form(), source.field(), source.reference(), column);
        }, reads);
    }

    private AggregateField source(JoinSource source, String name,
                                   FormAggregateReadSupport.PreparedRead read,
                                   FormAggregateReadSupport.JoinedRead joinRead) {
        if (source == null) {
            DynamicField field = read.readableForm().field(name);
            return new AggregateField(read.logicalForm(), field, null, reads.renderer.conditionRenderer().identifier(field.name()));
        }
        DynamicField field = joinRead.field(source, name);
        return new AggregateField(source.form(), field, new JoinFieldRef(source, field.name()),
                joinRead.expression(source, field.name()));
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

    private static int collectCondition(ConditionNode node,
                                        FieldUse use,
                                        FieldUseRequirements.Builder requirements,
                                        Map<String, AggregateResultTarget> targets,
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
                AggregateResultTarget target = targets.get(FieldIdentity.of(field).key());
                if (target == null) {
                    throw new IllegalArgumentException(
                            "HAVING may reference only declared group or aggregate aliases");
                }
                target.requireUse(requirements, use);
            }
            if (governed) {
                FieldUseGuard.approveTermExtension(reads.renderer, term, use);
            }
        }
        return nodes;
    }

    private static void appendClause(StringBuilder sql, String prefix, String fragment) {
        if (!fragment.isBlank()) {
            sql.append(prefix).append(fragment);
        }
    }

    private static DynamicField resultField(AggregateExpression<?> aggregate, DynamicField source) {
        return switch (aggregate.function()) {
            case COUNT_ROWS, COUNT, COUNT_DISTINCT -> DynamicField.of(aggregate.alias(), COUNT_RESULT_TYPE);
            case SUM, AVG, STDDEV_POP, STDDEV_SAMPLE, VARIANCE_POP, VARIANCE_SAMPLE ->
                    DynamicField.of(aggregate.alias(), DECIMAL_RESULT_TYPE);
            case MIN, MAX, REGISTERED -> DynamicField.of(aggregate.alias(), source.databaseType());
        };
    }

    private String renderAggregate(AggregateFunction function, DynamicField field, String qualifier) {
        String source = field == null ? null : reads.renderer.conditionRenderer().identifier(field.name());
        if (source != null && qualifier != null) {
            source = qualifier + "." + source;
        }
        return renderFunction(function, source, field == null ? null : field.databaseType().logicalType());
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
            case REGISTERED -> field;
            case COUNT_ROWS -> (sqlServerDialect ? "count_big" : "count") + "(*)";
            case COUNT -> (sqlServerDialect ? "count_big" : "count") + "(" + field + ")";
            case COUNT_DISTINCT -> (sqlServerDialect ? "count_big" : "count") + "(distinct " + field + ")";
            // SQL Server 会把整数 SUM/AVG 保留在源整数类型族。聚合前提升输入，
            // 同时避免 SUM 溢出和 AVG 整除截断；聚合后再 cast 已无法恢复丢失的事实。
            case SUM -> "sum(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case AVG -> "avg(" + stableDecimalInput(field, sourceType, sqlServerDialect) + ")";
            case STDDEV_POP -> (sqlServerDialect ? "stdevp" : "stddev_pop") + "(" + field + ")";
            case STDDEV_SAMPLE -> (sqlServerDialect ? "stdev" : "stddev_samp") + "(" + field + ")";
            case VARIANCE_POP -> (sqlServerDialect ? "varp" : "var_pop") + "(" + field + ")";
            case VARIANCE_SAMPLE -> (sqlServerDialect ? "var" : "var_samp") + "(" + field + ")";
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
                                                 List<AggregateInput> fields,
                                                 FieldUseSnapshot fieldUse,
                                                 SensitiveDisplayMode displayMode) {
        for (int index = 0; index < aggregates.size(); index++) {
            AggregateFunction function = aggregates.get(index).function();
            boolean computed = aggregates.get(index).operand() != null
                    && !(aggregates.get(index).operand() instanceof ValueExpression.Field);
            boolean changedType = computed || function == AggregateFunction.COUNT || function == AggregateFunction.COUNT_DISTINCT;
            fields.get(index).requireVisibility(fieldUse, displayMode, changedType, computed);
        }
    }


    /** JDBC/R2DBC 共用的 SQL、布局和解码事实。 */
    static final class Plan {

        private final SqlRequest request;
        private final SqlRequest countRequest;
        private final SqlExecutionOptions options;
        private final AggregateRowLayout layout;
        private final FormAggregateReadSupport reads;
        private final List<AggregateInput> groupFields;
        private final List<AggregateInput> aggregateFields;
        private final SensitiveDisplayMode displayMode;
        private final FieldUseSnapshot fieldUse;

        private Plan(SqlRequest request,
                     SqlRequest countRequest,
                     SqlExecutionOptions options,
                     AggregateRowLayout layout,
                     FormAggregateReadSupport reads,
                     List<AggregateInput> groupFields,
                     List<AggregateInput> aggregateFields,
                     SensitiveDisplayMode displayMode,
                     FieldUseSnapshot fieldUse) {
            this.request = Objects.requireNonNull(request, "aggregate SQL request must not be null");
            this.countRequest = countRequest;
            this.options = Objects.requireNonNull(options, "aggregate execution options must not be null");
            this.layout = Objects.requireNonNull(layout, "aggregate row layout must not be null");
            this.reads = Objects.requireNonNull(reads, "aggregate read support must not be null");
            this.groupFields = List.copyOf(groupFields);
            this.aggregateFields = List.copyOf(aggregateFields);
            this.displayMode = Objects.requireNonNull(displayMode, "aggregate display mode must not be null");
            this.fieldUse = Objects.requireNonNull(fieldUse, "aggregate field use snapshot must not be null");
        }

        SqlRequest request() {
            return request;
        }

        /** 仅分页计划提供总数请求；统计 HAVING 之后的结果行，而非基础表行。 */
        Optional<SqlRequest> countRequest() {
            return Optional.ofNullable(countRequest);
        }

        SqlExecutionOptions options() {
            return options;
        }

        AggregateRowLayout layout() {
            return layout;
        }

        FieldUseSnapshot fieldUse() {
            return fieldUse;
        }

        FormAggregateReadSupport reads() {
            return reads;
        }

        List<AggregateInput> groupFields() {
            return groupFields;
        }

        List<AggregateInput> aggregateFields() {
            return aggregateFields;
        }

        SensitiveDisplayMode displayMode() {
            return displayMode;
        }
    }
}
