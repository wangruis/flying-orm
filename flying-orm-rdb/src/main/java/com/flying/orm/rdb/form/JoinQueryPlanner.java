package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinClause;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.scope.JoinFieldDecision;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.form.FormReadPlanner.PlannedQuery;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.IdentityHashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * 把 JOIN AST、默认 Scope 和本次 Scope 固定为可执行的只读计划。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class JoinQueryPlanner {

    private static final ConditionGroup EMPTY = ConditionGroup.and().build();

    private final FormDataSqlRenderer renderer;
    private final FormScopeSupport scopes;
    private final SqlExecutionOptions defaultOptions;
    private final JoinResultProtector results;

    JoinQueryPlanner(FormDataSqlRenderer renderer,
                     FormScopeSupport scopes,
                     SqlExecutionOptions defaultOptions) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = Objects.requireNonNull(scopes, "form scope support must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "default SQL execution options must not be null");
        this.results = new JoinResultProtector(this.renderer);
    }

    /** 普通查询只在引用关系字段时绑定来源，未引用的模型关系不解析。 */
    JoinQuerySpec relatedQuery(QuerySpec spec, PageQuery page) {
        return relatedQuery(spec, page, true);
    }

    JoinQuerySpec relatedKeysetQuery(QuerySpec spec, KeysetPageQuery page) {
        if (spec.form().relations().isEmpty()) return null;
        return relatedQuery(spec, new PageQuery(1, page.size(), page.sorts().stream()
                .map(sort -> new PageSort(sort.field(), PageSort.Direction.valueOf(sort.direction().name()))).toList()));
    }

    JoinQuerySpec relatedQuery(QuerySpec spec, PageQuery page, boolean output) {
        DynamicForm form = spec.form();
        if (form.relations().isEmpty() || !referencesRelation(spec, page)) return null;
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(form);
        query.scope(query.root(), spec.scope());
        switch (spec.sensitiveDisplayMode()) {
            case FULL -> query.showSensitive();
            case MASKED -> query.masked();
            case DECLARED -> query.declaredDisplay();
        }
        ConditionGroup where = spec.where();
        if (spec.structuredInput().isPresent()) {
            StructuredConditionPolicy policy = FormScopeSupport.protectFrontendConditionFields(form,
                    spec.structuredPolicy().orElseThrow().withTerms(renderer.conditionRenderer().terms()),
                    scopes.effectiveScope(spec.scope()).fields());
            ConditionGroup compiled = scopes.structuredConditionResolver.compileFields(form, path -> {
                JoinFieldRef reference = query.field(path);
                DynamicForm source = reference.source().form();
                String name = reference.field();
                FieldScope fields = scopes.effectiveScope(reference.source().equals(query.root())
                        ? spec.scope() : DataScope.none()).fields();
                if (!fields.canRead(name)
                        || source.tenant().map(tenant -> tenant.fieldName().equalsIgnoreCase(name)).orElse(false)
                        || source.logicDelete().map(deleted -> deleted.fieldName().equalsIgnoreCase(name)).orElse(false)) {
                    throw StructuredConditionException.field(StructuredConditionErrorCode.FIELD_NOT_ALLOWED,
                            null, path, "structured condition field is not allowed");
                }
                return source.field(name);
            }, spec.structuredInput().orElseThrow(), policy);
            where = ConditionGroups.isEmpty(where) ? compiled : ConditionGroups.and(where, compiled);
        }
        query.where(where);
        for (String group : spec.groups()) query.groupBy(group);
        if (!output) {
            // 输出引用的关系仍决定行基数，但 count 不验证输出列、别名或排序。
            for (String path : spec.projections()) {
                if (relatedField(form, path)) query.related(path.substring(0, path.indexOf('.')));
            }
            for (PageSort sort : spec.sorts()) {
                if (relatedField(form, sort.field())) query.related(sort.field().substring(0, sort.field().indexOf('.')));
            }
            for (ValueExpression expression : spec.expressions().values()) expression.forEachField(field -> {
                String path = field.identity().name();
                if (relatedField(form, path)) query.related(path.substring(0, path.indexOf('.')));
            });
            return query.buildRelation();
        }
        Map<String, ValueExpression> expressions = spec.expressions().isEmpty() ? Map.of() : new LinkedHashMap<>();
        spec.expressions().forEach((alias, expression) -> expressions.put(alias,
                ExpressionFieldValue.mapFields(expression, field -> field.source() == null
                        ? ValueExpression.field(query.field(field.identity().name())) : field)));
        DynamicForm readable = FormScopeSupport.readableForm(form, scopes.effectiveScope(spec.scope()).fields());
        for (String path : FormQueryShapeGuard.projectionFields(spec, readable)) {
            ValueExpression expression = expressions.get(FieldIdentity.of(path).key());
            if (expression != null) query.select(expression, path);
            else {
                JoinFieldRef field = query.field(path);
                query.selectAs(field.source(), field.field(), field.field());
            }
        }
        List<PageSort> sorts = spec.sorts().isEmpty() && page != null ? page.sorts() : spec.sorts();
        for (PageSort sort : sorts) {
            ValueExpression expression = expressions.get(FieldIdentity.of(sort.field()).key());
            if (expression == null) query.orderBy(sort.field(), sort.direction());
            else query.orderBy(expression, sort.direction());
        }
        return query.build();
    }

    private static boolean referencesRelation(QuerySpec spec, PageQuery page) {
        for (String field : spec.projections()) if (relatedField(spec.form(), field)) return true;
        for (String field : spec.groups()) if (relatedField(spec.form(), field)) return true;
        for (PageSort sort : spec.sorts()) if (relatedField(spec.form(), sort.field())) return true;
        if (page != null) for (PageSort sort : page.sorts()) if (relatedField(spec.form(), sort.field())) return true;
        if (!spec.expressions().isEmpty()) {
            boolean[] related = { false };
            for (ValueExpression expression : spec.expressions().values()) {
                expression.forEachField(field -> related[0] |= relatedField(spec.form(), field.identity().name()));
                if (related[0]) return true;
            }
        }
        for (ConditionNode node : ConditionNodes.preorder(spec.where())) {
            if (node instanceof TermCondition term && relatedField(spec.form(), term.field())) return true;
        }
        if (spec.structuredInput().isPresent()) {
            for (StructuredConditionInput input : ConditionNodes.preorder(spec.structuredInput().orElseThrow())) {
                if (input.field() != null && relatedField(spec.form(), input.field())) return true;
            }
        }
        return false;
    }

    private static boolean relatedField(DynamicForm form, String name) {
        int separator = name.indexOf('.');
        return separator > 0 && form.findField(name).isEmpty()
                && form.relations().containsKey(name.substring(0, separator).toLowerCase(java.util.Locale.ROOT));
    }

    PlannedQuery plan(JoinQuerySpec spec, SqlExecutionOptions options) {
        return plan(spec, options, null, false, false, null);
    }

    PlannedQuery plan(JoinQuerySpec spec, SqlExecutionOptions options, PageQuery page, boolean first, boolean total, ReadLock lock) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PreparedJoin prepared = prepare(safeSpec);
        return planned(safeSpec, prepared, options, page, first, total, lock);
    }

    private PlannedQuery planned(JoinQuerySpec spec, PreparedJoin prepared, SqlExecutionOptions options,
                                  PageQuery page, boolean first, boolean total, ReadLock lock) {
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        SqlFragment relation = joins.relation(spec, prepared.sources().physicalForms(), prepared.sources().protections(),
                prepared.sources().valueConditions(), renderer.lockingReadDialect(), lock);
        PageQuery window = first ? PageQuery.of(1, 1) : page == null ? null : PageQuery.of(page.page(), page.size());
        SqlRequest request = joins.select(spec, relation, prepared.columns(), window,
                renderer.lockingReadDialect(), lock, prepared.sources().physicalForms().get(spec.root()));
        return planned(spec, prepared, request, total ? joins.count(spec, relation) : null, page, options);
    }

    GovernedPlanEnvelope<PlannedQuery> planGoverned(JoinQuerySpec spec,
                                                    SqlExecutionOptions options,
                                                    FieldUsePolicy policy,
                                                    QueryShapeLimits limits) {
        return planGoverned(spec, options, policy, limits, null, false, false, null);
    }

    GovernedPlanEnvelope<PlannedQuery> planGoverned(JoinQuerySpec spec, SqlExecutionOptions options,
            FieldUsePolicy policy, QueryShapeLimits limits, PageQuery page, boolean first, boolean total, ReadLock lock) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        PreparedJoin prepared = prepare(safeSpec, requirements, budget,
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        FieldUseSnapshot fieldUse = approveFieldUse(
                requirements.build(), prepared.sources().scopes(), policy);
        PlannedQuery plan = planned(safeSpec, prepared, options, page, first, total, lock);
        FieldUseGuard.accountRenderedRequest(budget, plan.request());
        if (plan.countRequest() != null) FieldUseGuard.accountRenderedRequest(new QueryShapeBudget(limits), plan.countRequest());
        return new GovernedPlanEnvelope<>(plan, fieldUse);
    }

    FormReadPlanner.PlannedCount count(JoinQuerySpec spec, SqlExecutionOptions options,
                                       FieldUsePolicy policy, QueryShapeLimits limits, boolean governed) {
        FieldUseRequirements.Builder requirements = governed ? FieldUseRequirements.builder() : null;
        QueryShapeBudget budget = governed ? new QueryShapeBudget(limits) : null;
        if (governed) budget.addProjections(1);
        PreparedSources sources = prepareSources(spec, requirements, budget, false, governed);
        if (governed) approveFieldUse(requirements.build(), sources.scopes(), policy);
        SqlRequest request = renderer.joinQueries().count(spec, renderer.joinQueries().relation(spec,
                sources.physicalForms(), sources.protections(), sources.valueConditions()));
        if (governed) FieldUseGuard.accountRenderedRequest(budget, request);
        return new FormReadPlanner.PlannedCount(spec.root().form(), request,
                options == null ? defaultOptions : options, DataScope.none(), null);
    }

    PlannedQuery page(JoinQuerySpec spec, PageQuery page, SqlExecutionOptions options) {
        return plan(spec, options, JoinQuerySqlRenderer.requireJoinPage(spec, page), false, true, null);
    }

    GovernedPlanEnvelope<PlannedQuery> pageGoverned(JoinQuerySpec spec,
                                                        PageQuery page,
                                                        SqlExecutionOptions options,
                                                        FieldUsePolicy policy,
                                                        QueryShapeLimits limits) {
        return planGoverned(spec, options, policy, limits,
                JoinQuerySqlRenderer.requireJoinPage(spec, page), false, true, null);
    }

    private PlannedQuery planned(JoinQuerySpec spec, PreparedJoin prepared, SqlRequest request,
                                 SqlRequest count, PageQuery page, SqlExecutionOptions options) {
        // 来源 Scope/显示策略已经绑定在关联结果计划中，不再对别名表单重复应用。
        return new PlannedQuery(prepared.resultForm(), request, options == null ? defaultOptions : options,
                DataScope.none(), SensitiveDisplayMode.FULL, null, List.of(), prepared.decodingPlan(),
                count, page, spec, prepared.resultPlan());
    }

    GovernedPlanEnvelope<FormReadPlanner.PlannedKeysetPage> keyset(
            JoinQuerySpec spec, KeysetPageQuery page, SqlExecutionOptions options, ReadLock lock,
            FieldUsePolicy policy, QueryShapeLimits limits) {
        FieldUseRequirements.Builder requirements = policy == null ? null : FieldUseRequirements.builder();
        QueryShapeBudget budget = policy == null ? null : new QueryShapeBudget(limits);
        SensitiveDisplayMode display = policy == null ? spec.sensitiveDisplayMode()
                : FieldUseGuard.effectiveDisplayMode(policy, spec.sensitiveDisplayMode());
        PreparedSources sources = prepareSources(spec, requirements, budget, false, policy != null);
        JoinReadGuard.validateOutput(spec, sources.readableForms(), requirements, null);
        Map<JoinFieldRef, String> aliases = new LinkedHashMap<>();
        Set<String> usedAliases = new HashSet<>();
        for (JoinProjection projection : spec.projections()) {
            String alias = FieldIdentity.of(projection.alias()).key();
            usedAliases.add(alias);
            if (projection.field() != null) aliases.putIfAbsent(projection.field(), alias);
        }
        Function<JoinFieldRef, String> alias = field -> aliases.computeIfAbsent(field, ignored -> {
            String candidate;
            int index = aliases.size();
            do { candidate = "__fo_ks_" + index++; } while (!usedAliases.add(candidate));
            return candidate;
        });
        List<KeysetSort> sorts = new ArrayList<>(page.sorts().size());
        for (int index = 0; index < page.sorts().size(); index++) {
            JoinFieldRef field = spec.orders().get(index).field();
            if (field == null) throw new IllegalArgumentException("keyset ordering requires source fields");
            KeysetSort sort = page.sorts().get(index);
            sorts.add(new KeysetSort(alias.apply(field), sort.direction(), sort.nullOrder()));
        }
        // 根键不能区分一对多行；包括中间关系在内的来源键共同标识最终结果行。
        List<String> stableKey = new ArrayList<>();
        for (JoinSource source : spec.sources()) {
            List<String> key = KeysetPageNormalizer.dynamicStableKey(source.form());
            if (key.isEmpty()) throw new IllegalArgumentException("keyset pagination requires a unique key for every join source");
            for (String field : key) stableKey.add(alias.apply(source.field(field)));
        }
        Map<String, JoinFieldRef> fields = new LinkedHashMap<>();
        aliases.forEach((field, name) -> fields.put(name, field));
        // 物理 NOT NULL 主键也会在外连接结果中为 NULL。
        Set<JoinSource> nullable = new HashSet<>();
        List<JoinSource> preceding = new ArrayList<>();
        preceding.add(spec.root());
        for (JoinClause join : spec.joins()) {
            if (join.type() == JoinType.RIGHT || join.type() == JoinType.FULL) nullable.addAll(preceding);
            if (join.type() == JoinType.LEFT || join.type() == JoinType.FULL) nullable.add(join.source());
            preceding.add(join.source());
        }
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(
                new KeysetPageQuery(page.size(), sorts, page.position()), Function.identity(), name -> {
                    JoinFieldRef field = fields.get(name);
                    return nullable.contains(field.source()) || field.source().form().field(field.field()).nullable();
                }, stableKey);
        for (int index = 0; index < normalized.sorts().size(); index++) {
            JoinFieldRef field = fields.get(normalized.sorts().get(index).field());
            FormQueryShapeGuard.requireKeysetField(field.source().form(), sources.readableForms().get(field.source()),
                    field.field(), display);
            if (requirements != null) {
                requirements.requireJoin(field, FieldUse.PROJECT);
                requirements.requireJoin(field, FieldUse.SORT, index < normalized.callerSortCount()
                        ? FieldUseOrigin.CALLER : FieldUseOrigin.INTERNAL_TIE_BREAKER);
            }
        }
        FieldUseSnapshot fieldUse = policy == null ? FieldUseSnapshot.unrestricted()
                : approveFieldUse(requirements.build(), sources.scopes(), policy);
        for (KeysetSort sort : normalized.sorts()) {
            JoinFieldRef field = fields.get(sort.field());
            if (fieldUse.joinVisibility(field) != FieldVisibility.FULL) {
                throw new ScopeAccessException(ScopeErrorCode.FIELD_NOT_READABLE, field.source().form().id(),
                        field.field(), "cursor field requires FULL visibility");
            }
        }
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(
                spec.projections().stream().map(JoinProjection::alias).toList(), normalized, true);
        List<JoinProjection> projections = new ArrayList<>(spec.projections());
        for (HiddenProjectionLayout.Projection selection : layout.selections()) {
            if (selection.hidden()) projections.add(new JoinProjection(fields.get(selection.field()), selection.label()));
        }
        if (budget != null) {
            budget.addProjections(projections.size());
            budget.addSorts(normalized.sorts().size());
        }
        FormQueryExpressions.Projection projection = FormQueryExpressions.prepareJoin(
                renderer, projections, sources.physicalForms(), display);
        SqlRequest request = renderer.joinQueries().selectKeyset(spec, sources, projection.columns(), normalized,
                fields, renderer.lockingReadDialect(), lock);
        if (budget != null) FieldUseGuard.accountRenderedRequest(budget, request);
        return new GovernedPlanEnvelope<>(new FormReadPlanner.PlannedKeysetPage(projection.resultForm(), request,
                normalized, layout, options == null ? defaultOptions : options, DataScope.none(), SensitiveDisplayMode.FULL,
                List.of(), FormFieldDecodingPlan.joinProjection(projections, projection.resultForm(), renderer),
                spec, results.plan(projections, sources.scopes(), display), null), fieldUse);
    }

    /** governed JOIN 在 SQL 生成前按来源自己的 FieldScope 完成一次审批。 */
    static FieldUseSnapshot approveFieldUse(
            FieldUseRequirements requirements,
            Map<JoinSource, DataScope> scopes,
            FieldUsePolicy policy) {
        Map<JoinSource, DataScope> safeScopes = Objects.requireNonNull(
                scopes, "join scopes must not be null");
        Map<JoinSource, FieldScope> fieldScopes = new LinkedHashMap<>(safeScopes.size());
        safeScopes.forEach((source, scope) -> fieldScopes.put(
                Objects.requireNonNull(source, "join source must not be null"),
                Objects.requireNonNull(scope, "join data scope must not be null").fields()));
        FieldUseSnapshot snapshot = Objects.requireNonNull(
                policy, "field use policy must not be null")
                .approveJoin(requirements, fieldScopes);
        for (JoinFieldDecision decision : snapshot.joinDecisions()) {
            if (decision.denied()) {
                rejectFieldUse(decision);
            }
        }
        return snapshot;
    }

    private static void rejectFieldUse(JoinFieldDecision decision) {
        JoinSource source = decision.field().source();
        String description = "source[" + source.ordinal() + ':' + source.form().id()
                + "]." + decision.field().field();
        throw new ScopeAccessException(
                decision.use().write()
                        ? ScopeErrorCode.FIELD_NOT_WRITABLE : ScopeErrorCode.FIELD_NOT_READABLE,
                source.form().id(),
                decision.field().field(),
                "join field [" + description + "] is not allowed for "
                        + decision.use() + " from " + decision.origin());
    }

    private PreparedJoin prepare(JoinQuerySpec safeSpec) {
        return prepare(safeSpec, null, null, safeSpec.sensitiveDisplayMode());
    }

    private PreparedJoin prepare(JoinQuerySpec safeSpec,
                                 FieldUseRequirements.Builder requirements,
                                 QueryShapeBudget budget,
                                 SensitiveDisplayMode displayMode) {
        PreparedSources sources = prepareSources(safeSpec, requirements, budget, true, requirements != null);
        JoinResultProtector.ResultPlan resultPlan = results.plan(safeSpec.projections(), sources.scopes(), displayMode);
        FormQueryExpressions.Projection projection = FormQueryExpressions.prepareJoin(
                renderer, safeSpec.projections(), sources.physicalForms(), displayMode);
        return new PreparedJoin(projection.resultForm(), sources, resultPlan,
                FormFieldDecodingPlan.joinProjection(safeSpec.projections(), projection.resultForm(), renderer), projection.columns());
    }

    /** 准备可供普通行和报表共同消费的来源事实，不创建投影解码器或结果表单。 */
    PreparedSources prepareSources(JoinQuerySpec safeSpec,
                                    FieldUseRequirements.Builder requirements,
                                    QueryShapeBudget budget) {
        return prepareSources(safeSpec, requirements, budget, false, requirements != null);
    }

    PreparedSources prepareAggregateSources(JoinQuerySpec spec,
                                             FieldUseRequirements.Builder requirements,
                                             QueryShapeBudget budget,
                                             boolean governed) {
        return prepareSources(spec, requirements, budget, false, governed);
    }

    private PreparedSources prepareSources(JoinQuerySpec safeSpec,
                                           FieldUseRequirements.Builder requirements,
                                           QueryShapeBudget budget,
                                           boolean includeOutput,
                                           boolean governed) {
        Map<JoinSource, ConditionGroup> protections = new LinkedHashMap<>();
        List<ScopedRead> scopedReads = new ArrayList<>(safeSpec.sources().size());
        Map<JoinSource, DynamicForm> readableForms = new LinkedHashMap<>();
        Map<JoinSource, DynamicForm> physicalForms = new LinkedHashMap<>();
        Map<JoinSource, DataScope> effectiveScopes = new LinkedHashMap<>();
        for (JoinSource source : safeSpec.sources()) {
            ScopedRead read = scopes.scopedRead(source.form(), EMPTY, safeSpec.scope(source));
            scopedReads.add(read);
            readableForms.put(source, read.form());
            effectiveScopes.put(source, read.scope());
        }
        if (includeOutput) {
            JoinReadGuard.validateOutput(safeSpec, readableForms, requirements, budget);
        }
        JoinReadGuard.validateRelations(safeSpec, readableForms, requirements, budget,
                governed ? renderer : null);
        for (int index = 0; index < safeSpec.sources().size(); index++) {
            JoinSource source = safeSpec.sources().get(index);
            ScopedRead read = scopedReads.get(index);
            DynamicForm physicalForm = renderer.protection().physicalForm(source.form());
            ProtectedFieldRuntime.PreparedQuery protection = renderer.protection().prepareQuery(
                    source.form(), physicalForm, read.where(), read.scope(), List.of());
            protections.put(source, protection.where());
            physicalForms.put(source, physicalForm);
        }
        Map<JoinCondition.Values, ConditionGroup> valueConditions = Map.of();
        for (JoinClause join : safeSpec.joins()) {
            valueConditions = prepareValues(join.condition(), physicalForms, effectiveScopes, valueConditions);
        }
        valueConditions = prepareValues(safeSpec.where(), physicalForms, effectiveScopes, valueConditions);
        return new PreparedSources(readableForms, physicalForms, protections, effectiveScopes, valueConditions);
    }

    private Map<JoinCondition.Values, ConditionGroup> prepareValues(
            JoinCondition condition, Map<JoinSource, DynamicForm> physicalForms,
            Map<JoinSource, DataScope> effectiveScopes, Map<JoinCondition.Values, ConditionGroup> existing) {
        if (condition == null || !condition.anyLeaf(leaf -> leaf instanceof JoinCondition.Values)) return existing;
        Map<JoinCondition.Values, ConditionGroup> prepared = existing.isEmpty() ? new IdentityHashMap<>() : existing;
        condition.forEachLeaf(leaf -> {
            if (leaf instanceof JoinCondition.Values values) {
                prepared.computeIfAbsent(values, key -> renderer.protection().prepareQuery(
                        key.source().form(), physicalForms.get(key.source()), key.condition(),
                        effectiveScopes.get(key.source()), List.of()).where());
            }
        });
        return prepared;
    }

    /** 容器只由本次规划创建并持有；来源身份不会降格为结果别名。 */
    record PreparedSources(
            Map<JoinSource, DynamicForm> readableForms,
            Map<JoinSource, DynamicForm> physicalForms,
            Map<JoinSource, ConditionGroup> protections,
            Map<JoinSource, DataScope> scopes,
            Map<JoinCondition.Values, ConditionGroup> valueConditions) {
    }

    private record PreparedJoin(
            DynamicForm resultForm,
            PreparedSources sources,
            JoinResultProtector.ResultPlan resultPlan,
            FormFieldDecodingPlan decodingPlan,
            SqlFragment columns) {
    }
}
