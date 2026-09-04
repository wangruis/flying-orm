package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseRequirements;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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

    PlannedJoin plan(JoinQuerySpec spec, SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PreparedJoin prepared = prepare(safeSpec);
        return new PlannedJoin(safeSpec, prepared.resultForm(),
                               renderer.joinQueries().select(
                                       safeSpec, prepared.physicalForms(), prepared.protections(),
                                       prepared.businessConditions()),
                               options == null ? defaultOptions : options,
                               prepared.scopes(),
                               prepared.resultPlan());
    }

    GovernedPlanEnvelope<PlannedJoin> planGoverned(JoinQuerySpec spec,
                                                    SqlExecutionOptions options,
                                                    FieldUsePolicy policy,
                                                    QueryShapeLimits limits) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        PreparedJoin prepared = prepare(safeSpec, requirements, budget,
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        SqlRequest request = renderer.joinQueries().select(
                safeSpec, prepared.physicalForms(), prepared.protections(), prepared.businessConditions());
        PlannedJoin plan = new PlannedJoin(safeSpec, prepared.resultForm(), request,
                                           options == null ? defaultOptions : options,
                                           prepared.scopes(), prepared.resultPlan());
        return new GovernedPlanEnvelope<>(
                plan,
                FieldUseGuard.approveCollected(safeSpec.root().form().id(), requirements.build(),
                                               FieldScope.unrestricted(), request, policy, budget));
    }

    PlannedJoinPage page(JoinQuerySpec spec, PageQuery page, SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        PreparedJoin prepared = prepare(safeSpec);
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        return new PlannedJoinPage(safeSpec, prepared.resultForm(),
                                   joins.count(safeSpec, prepared.physicalForms(), prepared.protections(),
                                               prepared.businessConditions()),
                                   joins.select(safeSpec, prepared.physicalForms(), prepared.protections(),
                                                prepared.businessConditions(), safePage),
                                   safePage,
                                   options == null ? defaultOptions : options,
                                   prepared.scopes(),
                                   prepared.resultPlan());
    }

    GovernedPlanEnvelope<PlannedJoinPage> pageGoverned(JoinQuerySpec spec,
                                                        PageQuery page,
                                                        SqlExecutionOptions options,
                                                        FieldUsePolicy policy,
                                                        QueryShapeLimits limits) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        FieldUseRequirements.Builder requirements = FieldUseRequirements.builder();
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        PreparedJoin prepared = prepare(safeSpec, requirements, budget,
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        SqlRequest count = joins.count(safeSpec, prepared.physicalForms(), prepared.protections(),
                                       prepared.businessConditions());
        SqlRequest data = joins.select(safeSpec, prepared.physicalForms(), prepared.protections(),
                                       prepared.businessConditions(), safePage);
        PlannedJoinPage plan = new PlannedJoinPage(safeSpec, prepared.resultForm(), count, data, safePage,
                                                   options == null ? defaultOptions : options,
                                                   prepared.scopes(), prepared.resultPlan());
        return new GovernedPlanEnvelope<>(
                plan,
                FieldUseGuard.approveCollected(safeSpec.root().form().id(), requirements.build(),
                                               FieldScope.unrestricted(), data, policy, budget));
    }

    private PreparedJoin prepare(JoinQuerySpec safeSpec) {
        return prepare(safeSpec, null, null, safeSpec.sensitiveDisplayMode());
    }

    private PreparedJoin prepare(JoinQuerySpec safeSpec,
                                 FieldUseRequirements.Builder requirements,
                                 QueryShapeBudget budget,
                                 SensitiveDisplayMode displayMode) {
        Map<JoinSource, ConditionGroup> protections = new LinkedHashMap<>();
        Map<JoinSource, ConditionGroup> businessConditions = new LinkedHashMap<>();
        Map<JoinSource, DynamicForm> readableForms = new LinkedHashMap<>();
        Map<JoinSource, DynamicForm> physicalForms = new LinkedHashMap<>();
        Map<JoinSource, com.flying.orm.core.scope.DataScope> effectiveScopes = new LinkedHashMap<>();
        for (JoinSource source : safeSpec.sources()) {
            ScopedRead read = scopes.scopedRead(source.form(), EMPTY, safeSpec.scope(source));
            readableForms.put(source, read.form());
            ProtectedFieldRuntime.PreparedQuery protection = renderer.protection().prepareQuery(
                    source.form(), read.form(), read.where(), read.scope());
            protections.put(source, protection.where());
            ProtectedFieldRuntime.PreparedQuery business = renderer.protection().prepareQuery(
                    source.form(), read.form(), safeSpec.where(source), read.scope());
            businessConditions.put(source, business.where());
            physicalForms.put(source, business.physicalForm());
            effectiveScopes.put(source, read.scope());
        }
        if (requirements == null) {
            JoinReadGuard.validate(safeSpec, readableForms);
        } else {
            JoinReadGuard.validate(safeSpec, readableForms, requirements, budget, renderer);
        }
        JoinResultProtector.ResultPlan resultPlan = results.plan(
                safeSpec, effectiveScopes, displayMode);
        return new PreparedJoin(JoinResultForms.create(safeSpec, physicalForms), physicalForms, protections,
                                businessConditions, effectiveScopes, resultPlan);
    }

    record PlannedJoin(JoinQuerySpec spec,
                       DynamicForm resultForm,
                       SqlRequest request,
                       SqlExecutionOptions options,
                       Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes,
                       JoinResultProtector.ResultPlan resultPlan) {
        PlannedJoin {
            spec = Objects.requireNonNull(spec, "join query spec must not be null");
            resultForm = Objects.requireNonNull(resultForm, "join result form must not be null");
            request = Objects.requireNonNull(request, "join SQL request must not be null");
            options = Objects.requireNonNull(options, "join execution options must not be null");
            scopes = Map.copyOf(Objects.requireNonNull(scopes, "join scopes must not be null"));
            resultPlan = Objects.requireNonNull(resultPlan, "join result plan must not be null");
        }
    }

    record PlannedJoinPage(JoinQuerySpec spec,
                           DynamicForm resultForm,
                           SqlRequest countRequest,
                           SqlRequest dataRequest,
                           PageQuery page,
                           SqlExecutionOptions options,
                           Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes,
                           JoinResultProtector.ResultPlan resultPlan) {
        PlannedJoinPage {
            spec = Objects.requireNonNull(spec, "join query spec must not be null");
            resultForm = Objects.requireNonNull(resultForm, "join result form must not be null");
            countRequest = Objects.requireNonNull(countRequest, "join count request must not be null");
            dataRequest = Objects.requireNonNull(dataRequest, "join data request must not be null");
            page = Objects.requireNonNull(page, "join page query must not be null");
            options = Objects.requireNonNull(options, "join execution options must not be null");
            scopes = Map.copyOf(Objects.requireNonNull(scopes, "join scopes must not be null"));
            resultPlan = Objects.requireNonNull(resultPlan, "join result plan must not be null");
        }
    }

    private record PreparedJoin(
            DynamicForm resultForm,
            Map<JoinSource, DynamicForm> physicalForms,
            Map<JoinSource, ConditionGroup> protections,
            Map<JoinSource, ConditionGroup> businessConditions,
            Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes,
            JoinResultProtector.ResultPlan resultPlan) {
    }
}
