package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageQuery;
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

    JoinQueryPlanner(FormDataSqlRenderer renderer,
                     FormScopeSupport scopes,
                     SqlExecutionOptions defaultOptions) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = Objects.requireNonNull(scopes, "form scope support must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "default SQL execution options must not be null");
    }

    PlannedJoin plan(JoinQuerySpec spec, SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PreparedJoin prepared = prepare(safeSpec);
        return new PlannedJoin(prepared.resultForm(),
                               renderer.joinQueries().select(
                                       safeSpec, prepared.physicalForms(), prepared.protections(),
                                       prepared.businessConditions()),
                               options == null ? defaultOptions : options,
                               prepared.scopes());
    }

    PlannedJoinPage page(JoinQuerySpec spec, PageQuery page, SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        PreparedJoin prepared = prepare(safeSpec);
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        return new PlannedJoinPage(prepared.resultForm(),
                                   joins.count(safeSpec, prepared.physicalForms(), prepared.protections(),
                                               prepared.businessConditions()),
                                   joins.select(safeSpec, prepared.physicalForms(), prepared.protections(),
                                                prepared.businessConditions(), safePage),
                                   safePage,
                                   options == null ? defaultOptions : options,
                                   prepared.scopes());
    }

    private PreparedJoin prepare(JoinQuerySpec safeSpec) {
        Map<JoinSource, ConditionGroup> protections = new LinkedHashMap<>();
        Map<JoinSource, ConditionGroup> businessConditions = new LinkedHashMap<>();
        Map<JoinSource, DynamicForm> readableForms = new LinkedHashMap<>();
        Map<JoinSource, DynamicForm> physicalForms = new LinkedHashMap<>();
        Map<JoinSource, com.flying.orm.core.scope.DataScope> effectiveScopes = new LinkedHashMap<>();
        for (JoinSource source : safeSpec.sources()) {
            ScopedRead read = scopes.scopedRead(source.form(), EMPTY, safeSpec.scope(source));
            protections.put(source, read.where());
            readableForms.put(source, read.form());
            ProtectedFieldRuntime.PreparedQuery business = renderer.protection().prepareQuery(
                    source.form(), read.form(), safeSpec.where(source), read.scope());
            businessConditions.put(source, business.where());
            physicalForms.put(source, business.physicalForm());
            effectiveScopes.put(source, read.scope());
        }
        JoinReadGuard.validate(safeSpec, readableForms);
        return new PreparedJoin(JoinResultForms.create(safeSpec), physicalForms, protections,
                                businessConditions, effectiveScopes);
    }

    record PlannedJoin(DynamicForm resultForm,
                       SqlRequest request,
                       SqlExecutionOptions options,
                       Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes) {
        PlannedJoin {
            resultForm = Objects.requireNonNull(resultForm, "join result form must not be null");
            request = Objects.requireNonNull(request, "join SQL request must not be null");
            options = Objects.requireNonNull(options, "join execution options must not be null");
            scopes = Map.copyOf(Objects.requireNonNull(scopes, "join scopes must not be null"));
        }
    }

    record PlannedJoinPage(DynamicForm resultForm,
                           SqlRequest countRequest,
                           SqlRequest dataRequest,
                           PageQuery page,
                           SqlExecutionOptions options,
                           Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes) {
        PlannedJoinPage {
            resultForm = Objects.requireNonNull(resultForm, "join result form must not be null");
            countRequest = Objects.requireNonNull(countRequest, "join count request must not be null");
            dataRequest = Objects.requireNonNull(dataRequest, "join data request must not be null");
            page = Objects.requireNonNull(page, "join page query must not be null");
            options = Objects.requireNonNull(options, "join execution options must not be null");
            scopes = Map.copyOf(Objects.requireNonNull(scopes, "join scopes must not be null"));
        }
    }

    private record PreparedJoin(
            DynamicForm resultForm,
            Map<JoinSource, DynamicForm> physicalForms,
            Map<JoinSource, ConditionGroup> protections,
            Map<JoinSource, ConditionGroup> businessConditions,
            Map<JoinSource, com.flying.orm.core.scope.DataScope> scopes) {
    }
}
