package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.join.JoinProjection;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.CorrelatedSqlTermHandler;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/** 复用 JOIN 来源规划与条件绑定，只负责 EXISTS 和复合关联键的组合。 */
final class RelatedQueryTermHandler implements CorrelatedSqlTermHandler {
    private final String id;
    private final FormDataSqlRenderer renderer;
    private final JoinQueryPlanner planner;
    private final Function<Object, JoinQuerySpec> query;
    private final boolean negated;

    RelatedQueryTermHandler(String id, FormDataSqlRenderer renderer,
                            Function<Object, JoinQuerySpec> query, boolean negated) {
        this.id = Names.key(id, "relation term id");
        this.renderer = renderer;
        this.query = Objects.requireNonNull(query, "relation query factory must not be null");
        this.negated = negated;
        this.planner = new JoinQueryPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
    }

    @Override
    public String id() { return id; }

    @Override
    public SqlFragment render(TermCondition term, SqlRenderContext context,
                              UnaryOperator<String> outerFields, UnaryOperator<String> outerQualifiers) {
        JoinQuerySpec spec = Objects.requireNonNull(query.apply(term.value()), "relation query must not be null");
        if (spec.projections().isEmpty()) {
            throw new IllegalArgumentException("relation query must select its correlation keys");
        }
        JoinQueryPlanner.PreparedSources sources = planner.prepareSources(spec, null, null);
        JoinReadGuard.validateOutput(spec, sources.readableForms(), null, null);
        JoinQuerySqlRenderer joins = renderer.joinQueries();
        String alias = "flying_relation";
        String qualifier = outerQualifiers.apply(term.field());
        if (alias.equalsIgnoreCase(qualifier) || joins.aliasIdentifier(alias).equalsIgnoreCase(qualifier)) {
            alias += "_nested";
        }
        alias = joins.aliasIdentifier(alias);
        StringJoiner columns = new StringJoiner(", ");
        StringJoiner correlations = new StringJoiner(" and ");
        for (JoinProjection projection : spec.projections()) {
            if (projection.field() == null || projection.field().source().form().protections()
                    .encrypted(projection.field().field()).isPresent()) {
                throw new IllegalArgumentException("relation keys must select unencrypted source fields");
            }
            String column = joins.aliasIdentifier(projection.alias());
            columns.add(joins.field(projection.field()) + " as " + column);
            String outerField = outerFields.apply(projection.alias());
            if (outerField == null) {
                throw new IllegalArgumentException("unknown outer correlation field: " + projection.alias());
            }
            correlations.add(alias + "." + column + " = " + outerField);
        }
        SqlFragment relation = joins.relation(spec, sources.physicalForms(), sources.protections(),
                sources.valueConditions());
        return new SqlFragment((negated ? "not exists" : "exists") + " (select 1 from (select "
                + columns + " " + relation.sql() + ") " + alias + " where " + correlations + ")",
                relation.parameters());
    }
}
