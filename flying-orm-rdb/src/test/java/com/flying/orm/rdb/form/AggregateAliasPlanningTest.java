package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateAliasPlanningTest {
    @Test
    void ordinaryReportRetainsDeclaredAliasesAndBoundParameters() {
        FormAggregatePlanner.Plan plan = planner().plan(report().build());
        assertEquals("select \"status\" as \"State\", count(\"id\") as \"row_count\", "
                + "count(\"score\") as \"nonnull_count\", sum(\"score\") as \"Total\" "
                + "from \"bench_rows\" where \"id\" >= ? and \"id\" <= ? "
                + "group by \"status\" order by \"State\" asc", plan.request().sql());
        assertEquals(List.of(1L, 1000L), plan.request().parameters());
    }

    @Test
    void sortsResolveDeclaredAliasSpellingWithoutHaving() {
        String sql = planner().plan(report().orderBy(PageSort.desc("TOTAL"), PageSort.asc("STATE")).build())
                .request().sql();
        assertTrue(sql.endsWith("group by \"status\" order by \"State\" asc, \"Total\" desc, \"State\" asc"), sql);
    }

    @Test
    void havingRetainsAliasNormalizationAndParameterOrder() {
        FormAggregatePlanner.Plan plan = planner().plan(report().having(
                ConditionGroup.and().where("TOTAL", ">", new BigDecimal("5.00")).build()).build());
        assertTrue(plan.request().sql().endsWith("group by \"status\" having sum(\"score\") > ? order by \"State\" asc"),
                plan.request().sql());
        assertEquals(List.of(1L, 1000L, new BigDecimal("5.00")), plan.request().parameters());
    }

    @Test
    void unknownSortStillFailsAtTheDeclaredAliasBoundary() {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> planner().plan(report().orderBy(PageSort.asc("missing")).build()));
        assertEquals("aggregate ordering may reference only declared result aliases", failure.getMessage());
    }

    @Test
    void unsortedReportKeepsAllResultColumnsWithoutAliasTargets() {
        FormAggregatePlanner.Plan plan = planner().plan(unsortedReport().build());
        assertEquals("select \"status\" as \"State\", count(\"id\") as \"row_count\", "
                + "count(\"score\") as \"nonnull_count\", sum(\"score\") as \"Total\" "
                + "from \"bench_rows\" where \"id\" >= ? and \"id\" <= ? group by \"status\"", plan.request().sql());
        assertEquals(4, plan.layout().size());
        assertEquals(List.of(1L, 1000L), plan.request().parameters());
    }

    @Test
    void pageSortCreatesItsOwnTargetWithoutHaving() {
        FormAggregatePlanner.Plan plan = planner().planPage(report().build(),
                PageQuery.of(1, 10, PageSort.desc("TOTAL")), false);
        assertTrue(plan.request().sql().endsWith("group by \"status\" order by \"Total\" desc limit ? offset ?"),
                plan.request().sql());
        assertEquals(List.of(1L, 1000L), plan.countRequest().orElseThrow().parameters());
        assertThrows(IllegalArgumentException.class, () -> planner().planPage(report().build(),
                PageQuery.of(1, 10, PageSort.asc("missing")), false));
    }

    @Test
    void immutableSpecOwnsLayoutAcrossOrdinaryAndPagedExecution() {
        AggregateSpec.Builder builder = report();
        AggregateSpec spec = builder.build();
        FormAggregatePlanner planner = planner();
        FormAggregatePlanner.Plan first = planner.plan(spec);
        assertSame(first.layout(), planner.plan(spec).layout());
        assertSame(first.layout(), planner.planPage(spec, PageQuery.of(1, 10), false).layout());
        AggregateSpec extended = builder.countRows("all_rows").build();
        FormAggregatePlanner.Plan next = planner.plan(extended);
        assertNotSame(first.layout(), next.layout());
        assertEquals(4, first.layout().size());
        assertEquals(5, next.layout().size());
    }

    private static AggregateSpec.Builder report() {
        return unsortedReport().orderBy(PageSort.asc("STATE"));
    }

    private static AggregateSpec.Builder unsortedReport() {
        DynamicForm form = DynamicForm.builder("bench_rows", "bench_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("status", "INTEGER"))
                .addField(DynamicField.of("score", "DECIMAL")).build();
        return AggregateSpec.builder(QuerySpec.of(form,
                        ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 1000L).build()))
                .group(GroupSelection.of("status", "State"))
                .aggregate(AggregateExpression.count("id", "row_count"))
                .aggregate(AggregateExpression.count("score", "nonnull_count"))
                .aggregate(AggregateExpression.sum("score", "Total"));
    }

    private static FormAggregatePlanner planner() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }
}
