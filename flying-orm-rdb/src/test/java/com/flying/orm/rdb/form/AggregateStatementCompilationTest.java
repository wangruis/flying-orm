package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateStatementCompilationTest {
    @Test
    void scalarAggregateParametersKeepPublishedOwnership() {
        FormAggregatePlanner planner = planner(RdbDialect.postgresql());
        SqlRequest request = planner.plan(report(1L)).request();
        SqlRequest count = planner.planPage(report(2L), PageQuery.of(1, 7), false)
                .countRequest().orElseThrow();
        assertTrue(OwnedBindableValues.isPublished(request.parameters()));
        assertTrue(OwnedBindableValues.isPublished(count.parameters()));
        assertEquals(List.of(1L), request.parameters());
        assertEquals(List.of(2L), count.parameters());
    }

    @Test
    void aggregateParameterReuseKeepsMutableInputsAndPublicViewsIsolated() {
        byte[] input = {1, 2, 3};
        DynamicForm form = DynamicForm.builder("binary_events", "binary_events")
                .addField(DynamicField.of("payload", "VARBINARY")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form,
                ConditionGroup.and().where("payload", "=", input).build())).countRows("rows").build();
        FormAggregatePlanner planner = planner(RdbDialect.postgresql());
        SqlRequest request = planner.plan(spec).request();
        FormAggregatePlanner.Plan page = planner.planPage(spec, PageQuery.of(1, 7), false);
        input[0] = 9;
        for (SqlRequest current : List.of(request, page.countRequest().orElseThrow(), page.request())) {
            ((byte[]) current.parameters().getFirst())[1] = 9;
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) current.parameters().getFirst());
        }
    }

    @Test
    void compilationKeepsOwnedMutableParametersWithoutResnapshotting() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormAggregateReadSupport reads = new FormAggregateReadSupport(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()));
        byte[] bytes = {1, 2, 3};
        SqlRequest request = new SqlRequest("select ?", List.of(bytes));
        List<Object> owned = OwnedBindableValues.ownedValues(request.parameters());
        SqlRequest compiled = renderer.compileRequest(request.sql(), request.parameters());
        assertSame(owned, OwnedBindableValues.ownedValues(compiled.parameters()));
        bytes[0] = 9;
        ((byte[]) compiled.parameters().getFirst())[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) compiled.parameters().getFirst());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) request.parameters().getFirst());
    }

    @Test
    void aggregatePlanCarriesReusableDriverSqlAcrossDialects() {
        for (RdbDialect dialect : dialects()) {
            SqlRequest request = planner(dialect).plan(report(1L)).request();
            assertCompiled(request, dialect);
            assertEquals(List.of(1L), request.parameters());
        }
    }

    @Test
    void countAndFinalPaginatedSqlAreBothCompiled() {
        for (RdbDialect dialect : dialects()) {
            FormAggregatePlanner.Plan plan = planner(dialect).planPage(report(2L), PageQuery.of(2, 7), false);
            assertCompiled(plan.request(), dialect);
            assertCompiled(plan.countRequest().orElseThrow(), dialect);
            assertEquals(List.of(2L), plan.countRequest().orElseThrow().parameters());
            assertEquals(3, plan.request().parameters().size());
        }
    }

    @Test
    void customPaginationKeepsNativeMarkersAndItsParameterOrder() {
        PaginationDialect pagination = new PaginationDialect() {
            @Override
            public SqlRequest paginate(String sql, List<Object> parameters, PageQuery page) {
                return SqlRequest.nativeSql(sql.replace("?", "$3") + " limit $1 offset $2",
                        paginationParameters(parameters, page));
            }

            @Override
            public List<Object> paginationParameters(List<Object> parameters, PageQuery page) {
                return List.of(page.size(), page.offset(), parameters.getFirst());
            }
        };
        RdbDialect base = RdbDialect.postgresql();
        RdbDialect dialect = RdbDialect.of(base.name(), base.schema(), pagination, base.upsert());
        FormAggregatePlanner.Plan plan = planner(dialect).planPage(report(9L), PageQuery.of(2, 7), false);
        assertEquals(SqlBindMarkerStyle.NATIVE, plan.request().bindMarkerStyle());
        assertEquals(List.of(7, 7L, 9L), plan.request().parameters());
        assertTrue(plan.request().sql().endsWith("limit $1 offset $2"));
        assertCompiled(plan.request(), dialect);
        assertCompiled(plan.countRequest().orElseThrow(), dialect);
        assertEquals(List.of(9L), plan.countRequest().orElseThrow().parameters());
    }

    @Test
    void separateCallsKeepCurrentParametersAndExecutionCanRecompileForAnotherProduct() {
        FormAggregatePlanner planner = planner(RdbDialect.postgresql());
        SqlRequest first = planner.plan(report(1L)).request();
        SqlRequest second = planner.plan(report(9L)).request();
        assertEquals(first.sql(), second.sql());
        assertEquals(List.of(1L), first.parameters());
        assertEquals(List.of(9L), second.parameters());
        assertTrue(SqlExecutionStatements.r2dbc(first, "PostgreSQL").contains("$1"));
        assertTrue(SqlExecutionStatements.r2dbc(first, "Microsoft SQL Server").contains("@P0"));
        assertEquals(List.of(1L), first.parameters());
    }

    private static void assertCompiled(SqlRequest request, RdbDialect dialect) {
        String product = DatabaseProduct.detect(dialect.name()).name();
        assertTrue(request.statement().preparedFor(product), dialect.name());
        String transport = request.statement().transportSql(product).orElseThrow();
        assertSame(transport, SqlExecutionStatements.r2dbc(request, dialect.name()));
        assertSame(transport, SqlExecutionStatements.r2dbc(request, dialect.name()));
        assertSame(request.sql(), SqlExecutionStatements.canonical(request, dialect.name()));
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static AggregateSpec report(long lower) {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("status", "INTEGER")).build();
        return AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().where("id", ">=", lower).build()))
                .group(GroupSelection.of("status", "status"))
                .aggregate(AggregateExpression.count("id", "count"))
                .orderBy(PageSort.asc("status")).build();
    }

    private static FormAggregatePlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }
}
