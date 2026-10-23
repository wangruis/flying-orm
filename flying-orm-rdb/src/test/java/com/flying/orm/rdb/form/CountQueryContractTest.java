package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CountQueryContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .addField(DynamicField.of("tenant", "BIGINT")).build();

    @Test
    void countUsesOneScalarSqlAndCountsGroupsWithoutLosingNullGroupsAcrossDialects() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().where("name", "=", "Ada").build())
                    .withProjection(List.of("unused_projection"), List.of())
                    .withSorts(List.of(PageSort.asc("unused_sort")));
            SqlRequest plain = count(planner, spec);
            String count = dialect.name().equals("sqlserver") ? "count_big" : "count";
            assertTrue(plain.sql().startsWith("select " + count + "(*) as total from "), plain.sql());
            assertEquals(List.of("Ada", 7L), plain.parameters());
            assertTrue(!plain.sql().contains("unused"), plain.sql());
            String relation = plain.sql().substring(("select " + count + "(*) as total from ").length());
            QuerySpec groupedSpec = spec.withProjection(List.of("name"), List.of("name"));
            SqlRequest grouped = count(planner, groupedSpec);
            String name = switch (dialect.name()) {
                case "mysql" -> "`name`";
                case "sqlserver" -> "[name]";
                default -> "\"name\"";
            };
            assertEquals("select " + count + "(*) as total from (select 1 as group_marker from "
                    + relation + " group by " + name + ") flying_count", grouped.sql());
            assertEquals(plain.parameters(), grouped.parameters());
            // Alternating grouped and ordinary plans must not reuse the wrong cached SQL shape.
            assertEquals(plain.sql(), count(planner, spec).sql());
            assertEquals(grouped.sql(), count(planner, groupedSpec).sql());
        }
    }

    @Test
    void countPreservesStructuredConditionsTrustedScopesAndLogicalDelete() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("deleted", "INTEGER")).logicDelete("deleted").build();
        QuerySpec spec = QuerySpec.structured(form, StructuredConditionInput.term("name", "=", "Ada"))
                .withScope(DataScope.where(ConditionGroup.and().where("id", ">", 10L).build()));
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        SqlRequest actual = count(planner, spec);
        SqlRequest selected = planner.reads.select(spec).request();
        assertEquals(selected.sql().substring(selected.sql().indexOf(" from ")),
                actual.sql().substring(actual.sql().indexOf(" from ")));
        assertEquals(selected.parameters(), actual.parameters());
        assertTrue(actual.parameters().containsAll(List.of("Ada", 10L, 7L, 0)));
    }

    @Test
    void countApprovesFilterAndGroupWithoutRequiringProjectionOrSortAndChargesFinalSql() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().where("name", "=", "Ada").build())
                .withProjection(List.of("id"), List.of())
                .withSorts(List.of(PageSort.asc("id")));
        FieldUsePolicy filterOnly = FieldUsePolicy.builder().allow("name", FieldUse.FILTER).build();
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxProjectionCount(1).withMaxBindCount(2);
        SqlRequest allowed = planner.reads.count(spec, filterOnly, limits, true).request();
        assertEquals(List.of("Ada", 7L), allowed.parameters());
        assertThrows(ScopeAccessException.class, () -> planner.reads.count(spec,
                FieldUsePolicy.builder().allow("name", FieldUse.PROJECT).build(), limits, true));
        QuerySpec grouped = spec.withProjection(List.of("name"), List.of("name"));
        assertThrows(ScopeAccessException.class, () -> planner.reads.count(grouped, filterOnly, limits, true));
        FieldUsePolicy filterAndGroup = FieldUsePolicy.builder()
                .allow("name", FieldUse.FILTER).allow("name", FieldUse.GROUP).build();
        assertTrue(planner.reads.count(grouped, filterAndGroup, limits, true).request().sql().contains("group by"));
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.count(spec, filterOnly, limits.withMaxBindCount(1), true));
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.count(spec, filterOnly, limits.withMaxSqlLength(allowed.sql().length() - 1), true));
    }

    @Test
    void pageCountMustCompleteBeforeStartingDataQuery() {
        IllegalStateException failure = new IllegalStateException("count cleanup failed");
        AtomicInteger queries = new AtomicInteger();
        ReactiveFormClient client = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> {
                    queries.incrementAndGet();
                    return request.sql().startsWith("select count(*)")
                            ? Flux.concat(Mono.just(DynamicRow.copyOf(Map.of("total", 1L))), Mono.error(failure))
                            : Flux.empty();
                });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
        }, FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> client.page(QuerySpec.of(FORM, ConditionGroup.and().build()), PageQuery.of(1, 10)).block()));
        assertEquals(1, queries.get());
        JoinQuerySpec.Builder relation = JoinQuerySpec.builder(FORM);
        JoinQuerySpec joined = relation.select(relation.root(), "id")
                .orderBy(relation.root(), "id", PageSort.Direction.ASC).build();
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> client.pageJoin(joined, PageQuery.of(1, 10)).block()));
        assertEquals(2, queries.get());
    }

    private static SqlRequest count(FormOperationPlanner planner, QuerySpec spec) {
        return planner.reads.count(spec, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults(), false).request();
    }

    private static FormOperationPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormOperationPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.tenant("tenant", 7L)), SqlExecutionOptions.unlimited());
    }
}
