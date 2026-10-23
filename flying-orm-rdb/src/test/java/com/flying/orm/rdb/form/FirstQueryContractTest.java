package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FirstQueryContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .addField(DynamicField.of("tenant", "BIGINT")).build();

    @Test
    void reactiveFirstWaitsForExecutionCompletionAndPropagatesTerminalFailures() {
        IllegalStateException failure = new IllegalStateException("result cleanup failed");
        ReactiveFormClient client = ReactiveFormClient.create(new com.flying.orm.rdb.reactive.ReactiveSqlExecutor() {
            public reactor.core.publisher.Flux<com.flying.orm.rdb.result.DynamicRow> query(SqlRequest request) {
                return reactor.core.publisher.Flux.concat(
                        reactor.core.publisher.Mono.just(com.flying.orm.rdb.result.DynamicRow.copyOf(java.util.Map.of("name", "Ada"))),
                        reactor.core.publisher.Mono.error(failure));
            }
            public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
        }, FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().build()).withProjection(List.of("name"), List.of());
        assertSame(failure, assertThrows(IllegalStateException.class, () -> client.first(spec).block()));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> client.withFieldUsePolicy(FieldUsePolicy.unrestricted()).first(spec).block()));
    }

    @Test
    void firstUsesNativeSqlWithAndWithoutOrderingAcrossFourDialects() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            for (boolean ordered : List.of(false, true)) {
                QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().where("name", "=", "Ada").build())
                        .withProjection(List.of("name"), List.of("name"));
                if (ordered) { spec = spec.withSorts(List.of(PageSort.asc("name"))); }
                SqlRequest plain = planner.reads.select(spec).request();
                SqlRequest first = planner.reads.first(spec).request();
                switch (dialect.name()) {
                    case "sqlserver" -> {
                        assertEquals("select top (?)" + plain.sql().substring(6), first.sql());
                        assertEquals(List.of(1, "Ada", 7L), first.parameters());
                    }
                    case "oracle" -> {
                        assertEquals(plain.sql() + " offset ? rows fetch next ? rows only", first.sql());
                        assertEquals(List.of("Ada", 7L, 0L, 1), first.parameters());
                    }
                    default -> {
                        assertEquals(plain.sql() + " limit ? offset ?", first.sql());
                        assertEquals(List.of("Ada", 7L, 1, 0L), first.parameters());
                    }
                }
                assertTrue(first.sql().contains("group by"));
                assertEquals(List.of("name"), planner.reads.first(spec).outputFields());
            }
        }
    }

    @Test
    void firstApprovesTheSameFieldsAndChargesTheFinalSqlParameters() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().where("name", "=", "Ada").build())
                .withProjection(List.of("name"), List.of());
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxBindCount(3);
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.firstGoverned(spec, FieldUsePolicy.unrestricted(), limits));
        assertEquals(List.of("Ada", 7L, 1, 0L), planner.reads.firstGoverned(spec,
                FieldUsePolicy.unrestricted(), limits.withMaxBindCount(4)).plan().request().parameters());
        FieldUsePolicy policy = FieldUsePolicy.builder()
                .allow("name", FieldUse.FILTER).allow("name", FieldUse.PROJECT)
                .visibility("name", FieldVisibility.FULL).build();
        assertThrows(ScopeAccessException.class, () -> planner.reads.firstGoverned(
                spec.withSorts(List.of(PageSort.asc("name"))), policy, QueryShapeLimits.defaults()));
    }

    private static FormOperationPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormOperationPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.tenant("tenant", 7L)), SqlExecutionOptions.unlimited());
    }
}
