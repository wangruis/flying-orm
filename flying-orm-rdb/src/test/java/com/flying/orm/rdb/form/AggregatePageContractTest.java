package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregatePageContractTest {
    private static final AggregateExpression<BigDecimal> GROSS = AggregateExpression.sum("amount", "gross");

    @Test
    void plansCountOfFinalGroupsAndNativeDataPageAcrossDialects() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = plan(dialect, PageQuery.of(2, 7), QueryShapeLimits.defaults());
            String status = dialect.schema().identifier("status");
            String gross = dialect.schema().identifier("gross");
            String amount = dialect.schema().identifier("amount");
            String base = "select " + status + " as " + status + ", sum(" + amount + ") as " + gross
                    + " from " + dialect.schema().identifier("orders")
                    + " where " + status + " = ? group by " + status + " having sum(" + amount + ") > ?";
            SqlRequest count = countRequest(plan);
            String countFunction = dialect.name().equals("sqlserver") ? "count_big" : "count";
            assertEquals("select " + countFunction + "(*) as " + dialect.schema().identifier("total")
                    + " from (" + base + ") " + dialect.schema().identifier("flying_aggregate_count"), count.sql());
            assertEquals(List.of("paid", BigDecimal.TEN), count.parameters());
            SqlRequest expected = dialect.pagination().paginate(base + " order by " + gross + " desc",
                    count.parameters(), PageQuery.of(2, 7));
            assertEquals(expected.sql(), plan.request().sql());
            assertEquals(expected.parameters(), plan.request().parameters());
        }
    }

    @Test
    void pageSortOverridesReportSortAndNoFrameworkSizeCeilingIsAdded() {
        FormAggregatePlanner.Plan plan = plan(RdbDialect.postgresql(),
                PageQuery.of(1, Integer.MAX_VALUE, PageSort.asc("status")), QueryShapeLimits.defaults());
        assertTrue(plan.request().sql().endsWith("order by \"status\" asc limit ? offset ?"));
        assertEquals(Integer.MAX_VALUE, plan.request().parameters().get(2));
        assertThrows(IllegalArgumentException.class, () -> plan(RdbDialect.postgresql(),
                PageQuery.of(1, 10, PageSort.asc("amount")), QueryShapeLimits.defaults()));
    }

    @Test
    void finalPaginationBindingsAreCoveredByCallerBudget() {
        assertThrows(IllegalArgumentException.class, () -> plan(RdbDialect.postgresql(),
                PageQuery.of(1, 10), QueryShapeLimits.defaults().withMaxBindCount(3)));
        assertEquals(4, plan(RdbDialect.postgresql(), PageQuery.of(1, 10),
                QueryShapeLimits.defaults().withMaxBindCount(4)).request().parameters().size());
    }

    @Test
    void clientsShareTypedResultsAndReactiveExecutionIsLazyAndRepeatable() {
        Fixture fixture = new Fixture();
        Mono<PageResult<AggregateRow>> pending = reactivePage(fixture.reactive, spec(), PageQuery.of(2, 7));
        assertTrue(fixture.requests.isEmpty());
        PageResult<AggregateRow> first = pending.block();
        assertEquals(2, fixture.requests.size());
        assertEquals(3L, first.total());
        assertEquals(new BigDecimal("25.50"), first.rows().getFirst().get(GROSS));
        assertEquals(2, first.page());
        assertEquals(7, first.size());
        pending.block();
        assertEquals(4, fixture.requests.size());
        PageResult<AggregateRow> sync = syncPage(fixture.sync, spec(), PageQuery.of(2, 7));
        assertEquals(first.rows().getFirst().values(), sync.rows().getFirst().values());
        assertEquals(first.total(), sync.total());
        assertEquals(fixture.requests.get(0).sql(), fixture.requests.get(4).sql());
        assertEquals(fixture.requests.get(1).sql(), fixture.requests.get(5).sql());
    }

    @Test
    void zeroTotalSkipsDataQueryInBothModes() {
        Fixture fixture = new Fixture();
        fixture.total = 0;
        assertTrue(syncPage(fixture.sync, spec(), PageQuery.of(1, 10)).rows().isEmpty());
        assertTrue(reactivePage(fixture.reactive, spec(), PageQuery.of(1, 10)).block().rows().isEmpty());
        assertEquals(2, fixture.requests.size());
        assertTrue(fixture.requests.stream().allMatch(request -> request.sql().startsWith("select count(")));
    }

    @Test
    void countErrorsAndCancellationNeverStartTheDataQuery() {
        Fixture fixture = new Fixture();
        fixture.countFailure = new IllegalStateException("count failed");
        assertThrows(IllegalStateException.class, () -> fixture.sync.aggregatePage(spec(), PageQuery.of(1, 10)));
        assertEquals("count failed", assertThrows(IllegalStateException.class,
                () -> fixture.reactive.aggregatePage(spec(), PageQuery.of(1, 10)).block()).getMessage());
        assertEquals(2, fixture.requests.size());
        fixture.countFailure = null;
        fixture.waitForCount = true;
        reactor.core.Disposable pending = fixture.reactive.aggregatePage(spec(), PageQuery.of(1, 10)).subscribe();
        pending.dispose();
        assertTrue(fixture.cancelled);
        assertEquals(3, fixture.requests.size());
        assertTrue(fixture.requests.stream().allMatch(request -> request.sql().startsWith("select count(")));
    }

    @Test
    void pageKeepsFieldApprovalAndMasksBeforePublishing() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .masked("status", MaskedFieldDefinition.builder("full").build()).build();
        AggregateSpec report = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .group(GroupSelection.of("status", "status")).aggregate(GROSS).build();
        FieldUsePolicy policy = FieldUsePolicy.builder().allow("status", FieldUse.GROUP)
                .visibility("status", FieldVisibility.MASKED).allow("amount", FieldUse.AGGREGATE)
                .visibility("amount", FieldVisibility.FULL).build();
        Fixture fixture = new Fixture();
        AggregateRow sync = fixture.sync.withFieldUsePolicy(policy).aggregatePage(report, PageQuery.of(1, 10))
                .rows().getFirst();
        AggregateRow reactive = fixture.reactive.withFieldUsePolicy(policy).aggregatePage(report, PageQuery.of(1, 10))
                .block().rows().getFirst();
        assertEquals("****", sync.get(GroupSelection.of("status", "status"), String.class));
        assertEquals(sync.values(), reactive.values());
        fixture.requests.clear();
        FieldUsePolicy deny = FieldUsePolicy.builder().allow("status", FieldUse.GROUP).build();
        assertThrows(ScopeAccessException.class,
                () -> fixture.sync.withFieldUsePolicy(deny).aggregatePage(report, PageQuery.of(1, 10)));
        assertThrows(ScopeAccessException.class,
                () -> fixture.reactive.withFieldUsePolicy(deny).aggregatePage(report, PageQuery.of(1, 10)).block());
        assertTrue(fixture.requests.isEmpty());
    }

    @Test
    void countWrapperIsSubjectToSqlBudgetAndUnpagedPlanDoesNotAllocateCount() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormAggregatePlanner.Plan plan = plan(dialect, PageQuery.of(1, 10), QueryShapeLimits.defaults());
        assertTrue(countRequest(plan).sql().length() > plan.request().sql().length());
        assertThrows(IllegalArgumentException.class, () -> plan(dialect, PageQuery.of(1, 10),
                QueryShapeLimits.defaults().withMaxSqlLength(countRequest(plan).sql().length() - 1)));
        assertTrue(planner(dialect, QueryShapeLimits.defaults()).plan(spec()).countRequest().isEmpty());
    }

    private static AggregateSpec spec() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL")).build();
        return AggregateSpec.builder(QuerySpec.of(form,
                        ConditionGroup.and().where("status", "=", "paid").build())
                        .withSorts(List.of(PageSort.desc("gross"))))
                .group(GroupSelection.of("status", "status")).aggregate(GROSS)
                .having(ConditionGroup.and().where("gross", ">", BigDecimal.TEN).build())
                .build();
    }

    private static FormAggregatePlanner.Plan plan(RdbDialect dialect, PageQuery page, QueryShapeLimits limits) {
        return planner(dialect, limits).planPage(spec(), page, false);
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, QueryShapeLimits limits) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                limits);
    }

    private static SqlRequest countRequest(FormAggregatePlanner.Plan plan) {
        return plan.countRequest().orElseThrow();
    }

    private static Mono<PageResult<AggregateRow>> reactivePage(ReactiveFormClient client, AggregateSpec spec, PageQuery page) {
        return client.aggregatePage(spec, page);
    }

    private static PageResult<AggregateRow> syncPage(SyncFormClient client, AggregateSpec spec, PageQuery page) {
        return client.aggregatePage(spec, page);
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private long total = 3;
        private RuntimeException countFailure;
        private boolean waitForCount;
        private boolean cancelled;
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        private final ReactiveFormClient reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> {
                    if (waitForCount) {
                        requests.add(request);
                        return Flux.<DynamicRow>never().doOnCancel(() -> cancelled = true);
                    }
                    return Flux.fromIterable(rows(request));
                });
            }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
        }, renderer);
        private final SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return rows(request); }
            public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new UnsupportedOperationException();
            }
        }, (request, completed) -> { throw new UnsupportedOperationException(); }, renderer);

        private List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            if (countFailure != null) throw countFailure;
            if (request.sql().startsWith("select count(")) return List.of(DynamicRow.copyOf(Map.of("total", total)));
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("status", "paid");
            values.put("gross", new BigDecimal("25.50"));
            return List.of(DynamicRow.copyOf(values));
        }
    }
}
