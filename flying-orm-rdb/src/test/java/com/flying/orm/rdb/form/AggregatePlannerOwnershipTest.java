package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregatePlannerOwnershipTest {
    @Test
    void reactiveBatchCollaboratorsReferenceTheOwnerInsteadOfCopyingItsDependencies() throws Exception {
        Field field = ReactiveFormClient.class.getDeclaredField("operations");
        field.setAccessible(true);
        ReactiveFormOperations owner = (ReactiveFormOperations) field.get(new Fixture().reactive);
        for (Object collaborator : List.of(owner.batchInserts, owner.batchUpdates)) {
            assertSame(Object.class, collaborator.getClass().getSuperclass(),
                    "batch operations must not inherit another copy of the whole runtime");
            Field[] fields = collaborator.getClass().getDeclaredFields();
            assertEquals(1, fields.length);
            assertTrue(Modifier.isFinal(fields[0].getModifiers()));
            fields[0].setAccessible(true);
            assertSame(owner, fields[0].get(collaborator));
        }
    }

    @Test
    void syncClientOwnsPlannerAndDerivedScopeHasAnIndependentPlanner() throws Exception {
        Fixture fixture = new Fixture();
        SyncFormClient scoped = fixture.sync.withDefaultDataScope(DataScope.tenant("tenant", "blue"));
        FormAggregatePlanner original = planner(fixture.sync);
        FormAggregatePlanner derived = planner(scoped);
        assertNotSame(original, derived);
        fixture.sync.previewFieldUse(report());
        assertTrue(fixture.requests.isEmpty());
        fixture.sync.aggregate(report());
        scoped.aggregatePage(report(), PageQuery.of(1, 5));
        fixture.sync.aggregate(report());
        assertSame(original, planner(fixture.sync));
        assertSame(derived, planner(scoped));
        assertEquals(List.of(1L), fixture.requests.getFirst().parameters());
        assertEquals(List.of(1L, "blue"), fixture.requests.get(1).parameters());
        assertEquals(List.of(1L), fixture.requests.getLast().parameters());
    }

    @Test
    void reactiveClientReusesPlannerWithoutSharingExecutionOrTenantState() throws Exception {
        Fixture fixture = new Fixture();
        ReactiveFormClient blue = fixture.reactive.withDefaultDataScope(DataScope.tenant("tenant", "blue"));
        ReactiveFormClient red = fixture.reactive.withDefaultDataScope(DataScope.tenant("tenant", "red"));
        FormAggregatePlanner original = planner(fixture.reactive);
        FormAggregatePlanner derived = planner(blue);
        assertNotSame(original, derived);
        assertNotSame(derived, planner(red));
        Flux<AggregateRow> pending = blue.aggregate(report());
        assertEquals(1L, Scannable.from(pending).parents().count(),
                "unbudgeted aggregate decoding must be assembled once above the deferred execution");
        assertTrue(fixture.requests.isEmpty());
        pending.blockLast();
        pending.blockLast();
        red.aggregatePage(report(), PageQuery.of(1, 5)).block();
        fixture.reactive.aggregate(report()).blockLast();
        assertSame(original, planner(fixture.reactive));
        assertSame(derived, planner(blue));
        assertEquals(List.of(1L, "blue"), fixture.requests.get(0).parameters());
        assertEquals(List.of(1L, "blue"), fixture.requests.get(1).parameters());
        assertEquals(List.of(1L, "red"), fixture.requests.get(2).parameters());
        assertEquals(List.of(1L), fixture.requests.getLast().parameters());
    }

    private static FormAggregatePlanner planner(Object client) throws Exception {
        Field owner = client.getClass().getDeclaredField("operations");
        owner.setAccessible(true);
        Object operations = owner.get(client);
        for (Field field : operations.getClass().getDeclaredFields()) {
            if (field.getType() != FormAggregatePlanner.class) continue;
            assertTrue(Modifier.isFinal(field.getModifiers()), "planner ownership must be immutable");
            field.setAccessible(true);
            return (FormAggregatePlanner) field.get(operations);
        }
        throw new AssertionError("form operations must own a reusable aggregate planner");
    }

    private static AggregateSpec report() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant", "VARCHAR")).build();
        return AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().where("id", ">", 1L).build()))
                .countRows("rows").build();
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        private final ReactiveFormClient reactive = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("unexpected write")); }
        }, renderer);
        private final SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) { return rows(request); }
            public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("unexpected write");
            }
        }, (request, completed) -> { throw new AssertionError("unexpected batch"); }, renderer);

        private List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return List.of(DynamicRow.copyOf(Map.of(request.sql().contains("flying_aggregate_count") ? "total" : "rows", 1L)));
        }
    }
}
