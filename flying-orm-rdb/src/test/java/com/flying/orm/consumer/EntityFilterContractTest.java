package com.flying.orm.consumer;

import com.flying.orm.core.annotation.*;
import com.flying.orm.core.condition.*;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.*;
import com.flying.orm.rdb.form.*;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.*;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EntityFilterContractTest {
    @Test
    void enumCollectionsUseTheSamePersistedValuesForArraysSetsAndIterables() {
        Fixture f = new Fixture();
        for (Object values : List.of(new State[]{State.DISABLED}, Set.of(State.DISABLED),
                (Iterable<State>) () -> List.of(State.DISABLED).iterator())) {
            StructuredConditionInput input = StructuredConditionInput.term("state", "in", values);
            f.sync.createQuery().filter(input).count();
            assertTrue(f.last().parameters().contains("D"), f.last().parameters().toString());
            f.reactive.createQuery().filter(input).count().block();
            assertTrue(f.last().parameters().contains("D"), f.last().parameters().toString());
        }
    }

    @Test
    void configuredCollectionBudgetBoundsIterableConsumption() {
        Fixture f = new Fixture();
        java.util.concurrent.atomic.AtomicInteger consumed = new java.util.concurrent.atomic.AtomicInteger();
        Iterable<Long> values = () -> new Iterator<>() {
            private int index;
            public boolean hasNext() { return index < 100; }
            public Long next() { consumed.incrementAndGet(); return (long) ++index; }
        };
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxCollectionSize(2);
        StructuredConditionInput input = StructuredConditionInput.term("id", "in", values);
        StructuredConditionException error = assertThrows(StructuredConditionException.class,
                () -> f.sync.createQuery().filter(input, policy).count());
        assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, error.code());
        assertTrue(consumed.get() <= 3, "consumed " + consumed.get());
        consumed.set(0);
        assertThrows(StructuredConditionException.class,
                () -> f.reactive.createQuery().filter(input, policy).count().block());
        assertTrue(consumed.get() <= 3, "consumed " + consumed.get());
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void aliasesRegisteredByTheClientRetainEntityEnumValues() {
        Fixture f = new Fixture(StructuredConditionResolver.composite(
                StructuredConditionCustomizer.allowOperator("same", "=")));
        f.sync.createQuery().filter(StructuredConditionInput.term("state", "same", State.DISABLED)).count();
        assertTrue(f.last().parameters().contains("D"), f.last().parameters().toString());
        f.reactive.createQuery().filter(StructuredConditionInput.term("state", "same", State.DISABLED)).count().block();
        assertTrue(f.last().parameters().contains("D"), f.last().parameters().toString());
    }

    @Test
    void malformedValuesKeepStructuredCompilerErrorsAndPaths() {
        Fixture f = new Fixture();
        for (StructuredConditionInput input : List.of(
                StructuredConditionInput.term("state", "eq", null),
                StructuredConditionInput.term("state", "in", List.of()),
                StructuredConditionInput.term("state", " ", "D"),
                new StructuredConditionInput("state", "eq", "D", "and", List.of()))) {
            assertThrows(StructuredConditionException.class, () -> f.sync.createQuery().filter(input).count());
            assertThrows(StructuredConditionException.class, () -> f.reactive.createQuery().filter(input).count().block());
        }
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void entityFiltersStillUseConfiguredCustomizers() {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        Fixture f = new Fixture(StructuredConditionResolver.composite(new StructuredConditionCustomizer() {
            public StructuredConditionInput adaptFields(com.flying.orm.core.form.DynamicForm form,
                    java.util.function.Function<String, com.flying.orm.core.form.DynamicField> fields,
                    StructuredConditionInput input) {
                calls.incrementAndGet();
                assertEquals("status_code", input.field());
                return StructuredConditionInput.term(input.field(), "eq", State.DISABLED);
            }
        }));
        f.sync.createQuery().filter(StructuredConditionInput.term("state", "eq", "A")).count();
        f.reactive.createQuery().filter(StructuredConditionInput.term("state", "eq", "A")).count().block();
        assertEquals(2, calls.get());
        assertTrue(f.last().parameters().contains("D"));
        assertFalse(f.last().parameters().contains("A"));
    }

    @Test
    void filtersMapPropertiesAndEnumsAcrossAllEntityReadTerminals() {
        Fixture f = new Fixture();
        StructuredConditionInput input = StructuredConditionInput.or(
                StructuredConditionInput.term("state", "eq", State.DISABLED),
                StructuredConditionInput.term("state", "in", List.of(State.ACTIVE)));
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().allowOnlyFields(List.of("state"));
        f.sync.createQuery().filter(input, policy).where(User::getId, 9L).fetch();
        SqlRequest expected = f.last();
        assertTrue(expected.sql().contains("\"status_code\" = ? or \"status_code\" in (?)"), expected.sql());
        assertTrue(expected.parameters().containsAll(List.of("D", "A", 9L, 7L, 0)));
        f.reactive.createQuery().filter(input, policy).where(User::getId, 9L).fetch().blockLast();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        assertNotNull(f.sync.createQuery().filter(input).one());
        assertNotNull(f.reactive.createQuery().filter(input).one().block());
        assertNotNull(f.sync.createQuery().filter(input).first());
        assertNotNull(f.reactive.createQuery().filter(input).first().block());
        assertEquals(1, f.sync.createQuery().filter(input).page(1, 5).total());
        assertEquals(1, f.reactive.createQuery().filter(input).page(1, 5).block().total());
        assertEquals(1, f.sync.createQuery().filter(input).count());
        assertEquals(1, f.reactive.createQuery().filter(input).count().block());
        assertTrue(f.last().parameters().containsAll(List.of("D", "A", 7L, 0)));
    }

    @Test
    void filterPolicyAndInternalFieldsCannotBeBypassedByPropertyAliases() {
        Fixture f = new Fixture();
        StructuredConditionPolicy denied = StructuredConditionPolicy.defaults().denyFields(List.of("state"));
        assertThrows(StructuredConditionException.class, () -> f.sync.createQuery()
                .filter(StructuredConditionInput.term("status_code", "eq", "D"), denied).count());
        assertThrows(StructuredConditionException.class, () -> f.reactive.createQuery()
                .filter(StructuredConditionInput.term("state", "eq", "D"), denied).count().block());
        assertThrows(StructuredConditionException.class, () -> f.sync.createQuery()
                .filter(StructuredConditionInput.term("deleted", "eq", 1)).fetch());
        assertThrows(StructuredConditionException.class, () -> f.sync.createQuery()
                .filter(StructuredConditionInput.term("missing", "eq", 1)).fetch());
        assertTrue(f.requests.isEmpty());
    }

    @Test
    void replacingFrontendFilterDoesNotChangeServerConditionsOrPublishedQueries() {
        Fixture f = new Fixture();
        EntityDmlQueryOperator<User> query = f.reactive.createQuery().where(User::getId, 9L)
                .filter(StructuredConditionInput.term("state", "eq", State.ACTIVE));
        Mono<Long> before = query.count();
        query.filter(StructuredConditionInput.term("state", "eq", State.DISABLED));
        assertTrue(f.requests.isEmpty());
        before.block();
        assertTrue(f.last().parameters().containsAll(List.of("A", 9L, 7L, 0)));
        assertFalse(f.last().parameters().contains("D"));
        query.count().block();
        assertTrue(f.last().parameters().containsAll(List.of("D", 9L, 7L, 0)));
    }

    @TableName("users")
    public static class User {
        @TableId(type = IdType.INPUT) private Long id;
        @TableField("status_code") private State state;
        private Long tenant;
        @TableLogic private Integer deleted;
        public Long getId() { return id; }
        public State getState() { return state; }
    }
    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String value;
        State(String value) { this.value = value; }
    }
    private static class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final SyncFormRepository<User> sync;
        final ReactiveFormRepository<User> reactive;
        Fixture() {
            this(null);
        }
        Fixture(StructuredConditionResolver resolver) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            SyncFormClient jdbc = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows(request); }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError(); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new AssertionError(); }
            }, (request, completed) -> { throw new AssertionError(); }, renderer).withDefaultDataScope(DataScope.tenant("tenant", 7L));
            ReactiveFormClient r2dbc = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
                public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError(); }
            }, renderer).withDefaultDataScope(DataScope.tenant("tenant", 7L));
            if (resolver != null) {
                jdbc = jdbc.withStructuredConditionResolver(resolver);
                r2dbc = r2dbc.withStructuredConditionResolver(resolver);
            }
            sync = SyncFormRepository.create(jdbc, jdbc.entityModels().metadata(User.class).toDynamicForm(), User.class);
            reactive = ReactiveFormRepository.create(r2dbc, r2dbc.entityModels().metadata(User.class).toDynamicForm(), User.class);
        }
        SqlRequest last() { return requests.getLast(); }
        List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return List.of(DynamicRow.copyOf(request.sql().startsWith("select count") ? Map.of("total", 1L)
                    : Map.of("id", 9L, "status_code", "A", "tenant", 7L, "deleted", 0)));
        }
    }
}
