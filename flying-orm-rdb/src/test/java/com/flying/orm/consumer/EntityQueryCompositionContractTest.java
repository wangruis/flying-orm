package com.flying.orm.consumer;

import com.flying.orm.core.annotation.*;
import com.flying.orm.core.condition.*;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.*;
import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.*;
import com.flying.orm.rdb.form.*;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.operator.*;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.*;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class EntityQueryCompositionContractTest {
    @Test
    void namedConditionsShareMappingAcrossQueryUpdateDeleteAndNestedGroups() {
        Fixture f = new Fixture();
        SyncFormRepository<Mapped> sync = SyncFormRepository.create(f.jdbc,
                f.jdbc.entityModels().metadata(Mapped.class).toDynamicForm(), Mapped.class);
        ReactiveFormRepository<Mapped> reactive = ReactiveFormRepository.create(f.r2dbc,
                f.r2dbc.entityModels().metadata(Mapped.class).toDynamicForm(), Mapped.class);
        sync.createQuery().select(Mapped::id).where(Mapped::state, State.ACTIVE)
                .or(group -> group.where(Mapped::state, "in", List.of(State.DISABLED))
                        .where(Mapped::id, ">", 0L)).fetch();
        SqlRequest expected = f.last();
        sync.createQuery().select("id").where("state", State.ACTIVE)
                .or(group -> group.where("user_state", "in", List.of(State.DISABLED))
                        .where("id", ">", 0L)).fetch();
        assertEquals(expected, f.last());
        int executed = f.requests.size();
        Flux<Mapped> query = reactive.createQuery().select("id").where("user_state", State.ACTIVE)
                .or(group -> group.where("state", "in", List.of(State.DISABLED))
                        .where("id", ">", 0L)).fetch();
        assertEquals(executed, f.requests.size());
        query.blockLast();
        query.blockLast();
        assertEquals(expected, f.last());
        assertTrue(expected.sql().contains("\"user_state\" = ?"));
        assertEquals(List.of("A", "D", 0L, 7L), expected.parameters());

        sync.createUpdate().set(Mapped::state, State.DISABLED).where(Mapped::state, State.ACTIVE).execute();
        expected = f.last();
        sync.createUpdate().set("state", State.DISABLED).where("state", State.ACTIVE).execute();
        assertEquals(expected, f.last());
        executed = f.requests.size();
        Mono<Long> update = reactive.createUpdate().set("state", State.DISABLED)
                .where("user_state", State.ACTIVE).execute();
        assertEquals(executed, f.requests.size());
        assertEquals(1L, update.block());
        assertEquals(expected, f.last());
        assertEquals(List.of("D", "A", 7L), expected.parameters());

        sync.createDelete().where(Mapped::state, "in", List.of(State.DISABLED)).execute();
        expected = f.last();
        sync.createDelete().where("state", "in", List.of(State.DISABLED)).execute();
        assertEquals(expected, f.last());
        reactive.createDelete().where("user_state", "in", List.of(State.DISABLED)).execute().block();
        assertEquals(expected, f.last());
        assertEquals(List.of("D", 7L), expected.parameters());
    }

    @Test
    void namedConditionsRejectUnknownFieldsAndKeepValuesBound() {
        Fixture f = new Fixture();
        String value = "'; drop table users; --";
        f.sync.createQuery().where("name", value).fetch();
        assertFalse(f.last().sql().contains(value));
        assertTrue(f.last().parameters().contains(value));
        int executed = f.requests.size();
        assertThrows(IllegalArgumentException.class, () -> f.sync.createQuery().where("unknown", 1));
        assertThrows(IllegalArgumentException.class, () -> f.reactive.createUpdate().where("name;drop table users", 1));
        assertThrows(IllegalArgumentException.class, () -> f.sync.createDelete().or(group -> group.where("unknown", 1)));
        assertEquals(executed, f.requests.size());
    }

    @Test
    void prioritySortingMapsEnumValuesAndKeepsPublishedCopiesIndependent() {
        Fixture f = new Fixture();
        Object[] preferred = { State.DISABLED, State.ACTIVE };
        EntityDmlQueryOperator<User> base = f.reactive.createQuery().select(User::id)
                .orderByFirst(User::state, preferred);
        preferred[0] = State.ACTIVE;
        EntityDmlQueryOperator<User> copy = base.copy().orderByDesc(User::id);
        Mono<?> published = copy.page(1, 2);
        base.orderByFirst(User::tenant, 99L);
        copy.orderByFirst(User::tenant, 100L);
        published.block();
        SqlRequest expected = f.last();
        assertTrue(expected.sql().contains("case \"state\" when ? then 0 when ? then 1 else 2 end asc, \"id\" desc"), expected.sql());
        assertTrue(expected.parameters().containsAll(List.of("D", "A")), expected.parameters().toString());
        assertFalse(expected.parameters().contains(99L));
        assertFalse(expected.parameters().contains(100L));
        f.sync.createQuery().select(User::id).orderByFirst(User::state, State.DISABLED, State.ACTIVE)
                .orderByDesc(User::id).page(1, 2);
        assertEquals(expected, f.last());
        assertFalse(expected.sql().substring(0, expected.sql().indexOf(" from ")).contains("__fo_order"));
    }

    @Test
    void allSelectableModelsApplyExclusionsAndClientDefaultFieldScope() {
        Fixture f = new Fixture();
        DataScope scope = DataScope.none().withFields(FieldScope.readable("id", "name"));
        SyncFormClient sync = f.jdbc.withDefaultDataScope(scope);
        ReactiveFormClient reactive = f.r2dbc.withDefaultDataScope(scope);
        Plain row = SyncEntityDmlOperator.create(sync, sync.entityRenderer(), Plain.class).query()
                .exclude(Plain::id).one();
        assertEquals("Ada", row.name()); assertNull(row.id()); assertNull(row.tenant());
        SqlRequest expected = f.last();
        assertEquals(row, EntityDmlOperator.create(reactive, reactive.entityRenderer(), Plain.class).query()
                .exclude(Plain::id).copy().one().block());
        assertEquals(expected.sql(), f.last().sql());
        assertThrows(MappingException.class, () -> SyncEntityDmlOperator.create(sync, sync.entityRenderer(), Plain.class)
                .query().exclude(Plain::id, Plain::name).one());
        assertThrows(MappingException.class, () -> EntityDmlOperator.create(reactive, reactive.entityRenderer(), Plain.class)
                .query().exclude(Plain::id, Plain::name).one());
    }

    @Test
    void copiedOptionalBusinessTermsRetainTheRegisteredShapeAndDomainValues() {
        SqlTermHandler custom = new SqlTermHandler() {
            public String id() { return "chosen-state"; }
            public ConditionValueShape shape() { return ConditionValueShape.COLLECTION; }
            public SqlFragment render(TermCondition term, SqlRenderContext context) {
                assertEquals(List.of(State.ACTIVE), term.value());
                return SqlTermHandler.in().render(TermCondition.of(term.field(), "in", List.of("A")), context);
            }
        };
        Fixture f = new Fixture(custom);
        f.sync.createQuery().copy().whereIfPresent(User::state, "chosen-state", List.of())
                .whereIfPresent(User::state, "chosen-state", List.of(State.ACTIVE)).copy().fetch();
        SqlRequest expected = f.last();
        f.reactive.createQuery().copy().whereIfPresent("state", "chosen-state", List.of())
                .whereIfPresent("state", "chosen-state", List.of(State.ACTIVE)).copy().fetch().blockLast();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        assertTrue(expected.parameters().contains("A"));
    }

    @Test
    void copiesPreserveSensitiveDisplayWithoutSharingLaterChanges() {
        Fixture f = new Fixture();
        SyncEntityDmlQueryOperator<User> sync = f.sync.createQuery().select(User::name).showSensitive();
        SyncEntityDmlQueryOperator<User> syncCopy = sync.copy();
        sync.masked();
        assertEquals("Ada", syncCopy.one().name());
        assertNotEquals("Ada", sync.one().name());
        EntityDmlQueryOperator<User> reactive = f.reactive.createQuery().select(User::name).masked();
        EntityDmlQueryOperator<User> reactiveCopy = reactive.copy();
        reactive.showSensitive();
        assertNotEquals("Ada", reactiveCopy.one().block().name());
        assertEquals("Ada", reactive.one().block().name());
    }

    @Test
    void optionalConditionsUseCoreRulesInQueriesAndNestedGroups() {
        Fixture f = new Fixture();
        f.sync.createQuery().whereIfPresent(User::name, null)
                .whereIfPresent(User::name, "  ").whereIfPresent(User::state, "in", List.of())
                .or(group -> group.whereIfPresent(User::name, null).whereIfPresent(User::state, State.ACTIVE))
                .whereIfPresent(User::id, ">", 0L).fetch();
        SqlRequest expected = f.last();
        assertFalse(expected.sql().substring(expected.sql().indexOf(" where ")).contains("\"name\""));
        assertTrue(expected.parameters().containsAll(List.of("A", 0L, 7L, 0)));
        f.reactive.createQuery().whereIfPresent("name", null)
                .whereIfPresent("name", "  ").whereIfPresent("state", "in", List.of())
                .or(group -> group.whereIfPresent("name", null).whereIfPresent("state", State.ACTIVE)
                        .whereIfPresent("state", "in", List.of()))
                .whereIfPresent("id", ">", 0L).fetch().blockLast();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        f.sync.createQuery().where(User::name, null).fetch();
        SqlRequest nullable = f.last();
        assertTrue(nullable.sql().contains("\"name\" = "));
        assertTrue(nullable.parameters().contains(null));
        f.reactive.createQuery().where("name", null).fetch().blockLast();
        assertEquals(nullable.sql(), f.last().sql());
        assertEquals(nullable.parameters(), f.last().parameters());
    }

    @Test
    void optionalConditionsHaveTheSameMeaningForWrites() {
        Fixture f = new Fixture();
        f.sync.createUpdate().set(User::name, "new").whereIfPresent(User::state, State.ACTIVE)
                .whereIfPresent(User::name, "like", null).execute();
        SqlRequest expected = f.last();
        f.reactive.createUpdate().set(User::name, "new").whereIfPresent("state", State.ACTIVE)
                .whereIfPresent("name", "like", null).execute().block();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        f.sync.createDelete().whereIfPresent(User::state, "in", List.of(State.ACTIVE))
                .whereIfPresent(User::name, null).execute();
        expected = f.last();
        f.reactive.createDelete().whereIfPresent("state", "in", List.of(State.ACTIVE))
                .whereIfPresent("name", null).execute().block();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
    }

    @Test
    void excludingDefaultsNeverReintroducesHiddenFieldsAndWorksOnAllTerminals() {
        Fixture f = new Fixture();
        SyncEntityDmlQueryOperator<User> sync = f.sync.createQuery().exclude(User::state, User::tenant, User::deleted);
        EntityDmlQueryOperator<User> reactive = f.reactive.createQuery().exclude(User::state, User::tenant, User::deleted);
        assertProjected(sync.fetch().getFirst()); assertProjected(sync.one()); assertProjected(sync.first());
        assertProjected(sync.page(1, 10).rows().getFirst());
        assertEquals(Set.of("id", "name"), sync.executeRows().getFirst().keySet());
        assertProjected(reactive.fetch().blockFirst()); assertProjected(reactive.one().block());
        assertProjected(reactive.first().block()); assertProjected(reactive.page(1, 10).block().rows().getFirst());
        assertEquals(Set.of("id", "name"), reactive.executeRows().blockFirst().keySet());
        for (SqlRequest request : f.requests) {
            if (!request.sql().startsWith("select count(")) {
                String projection = request.sql().substring(0, request.sql().indexOf(" from "));
                assertEquals("select \"id\", \"name\"", projection);
            }
        }
    }

    @Test
    void exclusionsWinRegardlessOfSelectOrderButCountDoesNotNeedProjection() {
        Fixture f = new Fixture();
        f.sync.createQuery().exclude(User::name).select(User::id, User::name).fetch();
        SqlRequest expected = f.last();
        f.reactive.createQuery().select(User::id, User::name).exclude(User::name).fetch().blockLast();
        assertEquals(expected.sql(), f.last().sql());
        assertTrue(expected.sql().startsWith("select \"id\" from "));
        int executed = f.requests.size();
        assertThrows(IllegalArgumentException.class,
                () -> f.sync.createQuery().select(User::name).exclude(User::name).fetch());
        assertThrows(IllegalArgumentException.class,
                () -> f.reactive.createQuery().select(User::name).exclude(User::name).fetch().blockLast());
        assertEquals(executed, f.requests.size());
        assertEquals(1L, f.sync.createQuery().select(User::name).exclude(User::name).count());
        assertEquals(1L, f.reactive.createQuery().select(User::name).exclude(User::name).count().block());
    }

    @Test
    void implicitExclusionsIntersectFieldScopeWithoutChangingExplicitSelectionRules() {
        Fixture f = new Fixture();
        DataScope readable = DataScope.none().withFields(FieldScope.readable("name", "state"));
        User user = f.sync.createQuery().scope(readable).exclude(User::state).one();
        assertNull(user.id()); assertEquals("Ada", user.name()); assertNull(user.state());
        assertThrows(MappingException.class, () -> f.reactive.createQuery().scope(readable)
                .exclude(User::name, User::state).one());
        int executed = f.requests.size();
        assertThrows(IllegalArgumentException.class, () -> f.sync.createQuery().scope(readable).select(User::id).fetch());
        assertEquals(executed, f.requests.size());
    }

    @Test
    void copiesIsolateConditionsSortsProjectionsFiltersAndCallbacksInBothDirections() {
        Fixture f = new Fixture();
        List<String> names = new ArrayList<>(List.of("Ada"));
        List<String> calls = new ArrayList<>();
        SyncEntityDmlQueryOperator<User> base = f.sync.createQuery().in(User::name, names)
                .where(User::state, State.ACTIVE).orderByAsc(User::id)
                .filter(StructuredConditionInput.term("id", ">", 0L)).onLoad(user -> calls.add("base"));
        SyncEntityDmlQueryOperator<User> branch = base.copy().where(User::id, 9L).exclude(User::name)
                .orderByDesc(User::name).onLoad(user -> calls.add("branch"));
        names.set(0, "external-change");
        base.where(User::tenant, 7L).onLoad(user -> calls.add("later-base"));
        branch.fetch();
        SqlRequest expected = f.last();
        assertEquals(List.of("base", "branch"), calls);
        assertTrue(expected.parameters().containsAll(List.of("Ada", "A", 9L, 0L)));
        assertFalse(expected.parameters().contains("external-change"));
        calls.clear();
        base.fetch();
        assertEquals(List.of("base", "later-base"), calls);
        assertFalse(f.last().parameters().contains(9L));
        assertFalse(f.last().sql().contains(" desc"));
        assertTrue(f.last().sql().startsWith("select \"id\", \"name\""));
        branch.filter(StructuredConditionInput.term("id", "<", 20L)).fetch();
        base.fetch();
        assertTrue(f.last().parameters().contains(0L));
        assertFalse(f.last().parameters().contains(20L));

        EntityDmlQueryOperator<User> reactiveBase = f.reactive.createQuery().in(User::name, List.of("Ada"))
                .where(User::state, State.ACTIVE).orderByAsc(User::id)
                .filter(StructuredConditionInput.term("id", ">", 0L));
        EntityDmlQueryOperator<User> reactiveBranch = reactiveBase.copy().where(User::id, 9L)
                .exclude(User::name).orderByDesc(User::name);
        Mono<User> published = reactiveBranch.one();
        reactiveBase.where(User::tenant, 7L);
        reactiveBranch.where(User::id, 100L).select(User::id);
        published.block();
        assertEquals(expected.sql(), f.last().sql());
        assertEquals(expected.parameters(), f.last().parameters());
        reactiveBase.fetch().blockLast();
        assertFalse(f.last().parameters().contains(9L));
        assertFalse(f.last().parameters().contains(100L));
    }

    @Test
    void copiedGroupsAndReactiveLoadCallbacksDoNotShareLaterMutations() {
        Fixture f = new Fixture();
        List<String> calls = new ArrayList<>();
        EntityDmlQueryOperator<User> base = f.reactive.createQuery().select(User::name).groupBy(User::name)
                .onLoad(user -> Mono.fromRunnable(() -> calls.add("base")));
        EntityDmlQueryOperator<User> branch = base.copy().select(User::state).groupBy(User::state)
                .onLoad(user -> Mono.fromRunnable(() -> calls.add("branch")));
        base.onLoad(user -> Mono.fromRunnable(() -> calls.add("later")));
        branch.fetch().blockLast();
        assertTrue(f.last().sql().contains("group by \"name\", \"state\""));
        assertEquals(List.of("base", "branch"), calls);
        calls.clear();
        base.fetch().blockLast();
        assertTrue(f.last().sql().endsWith("group by \"name\""));
        assertEquals(List.of("base", "later"), calls);
    }

    private static void assertProjected(User user) {
        assertEquals(9L, user.id()); assertEquals("Ada", user.name());
        assertNull(user.state()); assertNull(user.hidden()); assertNull(user.tenant()); assertNull(user.deleted());
    }

    @TableName("users")
    public record User(@TableId(type = IdType.INPUT) Long id,
                       @MaskedField(display = SensitiveDisplayMode.FULL, suffix = 0) String name, State state,
                       Long tenant, @TableLogic Integer deleted, @TableField(select = false) String hidden) { }
    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }
    @TableName("users")
    public record Plain(@TableId(type = IdType.INPUT) Long id, String name, Long tenant) { }

    @TableName("users")
    public record Mapped(@TableId(type = IdType.INPUT) Long id,
                         @TableField("user_state") State state, Long tenant) { }

    private static class Fixture {
        final List<SqlRequest> requests = new ArrayList<>();
        final SyncFormRepository<User> sync;
        final ReactiveFormRepository<User> reactive;
        final SyncFormClient jdbc;
        final ReactiveFormClient r2dbc;
        Fixture(SqlTermHandler... terms) {
            SqlRenderer.Builder conditions = SqlRenderer.builder().addDefaultTerms();
            for (SqlTermHandler term : terms) conditions.addTerm(term);
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions.build(), RdbDialect.postgresql());
            jdbc = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows(request); }
                public long rowsUpdated(SqlRequest request) { requests.add(request); return 1L; }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new AssertionError(); }
            }, (request, completed) -> { throw new AssertionError(); }, renderer).withDefaultDataScope(DataScope.tenant("tenant", 7L));
            r2dbc = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.fromSupplier(() -> { requests.add(request); return 1L; }); }
            }, renderer).withDefaultDataScope(DataScope.tenant("tenant", 7L));
            sync = SyncFormRepository.create(jdbc, jdbc.entityModels().metadata(User.class).toDynamicForm(), User.class);
            reactive = ReactiveFormRepository.create(r2dbc, r2dbc.entityModels().metadata(User.class).toDynamicForm(), User.class);
        }
        List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            if (request.sql().startsWith("select count(")) return List.of(DynamicRow.copyOf(Map.of("total", 1L)));
            String projection = request.sql().substring(0, request.sql().indexOf(" from "));
            Map<String, Object> row = new LinkedHashMap<>();
            Map.of("id", 9L, "name", "Ada", "state", "A", "tenant", 7L, "deleted", 0, "hidden", "secret")
                    .forEach((field, value) -> { if (projection.contains("\"" + field + "\"")) row.put(field, value); });
            return List.of(DynamicRow.copyOf(row));
        }
        SqlRequest last() { return requests.getLast(); }
    }
}
