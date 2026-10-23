package com.flying.orm.consumer;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.operator.EntityCondition;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityQueryPublicContractTest {
    private static final SqlExecutionOptions OPTIONS = SqlExecutionOptions.unlimited();

    @Test
    void wholeQueryInputUsesTheEntityMappingAndTheSameReactiveTerminal() {
        Fixture fixture = new Fixture(3);
        com.flying.orm.rdb.form.spec.QueryInput input = new com.flying.orm.rdb.form.spec.QueryInput(
                com.flying.orm.core.condition.StructuredConditionInput.term("state", "eq", State.ACTIVE),
                List.of("name"), List.of(), List.of(com.flying.orm.core.page.PageSort.desc("name")),
                com.flying.orm.core.page.PageQuery.of(2, 2));
        assertEquals(2, fixture.sync.createQuery().where(User::getId, ">", 0L).input(input).fetch().size());
        SqlRequest expected = fixture.dataRequest;
        assertEquals(List.of(0L, "A", 2, 2L), expected.parameters());
        assertNull(fixture.countRequest);
        Flux<User> reactive = fixture.reactive.createQuery().where(User::getId, ">", 0L).input(input).fetch();
        assertEquals(2, reactive.collectList().block().size());
        assertEquals(2, reactive.collectList().block().size());
        assertEquals(expected.sql(), fixture.dataRequest.sql());
        assertEquals(expected.parameters(), fixture.dataRequest.parameters());
        fixture.sync.createQuery().input(input).page(1, 5);
        assertEquals(List.of("A", 5, 0L), fixture.dataRequest.parameters());
        fixture.sync.createQuery().input(input).page();
        assertEquals(List.of("A", 2, 2L), fixture.dataRequest.parameters());
        fixture.reactive.createQuery().input(input).page().block();
        assertEquals(List.of("A", 2, 2L), fixture.dataRequest.parameters());
    }

    @Test
    void inputMapsPropertyNamesInFilterProjectionExclusionAndPageSorts() {
        Fixture fixture = new Fixture(0);
        com.flying.orm.rdb.form.spec.QueryInput input = new com.flying.orm.rdb.form.spec.QueryInput(
                com.flying.orm.core.condition.StructuredConditionInput.term("state", "eq", State.ACTIVE),
                List.of("id", "state"), List.of("id"), null,
                com.flying.orm.core.page.PageQuery.of(1, 4, com.flying.orm.core.page.PageSort.asc("state")));
        SyncFormRepository<MappedUser> sync = SyncFormRepository.create(fixture.syncClient,
                fixture.syncClient.entityModels().metadata(MappedUser.class).toDynamicForm(), MappedUser.class);
        ReactiveFormRepository<MappedUser> reactive = ReactiveFormRepository.create(fixture.reactiveClient,
                fixture.reactiveClient.entityModels().metadata(MappedUser.class).toDynamicForm(), MappedUser.class);
        sync.createQuery().input(input).fetch();
        SqlRequest expected = fixture.dataRequest;
        assertEquals("select \"user_state\" from \"users\" where \"user_state\" = ? order by \"user_state\" asc limit ? offset ?",
                expected.sql());
        assertEquals(List.of("A", 4, 0L), expected.parameters());
        reactive.createQuery().input(input).fetch().blockLast();
        assertEquals(expected.sql(), fixture.dataRequest.sql());
        assertEquals(expected.parameters(), fixture.dataRequest.parameters());
    }

    @Test
    void propertyNameProjectionAndGroupingUseTheSameEntityQueryOnBothTerminals() {
        Fixture fixture = new Fixture(0);
        SyncFormRepository<MappedUser> sync = SyncFormRepository.create(fixture.syncClient,
                fixture.syncClient.entityModels().metadata(MappedUser.class).toDynamicForm(), MappedUser.class);
        ReactiveFormRepository<MappedUser> reactive = ReactiveFormRepository.create(fixture.reactiveClient,
                fixture.reactiveClient.entityModels().metadata(MappedUser.class).toDynamicForm(), MappedUser.class);
        sync.createQuery().select("id", "state").exclude("id").groupBy("state")
                .where(MappedUser::getState, State.ACTIVE).orderByAsc("state").fetch();
        SqlRequest expected = fixture.dataRequest;
        assertEquals("select \"user_state\" from \"users\" where \"user_state\" = ? group by \"user_state\" order by \"user_state\" asc",
                expected.sql());
        assertEquals(List.of("A"), expected.parameters());
        int executions = fixture.executions.get();
        Flux<MappedUser> result = reactive.createQuery().select("id", "state").exclude("id").groupBy("state")
                .where(MappedUser::getState, State.ACTIVE).orderByAsc("state").fetch();
        assertEquals(executions, fixture.executions.get());
        result.blockLast();
        result.blockLast();
        assertEquals(expected.sql(), fixture.dataRequest.sql());
        assertEquals(expected.parameters(), fixture.dataRequest.parameters());
    }

    @Test
    void namedAndGetterProjectionComposeWithoutChangingDefaultQueries() {
        Fixture fixture = new Fixture(1);
        assertEquals(1, fixture.sync.createQuery().select().exclude().groupBy().fetch().size());
        SqlRequest defaults = fixture.dataRequest;
        fixture.reactive.createQuery().select().exclude().groupBy().fetch().blockLast();
        assertEquals(defaults.sql(), fixture.dataRequest.sql());
        fixture.sync.createQuery().select(new String[0]).groupBy(new String[0]).fetch();
        assertEquals(defaults.sql(), fixture.dataRequest.sql());
        fixture.sync.createQuery().select(List.<com.flying.orm.core.lambda.EntityProperty<User, ?>>of()
                .toArray(com.flying.orm.core.lambda.EntityProperty[]::new)).fetch();
        assertEquals(defaults.sql(), fixture.dataRequest.sql());
        partial(fixture.sync.createQuery().select(User::getId).select("name").exclude(User::getId).one());
        partial(fixture.reactive.createQuery().select("id").select(User::getName).exclude("id").one().block());
        partial(fixture.sync.createQuery().exclude("id", "state").one());
        assertThrows(IllegalArgumentException.class, () -> fixture.sync.createQuery().select("name;drop table users"));
    }

    @Test
    void synchronousNormalStopKeepsMappingAndOnlyLoadsVisitedEntities() {
        Fixture fixture = new Fixture(3);
        AtomicInteger loads = new AtomicInteger();
        List<Long> ids = new ArrayList<>();
        assertEquals(2L, fixture.sync.withListener(loading(loads)).createQuery()
                .where(User::getState, State.ACTIVE).forEachWhile(user -> {
                    assertEquals("loaded", user.name);
                    assertEquals(State.ACTIVE, user.state);
                    ids.add(user.id);
                    return user.id < 2L;
                }));
        assertEquals(List.of(1L, 2L), ids);
        assertEquals(2, loads.get());
        assertEquals(List.of("A"), fixture.dataRequest.parameters());
    }

    @Test
    void synchronousRowConsumptionKeepsEntityMappingAndLoadCallbacks() {
        Fixture fixture = new Fixture(3);
        AtomicInteger loads = new AtomicInteger();
        List<Long> ids = new ArrayList<>();
        long consumed = fixture.sync.withListener(loading(loads)).createQuery()
                .where(User::getState, State.ACTIVE).forEach(user -> {
                    assertEquals("loaded", user.name);
                    assertEquals(State.ACTIVE, user.state);
                    ids.add(user.id);
                });
        assertEquals(3L, consumed);
        assertEquals(List.of(1L, 2L, 3L), ids);
        assertEquals(3, loads.get());
        assertEquals(List.of("A"), fixture.dataRequest.parameters());
    }

    @Test
    void synchronousRowConsumptionStopsLoadCallbacksOnConsumerFailure() {
        Fixture fixture = new Fixture(3);
        AtomicInteger loads = new AtomicInteger();
        IllegalStateException expected = new IllegalStateException("consumer failed");
        assertSame(expected, assertThrows(IllegalStateException.class,
                () -> fixture.sync.withListener(loading(loads)).createQuery()
                        .forEach(user -> { throw expected; })));
        assertEquals(1, loads.get());
    }

    @Test
    void countIsLazyLongValuedAndDoesNotLoadOrProjectEntities() {
        Fixture fixture = new Fixture(0);
        fixture.total = 3_000_000_000L;
        AtomicInteger loaded = new AtomicInteger();
        assertEquals(fixture.total, fixture.sync.withListener(loading(loaded)).createQuery()
                .select(User::getName).where(User::getState, State.ACTIVE).orderByAsc(User::getName).count());
        assertEquals(1, fixture.executions.get());
        assertEquals(0, loaded.get());
        SqlRequest sync = fixture.countRequest;
        assertEquals("select count(*) as total from \"users\" where \"state\" = ?", sync.sql());
        assertEquals(List.of("A"), sync.parameters());
        Mono<Long> count = fixture.reactive.withListener(loading(loaded)).createQuery()
                .select(User::getName).where(User::getState, State.ACTIVE).orderByAsc(User::getName).count(OPTIONS);
        assertEquals(1, fixture.executions.get());
        assertEquals(fixture.total, count.block());
        assertEquals(fixture.total, count.block());
        assertEquals(3, fixture.executions.get());
        assertEquals(0, loaded.get());
        assertEquals(sync.sql(), fixture.countRequest.sql());
        assertEquals(sync.parameters(), fixture.countRequest.parameters());
        assertEquals(fixture.total, fixture.sync.createQuery().count(OPTIONS));
        assertEquals(fixture.total, fixture.reactive.createQuery().count().block());
        fixture.total = 0;
        assertEquals(0L, fixture.sync.createQuery().groupBy(User::getName).count());
        assertTrue(fixture.countRequest.sql().contains("group by \"name\""));
        assertEquals(0L, fixture.reactive.createQuery().groupBy(User::getName).count().block());
    }

    @Test
    void firstLimitsSqlWithoutCountAndPreservesProjectionEnumAndLoading() {
        Fixture fixture = new Fixture(3);
        AtomicInteger loads = new AtomicInteger();
        User first = fixture.sync.withListener(loading(loads)).createQuery()
                .select(User::getName).where(User::getState, State.ACTIVE)
                .orderByAsc(User::getName).first();
        assertEquals("loaded", first.name);
        assertNull(first.id);
        assertEquals(1, loads.get());
        assertEquals(1, fixture.executions.get());
        SqlRequest syncRequest = fixture.dataRequest;
        assertEquals("select \"name\" from \"users\" where \"state\" = ? order by \"name\" asc limit ? offset ?",
                syncRequest.sql());
        assertEquals(List.of("A", 1, 0L), syncRequest.parameters());
        Mono<User> reactive = fixture.reactive.withListener(loading(loads)).createQuery()
                .select(User::getName).where(User::getState, State.ACTIVE)
                .orderByAsc(User::getName).first(OPTIONS);
        assertEquals(1, fixture.executions.get(), "assembly must not execute SQL");
        assertEquals("loaded", reactive.block().name);
        assertEquals("loaded", reactive.block().name);
        assertEquals(3, loads.get());
        assertEquals(3, fixture.executions.get());
        assertEquals(syncRequest.sql(), fixture.dataRequest.sql());
        assertEquals(syncRequest.parameters(), fixture.dataRequest.parameters());
        partial(fixture.sync.createQuery().select(User::getName).first(OPTIONS));
        partial(fixture.reactive.createQuery().select(User::getName).first().block());
    }

    @Test
    void firstDoesNotNotifyForEmptyAndCapturesCallbacksAtAssembly() {
        Fixture empty = new Fixture(0);
        AtomicInteger callbacks = new AtomicInteger();
        assertNull(empty.sync.withListener(loading(callbacks)).createQuery().first());
        assertNull(empty.reactive.withListener(loading(callbacks)).createQuery().first().block());
        assertEquals(0, callbacks.get());
        Fixture fixture = new Fixture(3);
        EntityDmlQueryOperator<User> query = fixture.reactive.createQuery()
                .onLoad(user -> Mono.fromRunnable(callbacks::incrementAndGet));
        Mono<User> first = query.first();
        query.onLoad(user -> Mono.fromRunnable(() -> callbacks.addAndGet(10)));
        assertEquals(1L, first.block().id);
        assertEquals(1, callbacks.get());
        assertEquals(1L, query.first().block().id);
        assertEquals(12, callbacks.get());
    }

    @Test
    void syncEntityTerminalsUseTheSameExplicitProjection() {
        Fixture fixture = new Fixture(1);
        SyncFormRepository<User> repository = fixture.sync;
        assertAll(
                () -> partial(repository.createQuery().select(User::getName).fetch().getFirst()),
                () -> partial(repository.createQuery().select(User::getName).fetch(OPTIONS).getFirst()),
                () -> partial(repository.createQuery().select(User::getName).fetch().getFirst()),
                () -> partial(repository.createQuery().select(User::getName).fetch(OPTIONS).getFirst()),
                () -> partial(repository.createQuery().select(User::getName).one()),
                () -> partialPage(repository.createQuery().select(User::getName).page(1, 10)),
                () -> partialPage(repository.createQuery().select(User::getName).page(1, 10, OPTIONS)));
    }

    @Test
    void reactiveEntityTerminalsUseTheSameExplicitProjection() {
        Fixture fixture = new Fixture(1);
        ReactiveFormRepository<User> repository = fixture.reactive;
        assertAll(
                () -> partial(repository.createQuery().select(User::getName).fetch().single().block()),
                () -> partial(repository.createQuery().select(User::getName).fetch(OPTIONS).single().block()),
                () -> partial(repository.createQuery().select(User::getName).fetch().single().block()),
                () -> partial(repository.createQuery().select(User::getName).fetch(OPTIONS).single().block()),
                () -> partial(repository.createQuery().select(User::getName).one().block()),
                () -> partialPage(repository.createQuery().select(User::getName).page(1, 10).block()),
                () -> partialPage(repository.createQuery().select(User::getName).page(1, 10, OPTIONS).block()));
    }

    @Test
    void syncRepositoryListenerReachesEveryEntityTerminalExactlyOnce() {
        Fixture fixture = new Fixture(1);
        AtomicInteger callbacks = new AtomicInteger();
        SyncFormRepository<User> repository = fixture.sync.withListener(loading(callbacks));
        assertAll(
                () -> assertEquals("loaded", repository.createQuery().fetch().getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().fetch(OPTIONS).getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().fetch().getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().fetch(OPTIONS).getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().one().name),
                () -> assertEquals("loaded", repository.createQuery().page(1, 10).rows().getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().page(1, 10, OPTIONS).rows().getFirst().name));
        assertEquals(7, callbacks.get());
        assertEquals("Ada", repository.createQuery().select(User::getName).executeRows().getFirst().get("name"));
        assertEquals(7, callbacks.get(), "raw rows are not loaded entities");
    }

    @Test
    void reactiveRepositoryListenerReachesEveryEntityTerminalExactlyOnce() {
        Fixture fixture = new Fixture(1);
        AtomicInteger callbacks = new AtomicInteger();
        ReactiveFormRepository<User> repository = fixture.reactive.withListener(loading(callbacks));
        Flux<User> fetched = repository.createQuery().fetch();
        Mono<PageResult<User>> paged = repository.createQuery().page(1, 10);
        assertEquals(0, fixture.executions.get());
        assertEquals(0, callbacks.get());
        assertAll(
                () -> assertEquals("loaded", fetched.single().block().name),
                () -> assertEquals("loaded", repository.createQuery().fetch(OPTIONS).single().block().name),
                () -> assertEquals("loaded", repository.createQuery().fetch().single().block().name),
                () -> assertEquals("loaded", repository.createQuery().fetch(OPTIONS).single().block().name),
                () -> assertEquals("loaded", repository.createQuery().one().block().name),
                () -> assertEquals("loaded", paged.block().rows().getFirst().name),
                () -> assertEquals("loaded", repository.createQuery().page(1, 10, OPTIONS).block().rows().getFirst().name));
        assertEquals(7, callbacks.get());
        assertEquals("Ada", repository.createQuery().select(User::getName).executeRows().single().block().get("name"));
        assertEquals(7, callbacks.get());
    }

    @Test
    void reactiveLoadingWaitsForEachCallbackInDatabaseOrder() {
        Fixture fixture = new Fixture(3);
        List<Long> callbacks = new ArrayList<>();
        List<Long> delivered = new ArrayList<>();
        Sinks.Empty<Void> first = Sinks.empty();
        ReactiveFormRepository<User> repository = fixture.reactive.withListener(event -> {
            callbacks.add(event.entity().id);
            return event.entity().id == 1L ? first.asMono() : Mono.empty();
        });
        Disposable subscription = repository.createQuery().fetch().subscribe(user -> delivered.add(user.id));
        try {
            assertEquals(List.of(1L), callbacks);
            assertTrue(delivered.isEmpty());
            assertEquals(Sinks.EmitResult.OK, first.tryEmitEmpty());
            assertEquals(List.of(1L, 2L, 3L), callbacks);
            assertEquals(callbacks, delivered);
        } finally {
            subscription.dispose();
        }
    }

    @Test
    void cancellingAReadCancelsThePendingCallbackWithoutStartingAnother() {
        Fixture fixture = new Fixture(3);
        AtomicInteger callbacks = new AtomicInteger();
        AtomicBoolean cancelled = new AtomicBoolean();
        ReactiveFormRepository<User> repository = fixture.reactive.withListener(event -> {
            callbacks.incrementAndGet();
            return Mono.<Void>never().doOnCancel(() -> cancelled.set(true));
        });
        Disposable subscription = repository.createQuery().fetch().subscribe();
        try {
            assertEquals(1, callbacks.get());
        } finally {
            subscription.dispose();
        }
        assertTrue(cancelled.get());
        assertEquals(1, callbacks.get());
    }

    @Test
    void callbackFailureRemainsObservableAndStopsFollowingCallbacks() {
        Fixture fixture = new Fixture(3);
        IllegalStateException failure = new IllegalStateException("load failure");
        AtomicInteger callbacks = new AtomicInteger();
        ReactiveEntityListener<User> listener = event -> {
            callbacks.incrementAndGet();
            return Mono.error(failure);
        };
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.sync.withListener(listener).createQuery().fetch()));
        assertEquals(1, callbacks.get());
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.reactive.withListener(listener).createQuery().fetch().collectList().block()));
        assertEquals(2, callbacks.get());
    }

    @Test
    void emptyOrNonUniqueOneDoesNotNotifyAnEntityResult() {
        AtomicInteger callbacks = new AtomicInteger();
        Fixture empty = new Fixture(0);
        assertNull(empty.sync.withListener(loading(callbacks)).createQuery().one());
        assertNull(empty.reactive.withListener(loading(callbacks)).createQuery().one().block());
        Fixture multiple = new Fixture(2);
        assertThrows(RuntimeException.class,
                () -> multiple.sync.withListener(loading(callbacks)).createQuery().one());
        assertThrows(RuntimeException.class,
                () -> multiple.reactive.withListener(loading(callbacks)).createQuery().one().block());
        assertEquals(0, callbacks.get());
    }

    @Test
    void reactiveTerminalsCaptureCallbacksWhenAssembled() {
        Fixture fixture = new Fixture(1);
        List<String> calls = new ArrayList<>();
        EntityDmlQueryOperator<User> query = fixture.reactive.createQuery()
                .onLoad(user -> Mono.fromRunnable(() -> calls.add("first")));
        Flux<User> fetched = query.fetch();
        Mono<User> single = query.one();
        Mono<PageResult<User>> paged = query.page(1, 10);
        query.onLoad(user -> Mono.fromRunnable(() -> calls.add("second")));
        assertTrue(calls.isEmpty());
        fetched.blockLast();
        single.block();
        paged.block();
        assertEquals(List.of("first", "first", "first"), calls);
        calls.clear();
        query.fetch().blockLast();
        query.one().block();
        query.page(1, 10).block();
        assertEquals(List.of("first", "second", "first", "second", "first", "second"), calls);
    }

    @Test
    void syncCallbacksComposeAfterRepositoryLoading() {
        Fixture fixture = new Fixture(1);
        AtomicInteger repositoryLoads = new AtomicInteger();
        User result = fixture.sync.withListener(loading(repositoryLoads)).createQuery()
                .onLoad(user -> user.name += "-first")
                .onLoad(user -> user.name += "-second")
                .one();
        assertEquals("loaded-first-second", result.name);
        assertEquals(1, repositoryLoads.get());
    }

    @Test
    void cursorCallbacksAreCapturedWaitedAndCancelled() {
        for (Function<EntityDmlQueryOperator<User>, Mono<?>> terminal : cursorTerminals()) {
            Fixture fixture = new Fixture(1);
            Sinks.One<Void> callback = Sinks.one();
            AtomicInteger calls = new AtomicInteger();
            AtomicBoolean delivered = new AtomicBoolean();
            AtomicBoolean cancelled = new AtomicBoolean();
            EntityDmlQueryOperator<User> query = fixture.reactive.createQuery().onLoad(user -> {
                calls.incrementAndGet();
                return callback.asMono().doOnCancel(() -> cancelled.set(true));
            });
            Mono<?> result = terminal.apply(query);
            query.onLoad(user -> Mono.error(new AssertionError("late callback must not be captured")));
            assertEquals(0, fixture.executions.get());
            Disposable subscription = result.subscribe(page -> delivered.set(true));
            try {
                assertEquals(1, calls.get());
                assertTrue(!delivered.get());
                assertEquals(Sinks.EmitResult.OK, callback.tryEmitEmpty());
                assertTrue(delivered.get());
            } finally {
                subscription.dispose();
            }
            assertTrue(!cancelled.get());
            EntityDmlQueryOperator<User> pending = fixture.reactive.createQuery()
                    .onLoad(user -> Mono.<Void>never().doOnCancel(() -> cancelled.set(true)));
            Disposable waiting = terminal.apply(pending).subscribe();
            waiting.dispose();
            assertTrue(cancelled.get());
        }
    }

    @Test
    void cursorCallbackFailuresPropagateAndEmptyPagesDoNotLoad() {
        IllegalStateException failure = new IllegalStateException("cursor load failure");
        for (Function<EntityDmlQueryOperator<User>, Mono<?>> terminal : cursorTerminals()) {
            Fixture fixture = new Fixture(2);
            AtomicInteger calls = new AtomicInteger();
            EntityDmlQueryOperator<User> query = fixture.reactive.createQuery().onLoad(user -> {
                calls.incrementAndGet();
                return Mono.error(failure);
            });
            assertSame(failure, assertThrows(IllegalStateException.class, () -> terminal.apply(query).block()));
            assertEquals(1, calls.get());
            Fixture empty = new Fixture(0);
            terminal.apply(empty.reactive.createQuery().onLoad(user -> Mono.error(failure))).block();
        }
        Fixture fixture = new Fixture(2);
        assertSame(failure, assertThrows(IllegalStateException.class, () -> fixture.sync.createQuery()
                .onLoad(user -> { throw failure; }).keysetPage(KeysetPageQuery.first(10, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)))));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> fixture.sync.createQuery()
                .onLoad(user -> { throw failure; }).keysetPage(KeysetPageQuery.first(10, KeysetSort.asc("id", NullOrder.LAST)))));
    }

    private static List<Function<EntityDmlQueryOperator<User>, Mono<?>>> cursorTerminals() {
        return List.of(query -> query.keysetPage(KeysetPageQuery.first(10, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))),
                query -> query.keysetPage(KeysetPageQuery.first(10, KeysetSort.asc("id", NullOrder.LAST))));
    }

    @Test
    void recursivePublicConditionsShareEntityEnumMapping() {
        Fixture fixture = new Fixture(1);
        Consumer<EntityCondition<User>> conditions = group -> group
                .andGroup(nested -> nested.notIn(User::getState, List.of(State.DISABLED))
                        .where(User::getId, 1L))
                .or(nested -> nested.where(User::getName, "Ada").isNull(User::getState));
        fixture.sync.createQuery().or(conditions).fetch();
        SqlRequest syncRequest = fixture.dataRequest;
        fixture.reactive.createQuery().or(conditions).fetch().blockLast();
        assertEquals(syncRequest.sql(), fixture.dataRequest.sql());
        assertEquals(syncRequest.parameters(), fixture.dataRequest.parameters());
        assertEquals(List.of("D", 1L, "Ada"), syncRequest.parameters());
        assertTrue(syncRequest.sql().contains("(\"state\" not in (?) and \"id\" = ?)"), syncRequest.sql());
        assertTrue(syncRequest.sql().contains("or (\"name\" = ? or \"state\" is null)"), syncRequest.sql());
    }

    @Test
    void groupedEntityResultsRequireAndHonorExplicitProjection() {
        Fixture fixture = new Fixture(1);
        partial(fixture.sync.createQuery().select(User::getName).groupBy(User::getName).one());
        partial(fixture.reactive.createQuery().select(User::getName).groupBy(User::getName).one().block());
        assertThrows(IllegalStateException.class,
                () -> fixture.sync.createQuery().groupBy(User::getName).fetch());
        assertThrows(IllegalStateException.class,
                () -> fixture.reactive.createQuery().groupBy(User::getName).fetch());
    }

    private static ReactiveEntityListener<User> loading(AtomicInteger callbacks) {
        return event -> Mono.fromRunnable(() -> {
            assertEquals(EntityLifecyclePhase.POST_LOAD, event.phase());
            callbacks.incrementAndGet();
            event.entity().name = "loaded";
        });
    }

    private static void partial(User user) {
        assertEquals("Ada", user.name);
        assertNull(user.id);
    }

    private static void partialPage(PageResult<User> page) {
        assertEquals(1, page.total());
        assertEquals(1, page.page());
        assertEquals(10, page.size());
        partial(page.rows().getFirst());
    }

    @TableName("users")
    public static final class User {
        @TableId(type = IdType.INPUT)
        private Long id;
        private String name;
        private State state;
        public Long getId() { return id; }
        public String getName() { return name; }
        public State getState() { return state; }
    }

    public enum State {
        ACTIVE("A"), DISABLED("D");
        @EnumValue private final String code;
        State(String code) { this.code = code; }
    }

    @TableName("users")
    public static final class MappedUser {
        @TableId(type = IdType.INPUT) private Long id;
        @com.flying.orm.core.annotation.TableField("user_state") private State state;
        public Long getId() { return id; }
        public State getState() { return state; }
    }

    private static final class Fixture {
        private final int rowCount;
        private final AtomicInteger executions = new AtomicInteger();
        private final SyncFormRepository<User> sync;
        private final ReactiveFormRepository<User> reactive;
        private final SyncFormClient syncClient;
        private final ReactiveFormClient reactiveClient;
        private SqlRequest dataRequest;
        private SqlRequest countRequest;
        private long total;

        private Fixture(int rowCount) {
            this.rowCount = rowCount;
            this.total = rowCount;
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
            syncClient = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows(request); }
                public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new UnsupportedOperationException();
                }
            }, (request, rowCompleted) -> { throw new UnsupportedOperationException(); }, renderer);
            reactiveClient = ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
            }, renderer);
            sync = SyncFormRepository.create(syncClient, syncClient.entityModels().metadata(User.class).toDynamicForm(), User.class);
            reactive = ReactiveFormRepository.create(reactiveClient,
                    reactiveClient.entityModels().metadata(User.class).toDynamicForm(), User.class);
        }

        private List<DynamicRow> rows(SqlRequest request) {
            executions.incrementAndGet();
            if (request.sql().startsWith("select count(*)")) {
                countRequest = request;
                return List.of(DynamicRow.copyOf(Map.of("total", total)));
            }
            dataRequest = request;
            String projection = request.sql().substring(0, request.sql().indexOf(" from "));
            List<DynamicRow> rows = new ArrayList<>();
            int deliveredRows = request.sql().endsWith(" limit ? offset ?")
                    ? Math.min(rowCount, (Integer) request.parameters().get(request.parameters().size() - 2)) : rowCount;
            for (long id = 1; id <= deliveredRows; id++) {
                Map<String, Object> row = new LinkedHashMap<>();
                if (projection.contains("\"id\"")) { row.put("id", id); }
                if (projection.contains("\"name\"")) { row.put("name", "Ada"); }
                if (projection.contains("\"state\"")) { row.put("state", "A"); }
                rows.add(DynamicRow.copyOf(row));
            }
            return rows;
        }
    }
}
