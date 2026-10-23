package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.*;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.repository.ReactiveFormRepository;
import com.flying.orm.rdb.repository.SyncFormRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import static org.junit.jupiter.api.Assertions.*;

class QueryPreparationTest {
    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("tenant", "BIGINT")).build();
    private static final QuerySpec QUERY = QuerySpec.of(FORM, ConditionGroup.and().build());
    private static final PageQuery PAGE = PageQuery.of(1, 2);
    private static final KeysetPageQuery KEYSET = KeysetPageQuery.first(2,
            KeysetSort.asc("id", NullOrder.LAST));
    private static final QuerySpec LOCK = QUERY.withLock(ReadLock.update());

    @Test
    void exposesPreparationOnBothClientTypes() {
        assertDoesNotThrow(() -> ReactiveFormClient.class.getMethod("withQueryPreparation", Function.class));
        assertDoesNotThrow(() -> SyncFormClient.class.getMethod("withQueryPreparation", UnaryOperator.class));
        assertDoesNotThrow(() -> ReactiveFormRepository.class.getMethod("withQueryPreparation", Function.class));
        assertDoesNotThrow(() -> SyncFormRepository.class.getMethod("withQueryPreparation", UnaryOperator.class));
    }

    @Test
    void ordinaryReadTerminalsUseTheLockStoredInTheQuery() {
        for (boolean reactive : List.of(false, true)) {
            Capture capture = new Capture();
            if (reactive) {
                ReactiveFormClient client = capture.reactive();
                client.select(LOCK).collectList().block();
                client.first(LOCK).block();
                client.keysetPage(LOCK, KEYSET).block();
            } else {
                SyncFormClient client = capture.sync();
                client.select(LOCK);
                client.first(LOCK);
                client.keysetPage(LOCK, KEYSET);
            }
            assertEquals(3, capture.requests.size());
            for (SqlRequest request : capture.requests) assertTrue(request.sql().endsWith("FOR UPDATE"), request.sql());
            assertTrue(capture.requests.get(1).sql().contains("limit"), capture.requests.get(1).sql());
            assertEquals(RdbDialect.postgresql().pagination().firstSelectParameters(List.of()),
                    capture.requests.get(1).parameters());
        }
    }

    @Test
    void unsupportedLockCombinationsDoNotSilentlyLoseTheLock() {
        Capture capture = new Capture();
        assertThrows(UnsupportedOperationException.class, () -> capture.sync().page(LOCK, PAGE));
        assertThrows(UnsupportedOperationException.class, () -> capture.reactive().page(LOCK, PAGE).block());
        assertTrue(capture.requests.isEmpty());
    }

    @TestFactory
    Stream<DynamicTest> queryTerminalsPrepareOnceAndKeepDefaultScope() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of("select", "first", "count", "page",
                "keyset", "lock", "lockKeyset", "governed", "entity", "entityCount", "entityPage",
                "repository", "repositoryPage").map(terminal -> DynamicTest.dynamicTest(
                        reactive + "/" + terminal, () -> {
                            Capture capture = new Capture();
                            AtomicInteger prepared = new AtomicInteger();
                            UnaryOperator<QuerySpec> prepare = spec -> {
                                prepared.incrementAndGet();
                                return spec.withScope(DataScope.tenant("tenant", 7L));
                            };
                            DataScope defaults = DataScope.where(ConditionGroup.and().where("id", "=", 99L).build());
                            if (reactive) {
                                ReactiveFormClient client = capture.reactive().withQueryPreparation(
                                        spec -> Mono.fromSupplier(() -> prepare.apply(spec)))
                                        .withDefaultDataScope(defaults)
                                        .withDefaultExecutionOptions(SqlExecutionOptions.unlimited());
                                Mono<?> pending = read(client, terminal);
                                assertEquals(0, prepared.get());
                                assertTrue(capture.requests.isEmpty());
                                pending.block();
                            } else {
                                SyncFormClient client = capture.sync().withQueryPreparation(prepare)
                                        .withDefaultDataScope(defaults)
                                        .withDefaultExecutionOptions(SqlExecutionOptions.unlimited());
                                read(client, terminal);
                            }
                            assertEquals(1, prepared.get(), "one preparation per logical terminal");
                            assertFalse(capture.requests.isEmpty());
                            for (SqlRequest request : capture.requests) {
                                assertTrue(request.parameters().containsAll(List.of(7L, 99L)), request.toString());
                            }
                            if (terminal.equals("page") || terminal.endsWith("Page")) {
                                assertEquals(2, capture.requests.size(), "count and data use one preparation");
                            }
                        })));
    }

    @Test
    void reactivePreparationIsPerSubscriptionAndSeesSubscriberContext() {
        Capture capture = new Capture();
        ReactiveFormClient base = capture.reactive();
        ReactiveFormClient prepared = base.withQueryPreparation(spec -> Mono.deferContextual(context ->
                Mono.just(spec.withScope(DataScope.tenant("tenant", context.get("tenant"))))));
        Mono<List<DynamicRow>> query = prepared.select(QUERY).collectList();
        query.contextWrite(context -> context.put("tenant", 7L)).block();
        query.contextWrite(context -> context.put("tenant", 8L)).block();
        base.select(QUERY).collectList().block();
        assertEquals(List.of(7L), capture.requests.get(0).parameters());
        assertEquals(List.of(8L), capture.requests.get(1).parameters());
        assertTrue(capture.requests.get(2).parameters().isEmpty());
    }

    @Test
    void composedPreparationIsOrderedAndFailuresPreventExecution() {
        Capture capture = new Capture();
        List<Integer> calls = new ArrayList<>();
        UnaryOperator<QuerySpec> first = spec -> { calls.add(1); return spec.withScope(DataScope.tenant("tenant", 7L)); };
        UnaryOperator<QuerySpec> second = spec -> {
            calls.add(2);
            assertFalse(spec.scope().condition().isEmpty());
            return spec;
        };
        capture.sync().withQueryPreparation(first).withQueryPreparation(second).count(QUERY);
        assertEquals(List.of(1, 2), calls);
        calls.clear();
        capture.reactive().withQueryPreparation(spec -> Mono.just(first.apply(spec)))
                .withQueryPreparation(spec -> Mono.just(second.apply(spec))).count(QUERY).block();
        assertEquals(List.of(1, 2), calls);
        capture.requests.clear();
        IllegalStateException failure = new IllegalStateException("prepare failed");
        assertSame(failure, assertThrows(IllegalStateException.class, () -> capture.sync()
                .withQueryPreparation(spec -> { throw failure; }).count(QUERY)));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> capture.reactive()
                .withQueryPreparation(spec -> Mono.error(failure)).count(QUERY).block()));
        assertThrows(RuntimeException.class, () -> capture.reactive()
                .withQueryPreparation(spec -> Mono.empty()).count(QUERY).block());
        assertThrows(RuntimeException.class, () -> capture.reactive()
                .withQueryPreparation(spec -> Flux.just(spec, spec)).count(QUERY).block());
        AtomicInteger cancelled = new AtomicInteger();
        reactor.core.Disposable subscription = capture.reactive().withQueryPreparation(
                spec -> Mono.<QuerySpec>never().doOnCancel(cancelled::incrementAndGet)).select(QUERY).subscribe();
        subscription.dispose();
        assertEquals(1, cancelled.get());
        assertTrue(capture.requests.isEmpty());
    }

    @Test
    void nullFromAnEarlierSynchronousPreparationCannotBeRecoveredByTheNext() {
        Capture capture = new Capture();
        AtomicInteger later = new AtomicInteger();
        SyncFormClient client = capture.sync().withQueryPreparation(spec -> null)
                .withQueryPreparation(spec -> { later.incrementAndGet(); return QUERY; });
        assertThrows(NullPointerException.class, () -> client.select(QUERY));
        assertEquals(0, later.get());
        assertTrue(capture.requests.isEmpty());
    }

    @Test
    void repositoryPreparationIsScopedToTheDerivedRepository() {
        Capture capture = new Capture();
        AtomicInteger calls = new AtomicInteger();
        UnaryOperator<QuerySpec> preparation = spec -> {
            calls.incrementAndGet();
            return spec.withScope(DataScope.tenant("tenant", 7L));
        };
        ReactiveFormRepository<User> base = ReactiveFormRepository.create(capture.reactive(), FORM, User.class);
        ReactiveFormRepository<User> derived = base.withQueryPreparation(spec -> Mono.just(preparation.apply(spec)))
                .withListener(event -> Mono.empty());
        derived.createQuery().copy().count().block();
        base.createQuery().count().block();
        SyncFormRepository<User> sync = SyncFormRepository.create(capture.sync(), FORM, User.class);
        sync.withQueryPreparation(preparation).withListener(event -> Mono.empty()).createQuery().copy().count();
        sync.createQuery().count();
        assertEquals(2, calls.get());
        assertEquals(List.of(7L), capture.requests.get(0).parameters());
        assertTrue(capture.requests.get(1).parameters().isEmpty());
        assertEquals(List.of(7L), capture.requests.get(2).parameters());
        assertTrue(capture.requests.get(3).parameters().isEmpty());
    }

    @Test
    void reactivePreparationMustFinishBeforeSqlStarts() {
        Capture capture = new Capture();
        reactor.core.publisher.Sinks.Many<QuerySpec> gate = reactor.core.publisher.Sinks.many().unicast().onBackpressureBuffer();
        AtomicInteger results = new AtomicInteger();
        capture.reactive().withQueryPreparation(spec -> gate.asFlux()).count(QUERY)
                .subscribe(count -> results.incrementAndGet());
        gate.tryEmitNext(QUERY);
        assertTrue(capture.requests.isEmpty());
        gate.tryEmitComplete();
        assertEquals(1, capture.requests.size());
        assertEquals(1, results.get());
    }

    @Test
    void synchronousPreparationDoesNotRunOnAReactorEventThread() {
        Capture capture = new Capture();
        AtomicInteger prepared = new AtomicInteger();
        SyncFormClient client = capture.sync().withQueryPreparation(spec -> {
            prepared.incrementAndGet();
            return spec;
        });
        assertThrows(IllegalStateException.class, () -> Mono.fromCallable(() -> client.count(QUERY))
                .subscribeOn(reactor.core.scheduler.Schedulers.parallel()).block());
        assertEquals(0, prepared.get());
        assertTrue(capture.requests.isEmpty());
    }

    private static Mono<?> read(ReactiveFormClient client, String terminal) {
        return switch (terminal) {
            case "select" -> client.select(QUERY).collectList();
            case "first" -> client.first(QUERY);
            case "count" -> client.count(QUERY);
            case "page" -> client.page(QUERY, PAGE);
            case "keyset" -> client.keysetPage(QUERY, KEYSET);
            case "lock" -> client.lockingRead(LOCK).collectList();
            case "lockKeyset" -> client.lockingRead(LOCK, KEYSET);
            case "governed" -> client.selectGoverned(QUERY, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()).collectList();
            case "entity" -> client.entity(User.class).query().fetch().collectList();
            case "entityCount" -> client.entity(User.class).query().count();
            case "entityPage" -> client.entity(User.class).query().page(1, 2);
            case "repository" -> ReactiveFormRepository.create(client, FORM, User.class).findById(99L);
            default -> ReactiveFormRepository.create(client, FORM, User.class).page(ConditionGroup.and().build(), PAGE);
        };
    }

    private static void read(SyncFormClient client, String terminal) {
        switch (terminal) {
            case "select" -> client.select(QUERY);
            case "first" -> client.first(QUERY);
            case "count" -> client.count(QUERY);
            case "page" -> client.page(QUERY, PAGE);
            case "keyset" -> client.keysetPage(QUERY, KEYSET);
            case "lock" -> client.lockingRead(LOCK);
            case "lockKeyset" -> client.lockingRead(LOCK, KEYSET);
            case "governed" -> client.selectGoverned(QUERY, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());
            case "entity" -> client.entity(User.class).query().fetch();
            case "entityCount" -> client.entity(User.class).query().count();
            case "entityPage" -> client.entity(User.class).query().page(1, 2);
            case "repository" -> SyncFormRepository.create(client, FORM, User.class).findById(99L);
            default -> SyncFormRepository.create(client, FORM, User.class).page(ConditionGroup.and().build(), PAGE);
        }
    }

    @TableName("users")
    record User(@TableId Long id, Long tenant) {}

    private static final class Capture {
        final List<SqlRequest> requests = new ArrayList<>();
        List<DynamicRow> rows(SqlRequest request) {
            requests.add(request);
            return request.sql().startsWith("select count(")
                    ? List.of(DynamicRow.copyOf(Map.of("total", 1L))) : List.of();
        }
        FormDataSqlRenderer renderer() {
            return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        }
        ReactiveFormClient reactive() {
            return ReactiveFormClient.create(new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.defer(() -> Flux.fromIterable(rows(request))); }
                public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            }, renderer());
        }
        SyncFormClient sync() {
            return SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows(request); }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
                public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) { return rows(request); }
                public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) { throw new AssertionError("unexpected write"); }
                public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                        SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { throw new AssertionError("unexpected write"); }
            }, (request, completion) -> { throw new AssertionError("unexpected batch"); }, renderer());
        }
    }
}
