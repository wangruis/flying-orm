package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.EntityPostWriteException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RepositoryExecutionLifecycleTest {

    @Test
    void syncPreOnlyListenerChangesTheSqlInputExactlyOnce() {
        List<String> events = new ArrayList<>();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        SyncFormRepository<Person> repository = sync(events, RepositoryExecutionLifecycleTest::keys, request::set)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.PRE_PERSIST) {
                        events.add("PRE_ONLY");
                        event.entity().setName("from-pre");
                    }
                    return Mono.empty();
                });

        assertEquals(1L, repository.insert(new Person()));

        assertEquals(List.of("PRE_ONLY", "SQL_KEYS"), events);
        assertEquals(List.of("from-pre"), request.get().parameters());
    }

    @Test
    void syncWritesComposeCallbacksWithoutTransactionLookup() {
        List<String> events = new ArrayList<>();
        SyncFormRepository<Person> repository = sync(events).withListener(event -> {
            events.add(event.phase().name());
            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                assertEquals(7L, event.entity().id);
            }
            return Mono.empty();
        });
        Person person = new Person();
        assertEquals(1L, repository.insert(person));
        assertEquals(1L, repository.update(person, where()));
        assertEquals(1L, repository.delete(person, where()));
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST", "PRE_UPDATE", "SQL",
                "POST_UPDATE", "PRE_REMOVE", "SQL", "POST_REMOVE"), events);
    }

    @Test
    void reactiveWritesComposeCallbacksWithoutTransactionLookup() {
        List<String> events = new ArrayList<>();
        ReactiveFormRepository<Person> repository = reactive(events).withListener(event -> Mono.fromRunnable(() -> {
            events.add(event.phase().name());
            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                assertEquals(7L, event.entity().id);
            }
        }));
        Person person = new Person();
        Mono<Long> insert = repository.insert(person);
        assertEquals(List.of(), events);
        assertNull(person.id);
        assertEquals(1L, insert.block());
        assertEquals(1L, repository.update(person, where()).block());
        assertEquals(1L, repository.delete(person, where()).block());
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST", "PRE_UPDATE", "SQL",
                "POST_UPDATE", "PRE_REMOVE", "SQL", "POST_REMOVE"), events);
    }

    @Test
    void syncGeneratedKeysWithoutListenersNeverLookUpTransactions() {
        List<String> events = new ArrayList<>();
        Person person = new Person();
        assertEquals(1L, sync(events).insert(person));
        assertEquals(7L, person.id);
        assertEquals(List.of("SQL_KEYS"), events);
    }

    @Test
    void reactiveGeneratedKeysWithoutListenersNeverLookUpTransactions() {
        List<String> events = new ArrayList<>();
        Person person = new Person();
        assertEquals(1L, reactive(events).insert(person).block());
        assertEquals(7L, person.id);
        assertEquals(List.of("SQL_KEYS"), events);
    }

    @Test
    void syncKeyReadFailureRetainsCountAndCauseWithoutPostCallback() {
        GeneratedKeyReadException read = new GeneratedKeyReadException(3L, new IllegalStateException("private-key"));
        assertKeyFailure(false, () -> { throw read; }, GeneratedKeyResolutionException.Phase.READ, 3L, read);
    }

    @Test
    void reactiveKeyReadFailureRetainsCountAndCauseWithoutPostCallback() {
        GeneratedKeyReadException read = new GeneratedKeyReadException(3L, new IllegalStateException("private-key"));
        assertKeyFailure(true, () -> { throw read; }, GeneratedKeyResolutionException.Phase.READ, 3L, read);
    }

    @Test
    void syncKeyAssignmentFailureRetainsCountAndLeavesEntityUnchanged() {
        assertKeyFailure(false, () -> new SqlWriteResult(11L, List.of()),
                GeneratedKeyResolutionException.Phase.ASSIGN, 11L, null);
    }

    @Test
    void reactiveKeyAssignmentFailureRetainsCountAndLeavesEntityUnchanged() {
        assertKeyFailure(true, () -> new SqlWriteResult(11L, List.of()),
                GeneratedKeyResolutionException.Phase.ASSIGN, 11L, null);
    }

    private static void assertKeyFailure(boolean reactive, Supplier<SqlWriteResult> keys,
                                         GeneratedKeyResolutionException.Phase phase, long rows, Throwable cause) {
        List<String> events = new ArrayList<>();
        Person person = new Person();
        com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> listener = event -> {
            events.add(event.phase().name());
            return Mono.empty();
        };
        GeneratedKeyResolutionException failure = assertThrows(GeneratedKeyResolutionException.class, () -> {
            if (reactive) {
                reactive(events, () -> Mono.fromSupplier(keys)).withListener(listener).insert(person).block();
            } else {
                sync(events, keys).withListener(listener).insert(person);
            }
        });
        assertEquals(phase, failure.phase());
        assertEquals(rows, failure.affectedRows());
        if (cause != null) {
            assertSame(cause, failure.getCause());
        } else {
            assertInstanceOf(com.flying.orm.rdb.mapping.MappingException.class, failure.getCause());
        }
        assertNull(person.id);
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS"), events);
        assertFalse(failure.getMessage().contains("private-key"));
        assertEquals(phase.name(), failure.toErrorReport().resource());
    }

    @Test
    void postFailuresRetainExecutionFactsAndCause() {
        for (boolean reactive : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            Person person = new Person();
            IllegalStateException cause = new IllegalStateException("private-entity");
            com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> listener = event -> {
                events.add(event.phase().name());
                return event.phase() == EntityLifecyclePhase.POST_PERSIST ? Mono.error(cause) : Mono.empty();
            };
            EntityPostWriteException failure = assertThrows(EntityPostWriteException.class, () -> {
                if (reactive) {
                    reactive(events).withListener(listener).insert(person).block();
                } else {
                    sync(events).withListener(listener).insert(person);
                }
            });
            assertEquals(EntityLifecyclePhase.POST_PERSIST, failure.phase());
            assertEquals(1L, failure.result());
            assertSame(cause, failure.getCause());
            assertEquals(7L, person.id);
            assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST"), events);
            assertFalse(failure.getMessage().contains("private-entity"));
            assertEquals("POST_WRITE_CALLBACK_FAILED", failure.toErrorReport().code());
        }
    }

    @Test
    void preFailurePreventsSqlAndPost() {
        for (boolean reactive : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            IllegalStateException cause = new IllegalStateException("pre failed");
            com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> listener = event -> {
                events.add(event.phase().name());
                return Mono.error(cause);
            };
            assertSame(cause, assertThrows(IllegalStateException.class, () -> {
                if (reactive) {
                    reactive(events).withListener(listener).insert(new Person()).block();
                } else {
                    sync(events).withListener(listener).insert(new Person());
                }
            }));
            assertEquals(List.of("PRE_PERSIST"), events);
        }
    }

    @Test
    void reactiveCancelDoesNotAssignKeysOrFirePost() {
        List<String> events = new ArrayList<>();
        Person person = new Person();
        Mono<Long> insert = reactive(events, Mono::never).withListener(event -> {
            events.add(event.phase().name());
            return Mono.empty();
        }).insert(person);
        reactor.core.Disposable subscription = insert.subscribe();
        subscription.dispose();
        assertNull(person.id);
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS"), events);
    }

    @Test
    void reactiveRepeatSubscriptionExecutesTheWholeLifecycleAgain() {
        List<String> events = new ArrayList<>();
        Person person = new Person();
        Mono<Long> insert = reactive(events).withListener(event -> {
            events.add(event.phase().name());
            return Mono.empty();
        }).insert(person);
        assertEquals(1L, insert.block());
        person.id = null;
        assertEquals(1L, insert.block());
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST", "PRE_PERSIST", "SQL_KEYS",
                "POST_PERSIST"), events);
    }

    @Test
    void sqlFailureIsNotReclassifiedAsAKeyOrPostFailure() {
        for (boolean reactive : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            Person person = new Person();
            IllegalArgumentException cause = new IllegalArgumentException("write failed");
            Supplier<SqlWriteResult> operation = () -> { throw cause; };
            com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> listener = event -> {
                events.add(event.phase().name());
                return Mono.empty();
            };
            assertSame(cause, assertThrows(IllegalArgumentException.class, () -> {
                if (reactive) {
                    reactive(events, () -> Mono.fromSupplier(operation)).withListener(listener).insert(person).block();
                } else {
                    sync(events, operation).withListener(listener).insert(person);
                }
            }));
            assertNull(person.id);
            assertEquals(List.of("PRE_PERSIST", "SQL_KEYS"), events);
        }
    }

    @Test
    void postCallbackPreservesVirtualMachineErrorPriority() {
        for (boolean reactive : List.of(false, true)) {
            List<String> events = new ArrayList<>();
            Person person = new Person();
            SyntheticVirtualMachineError fatal = new SyntheticVirtualMachineError();
            com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> listener = event -> {
                events.add(event.phase().name());
                return event.phase() == EntityLifecyclePhase.POST_PERSIST
                        ? Mono.error(new CompletionException(fatal)) : Mono.empty();
            };
            if (reactive) {
                assertSame(fatal, reactive(events).withListener(listener).insert(person)
                        .materialize().block().getThrowable());
            } else {
                assertSame(fatal, assertThrows(SyntheticVirtualMachineError.class,
                        () -> sync(events).withListener(listener).insert(person)));
            }
            assertEquals(7L, person.id);
            assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST"), events);
        }
    }

    @Test
    void generatedKeyReadPreservesDirectVirtualMachineErrors() {
        SyntheticVirtualMachineError fatal = new SyntheticVirtualMachineError();
        List<String> events = new ArrayList<>();
        assertSame(fatal, assertThrows(SyntheticVirtualMachineError.class,
                () -> sync(events, () -> { throw fatal; }).insert(new Person())));
        assertEquals(List.of("SQL_KEYS"), events);
        events.clear();
        assertSame(fatal, reactive(events, () -> Mono.error(fatal)).insert(new Person())
                .materialize().block().getThrowable());
        assertEquals(List.of("SQL_KEYS"), events);
    }

    @Test
    void generatedKeyReadPreservesWrappedVirtualMachineErrors() {
        SyntheticVirtualMachineError fatal = new SyntheticVirtualMachineError();
        GeneratedKeyReadException read = new GeneratedKeyReadException(3L, new CompletionException(fatal));
        assertSame(fatal, assertThrows(SyntheticVirtualMachineError.class,
                () -> sync(new ArrayList<>(), () -> { throw read; }).insert(new Person())));
        assertSame(fatal, reactive(new ArrayList<>(), () -> Mono.error(read)).insert(new Person())
                .materialize().block().getThrowable());
    }

    @Test
    void reactiveWaitsForEachCallbackCompletion() {
        List<String> events = new ArrayList<>();
        reactor.core.publisher.Sinks.Empty<Void> pre = reactor.core.publisher.Sinks.empty();
        reactor.core.publisher.Sinks.Empty<Void> post = reactor.core.publisher.Sinks.empty();
        List<Long> result = new ArrayList<>();
        reactor.core.Disposable subscription = reactive(events).withListener(event -> {
            events.add(event.phase().name());
            return event.phase() == EntityLifecyclePhase.PRE_PERSIST ? pre.asMono() : post.asMono();
        }).insert(new Person()).subscribe(result::add);
        assertEquals(List.of("PRE_PERSIST"), events);
        assertEquals(List.of(), result);
        assertEquals(reactor.core.publisher.Sinks.EmitResult.OK, pre.tryEmitEmpty());
        assertEquals(List.of("PRE_PERSIST", "SQL_KEYS", "POST_PERSIST"), events);
        assertEquals(List.of(), result);
        assertEquals(reactor.core.publisher.Sinks.EmitResult.OK, post.tryEmitEmpty());
        assertEquals(List.of(1L), result);
        subscription.dispose();
    }

    private static SyncFormRepository<Person> sync(List<String> events) {
        return sync(events, RepositoryExecutionLifecycleTest::keys);
    }

    private static SyncFormRepository<Person> sync(List<String> events, Supplier<SqlWriteResult> keys) {
        return sync(events, keys, ignored -> { });
    }

    private static SyncFormRepository<Person> sync(List<String> events, Supplier<SqlWriteResult> keys,
            Consumer<com.flying.orm.core.sql.render.SqlRequest> request) {
        SyncSqlExecutor executor = (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{SyncSqlExecutor.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("currentTransaction")) {
                        events.add("TRANSACTION_LOOKUP");
                        return Optional.empty();
                    }
                    if (method.getName().equals("rowsUpdatedReturningKeys")) {
                        request.accept((com.flying.orm.core.sql.render.SqlRequest) arguments[0]);
                        events.add("SQL_KEYS");
                        return keys.get();
                    }
                    if (method.getName().equals("rowsUpdated")) {
                        events.add("SQL");
                        return 1L;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        SyncBatchExecutor batches = (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> { throw new AssertionError("unexpected batch"); });
        SyncFormClient client = SyncFormClient.create(executor, batches, renderer());
        return SyncFormRepository.create(client, client.entityModels().metadata(Person.class).toDynamicForm(),
                Person.class);
    }

    private static ReactiveFormRepository<Person> reactive(List<String> events) {
        return reactive(events, () -> Mono.fromSupplier(RepositoryExecutionLifecycleTest::keys));
    }

    private static ReactiveFormRepository<Person> reactive(List<String> events, Supplier<Mono<SqlWriteResult>> keys) {
        ReactiveSqlExecutor executor = (ReactiveSqlExecutor) Proxy.newProxyInstance(
                ReactiveSqlExecutor.class.getClassLoader(), new Class<?>[]{ReactiveSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("currentTransaction")) {
                        events.add("TRANSACTION_LOOKUP");
                        return Mono.empty();
                    }
                    if (method.getName().equals("rowsUpdatedReturningKeys")) {
                        return Mono.defer(() -> { events.add("SQL_KEYS"); return keys.get(); });
                    }
                    if (method.getName().equals("rowsUpdated")) {
                        return Mono.fromSupplier(() -> { events.add("SQL"); return 1L; });
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        return ReactiveFormRepository.create(client, client.entityModels().metadata(Person.class).toDynamicForm(),
                Person.class);
    }

    private static SqlWriteResult keys() {
        return new SqlWriteResult(1L, List.of(DynamicRow.copyOf(Map.of("id", 7L))));
    }

    private static ConditionGroup where() {
        return ConditionGroup.and().where("id", "=", 7L).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    @TableName("people")
    private static final class Person {
        @TableId(type = IdType.AUTO)
        private Long id;
        private String name = "Ada";

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
        private static final long serialVersionUID = 1L;
    }
}
