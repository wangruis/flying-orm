package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.Feature;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.repository.BatchEvidenceFixtures;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.support.ListQueryFixture;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import org.reactivestreams.Publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.*;

class FormWriteInterceptorTest {

    @Test
    void syncConditionalWritesUseTableInterceptor() {
        List<String> events = new ArrayList<>();
        SyncWriteInterceptor interceptor = new SyncWriteInterceptor() {
            @Override
            public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                events.add("before:" + spec.operation());
                R result = next.apply(spec);
                events.add("after:" + result);
                return result;
            }
        };
        DynamicForm form = form(interceptor);
        SyncFormClient client = SyncFormClient.create(new ListQueryFixture() {
            @Override public List<DynamicRow> query(SqlRequest request) { return List.of(); }
            @Override public long rowsUpdated(SqlRequest request) { events.add("sql"); return 2; }
        }, (request, completed) -> { throw new AssertionError("not a batch"); }, renderer());

        assertEquals(2, client.update(WriteSpec.update(form, Map.of("name", "Ada"), where())));
        assertEquals(2, client.delete(WriteSpec.delete(form, where())));
        assertEquals(List.of("before:UPDATE", "sql", "after:2", "before:DELETE", "sql", "after:2"), events);
    }

    @Test
    void reactiveConditionalWritesInterceptEachSubscription() {
        List<String> events = new ArrayList<>();
        ReactiveWriteInterceptor interceptor = new ReactiveWriteInterceptor() {
            @Override
            public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                events.add("before:" + spec.operation());
                return next.apply(spec).doOnNext(result -> events.add("after:" + result));
            }
        };
        DynamicForm form = form(interceptor);
        ReactiveFormClient client = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> { events.add("sql"); return 2L; });
            }
        }, renderer());
        Mono<Long> update = client.update(WriteSpec.update(form, Map.of("name", "Ada"), where()));
        assertEquals(List.of(), events);
        assertEquals(2L, update.block());
        assertEquals(2L, update.block());
        assertEquals(2L, client.delete(WriteSpec.delete(form, where())).block());
        assertEquals(List.of("before:UPDATE", "sql", "after:2", "before:UPDATE", "sql", "after:2",
                "before:DELETE", "sql", "after:2"), events);
    }

    @Test
    void allSingleWriteTerminalsPreserveResultsAndPhysicalDeleteChoice() {
        Fixture fixture = new Fixture();
        List<Object> results = new ArrayList<>();
        DynamicForm form = form(new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                R result = next.apply(spec);
                results.add(result);
                return result;
            }
        }, new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                return next.apply(spec).doOnNext(results::add);
            }
        });
        WriteSpec insert = WriteSpec.insert(form, Map.of("id", 7L, "name", "Ada"));
        assertEquals(2, fixture.sync().insert(insert));
        assertSame(fixture.keys, fixture.sync().insertReturningKeys(insert));
        assertEquals(2, fixture.sync().physicalDelete(WriteSpec.delete(form, where())));
        assertTrue(fixture.requests.getLast().sql().startsWith("delete "));
        assertEquals(2L, fixture.reactive().insert(insert).block());
        assertSame(fixture.keys, fixture.reactive().insertReturningKeys(insert).block());
        assertEquals(2L, fixture.reactive().physicalDelete(WriteSpec.delete(form, where())).block());
        assertTrue(fixture.requests.getLast().sql().startsWith("delete "));
        assertEquals(List.of(2L, fixture.keys, 2L, 2L, fixture.keys, 2L), results);
        DynamicForm softDelete = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .logicDelete("deleted", 0, 1).features(form.features()).build();
        fixture.sync().physicalDelete(WriteSpec.delete(softDelete, where()));
        assertTrue(fixture.requests.getLast().sql().startsWith("delete "));
        fixture.reactive().physicalDelete(WriteSpec.delete(softDelete, where())).block();
        assertTrue(fixture.requests.getLast().sql().startsWith("delete "));
    }

    @Test
    void replacementSpecReachesPlannerWithoutChangingOriginal() {
        Fixture fixture = new Fixture();
        DataScope extra = DataScope.where(ConditionGroup.and().where("name", "=", "allowed").build());
        DynamicForm form = form(new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                return next.apply(spec.withScope(extra));
            }
        }, new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                return next.apply(spec.withScope(extra));
            }
        });
        WriteSpec original = WriteSpec.update(form, Map.of("name", "next"), where());
        fixture.sync().update(original);
        fixture.reactive().update(original).block();
        assertSame(DataScope.none(), original.scope());
        for (SqlRequest request : fixture.requests) {
            assertEquals(List.of("next", 7L, "allowed"), request.parameters());
        }
    }

    @Test
    void featureInheritanceAndLocalOverrideUseOnlyEffectiveInterceptor() {
        Fixture fixture = new Fixture();
        List<String> calls = new ArrayList<>();
        FeatureRegistry parent = FeatureRegistry.builder()
                .add(syncRecorder(calls, "parent-sync")).add(reactiveRecorder(calls, "parent-reactive")).build();
        DynamicForm inherited = form(FeatureRegistry.builder().parent(parent).build());
        DynamicForm overridden = form(FeatureRegistry.builder().parent(parent)
                .add(syncRecorder(calls, "local-sync")).add(reactiveRecorder(calls, "local-reactive")).build());
        for (DynamicForm form : List.of(inherited, overridden)) {
            fixture.sync().delete(WriteSpec.delete(form, where()));
            fixture.reactive().delete(WriteSpec.delete(form, where())).block();
        }
        assertEquals(List.of("parent-sync", "parent-reactive", "local-sync", "local-reactive"), calls);
    }

    @Test
    void asyncBeforeAndAfterAreComposedAndCancellationStopsUnstartedSql() {
        Fixture fixture = new Fixture();
        Sinks.Empty<Void> before = Sinks.empty();
        Sinks.Empty<Void> after = Sinks.empty();
        DynamicForm form = form(new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                return before.asMono().then(Mono.defer(() -> next.apply(spec)))
                        .flatMap(result -> after.asMono().thenReturn(result));
            }
        });
        Mono<Long> operation = fixture.reactive().delete(WriteSpec.delete(form, where()));
        operation.toFuture().cancel(true);
        assertEquals(0, fixture.requests.size());
        CompletableFuture<Long> result = operation.toFuture();
        assertEquals(0, fixture.requests.size());
        before.tryEmitEmpty();
        assertEquals(1, fixture.requests.size());
        assertFalse(result.isDone());
        after.tryEmitEmpty();
        assertEquals(2L, result.join());
    }

    @Test
    void failuresAreNotRecoveredOrRewritten() {
        Fixture fixture = new Fixture();
        IllegalStateException failure = new IllegalStateException("application hook");
        DynamicForm denied = form(new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) { throw failure; }
        }, new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) { throw failure; }
        });
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.sync().delete(WriteSpec.delete(denied, where()))));
        Mono<Long> deferred = assertDoesNotThrow(() -> fixture.reactive().delete(WriteSpec.delete(denied, where())));
        assertSame(failure, assertThrows(IllegalStateException.class, deferred::block));
        assertEquals(0, fixture.requests.size());

        fixture.failure = failure;
        DynamicForm observing = form(new SyncWriteInterceptor() {}, new ReactiveWriteInterceptor() {});
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.sync().delete(WriteSpec.delete(observing, where()))));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.reactive().delete(WriteSpec.delete(observing, where())).block()));
        assertEquals(2, fixture.requests.size());
    }

    @Test
    void afterFailureDoesNotRepeatTheAlreadyExecutedWrite() {
        Fixture fixture = new Fixture();
        IllegalStateException failure = new IllegalStateException("after SQL");
        DynamicForm form = form(new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                assertEquals(2L, next.apply(spec));
                throw failure;
            }
        }, new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                return next.apply(spec).flatMap(result -> {
                    assertEquals(2L, result);
                    return Mono.error(failure);
                });
            }
        });
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.sync().delete(WriteSpec.delete(form, where()))));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.reactive().delete(WriteSpec.delete(form, where())).block()));
        assertEquals(2, fixture.requests.size());
    }

    @Test
    void cancellationReachesTheOriginalExecutorPublisher() {
        AtomicInteger cancelled = new AtomicInteger();
        ReactiveFormClient client = ReactiveFormClient.create(new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.<Long>never().doOnCancel(cancelled::incrementAndGet);
            }
        }, renderer());
        DynamicForm form = form(new ReactiveWriteInterceptor() {});
        CompletableFuture<Long> running = client.delete(WriteSpec.delete(form, where())).toFuture();
        assertFalse(running.isDone());
        running.cancel(true);
        assertEquals(1, cancelled.get());
    }

    @Test
    void allBatchOperationsKeepLazyRowsTypedEvidenceAndRowCompletion() {
        Fixture fixture = new Fixture();
        List<String> operations = new ArrayList<>();
        List<BatchExecutionEvidence> results = new ArrayList<>();
        DynamicForm form = form(new SyncWriteInterceptor() {
            @Override public BatchExecutionEvidence batch(
                    BatchSpec spec, Function<BatchSpec, BatchExecutionEvidence> next) {
                operations.add(spec.operation().name());
                BatchExecutionEvidence result = next.apply(spec);
                results.add(result);
                return result;
            }
        }, new ReactiveWriteInterceptor() {
            @Override public Mono<BatchExecutionEvidence> batch(
                    BatchSpec spec, Function<BatchSpec, Mono<BatchExecutionEvidence>> next) {
                operations.add(spec.operation().name());
                return next.apply(spec).doOnNext(results::add);
            }
        });
        AtomicInteger subscriptions = new AtomicInteger();
        Flux<Map<String, Object>> rows = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.just(Map.of("id", 7L, "name", "Ada"), Map.of("id", 8L, "name", "Lin"));
        });
        Flux<BatchUpdate> updates = rows.map(row -> new BatchUpdate(Map.of("name", row.get("name")), where()));
        for (BatchSpec spec : List.of(BatchSpec.insert(form, rows), BatchSpec.upsert(form, rows),
                BatchSpec.update(form, updates))) {
            List<Long> completed = new ArrayList<>();
            int previous = subscriptions.get();
            Mono<BatchExecutionEvidence> pending = fixture.reactive().writeBatch(spec,
                    offset -> Mono.fromRunnable(() -> completed.add(offset)));
            assertEquals(previous, subscriptions.get());
            BatchExecutionEvidence reactiveResult = pending.block();
            assertSame(reactiveResult, results.getLast());
            assertEquals(2, reactiveResult.successfulCount());
            assertEquals(List.of(0L, 1L), completed);
            completed.clear();
            BatchExecutionEvidence syncResult = fixture.sync().writeBatch(spec, completed::add);
            assertSame(syncResult, results.getLast());
            assertEquals(List.of(0L, 1L), completed);
        }
        assertEquals(6, subscriptions.get());
        assertEquals(List.of("INSERT", "INSERT", "UPSERT", "UPSERT", "UPDATE", "UPDATE"), operations);
    }

    @Test
    void unrelatedFeaturesDoNotChangeOrdinaryWrites() {
        Fixture fixture = new Fixture();
        for (DynamicForm form : List.of(form(), form(() -> "unrelated"))) {
            assertEquals(2, fixture.sync().update(WriteSpec.update(form, Map.of("name", "Ada"), where())));
            assertEquals(2L, fixture.reactive().update(WriteSpec.update(form, Map.of("name", "Ada"), where())).block());
        }
        assertEquals(4, fixture.requests.size());
    }

    private static SyncWriteInterceptor syncRecorder(List<String> calls, String label) {
        return new SyncWriteInterceptor() {
            @Override public <R> R write(WriteSpec spec, Function<WriteSpec, R> next) {
                calls.add(label);
                return next.apply(spec);
            }
        };
    }

    private static ReactiveWriteInterceptor reactiveRecorder(List<String> calls, String label) {
        return new ReactiveWriteInterceptor() {
            @Override public <R> Mono<R> write(WriteSpec spec, Function<WriteSpec, Mono<R>> next) {
                calls.add(label);
                return next.apply(spec);
            }
        };
    }

    private static final class Fixture {
        private final List<SqlRequest> requests = new ArrayList<>();
        private final SqlWriteResult keys = new SqlWriteResult(2, List.of(DynamicRow.copyOf(Map.of("id", 7L))));
        private RuntimeException failure;

        private long write(SqlRequest request) {
            requests.add(request);
            if (failure != null) throw failure;
            return 2;
        }

        private SyncFormClient sync() {
            return SyncFormClient.create(new ListQueryFixture() {
                @Override public List<DynamicRow> query(SqlRequest request) { return List.of(); }
                @Override public long rowsUpdated(SqlRequest request) { return write(request); }
                @Override public SqlWriteResult rowsUpdatedReturningKeys(
                        SqlRequest request, SqlExecutionOptions options, String column) {
                    write(request);
                    return keys;
                }
            }, (request, completed) -> {
                List<Object[]> rows = Flux.from(request.rows()).collectList().block();
                for (int index = 0; index < rows.size(); index++) {
                    if (completed != null) completed.accept(index);
                }
                return BatchEvidenceFixtures.successful(rows.size());
            }, renderer());
        }

        private ReactiveFormClient reactive() {
            return ReactiveFormClient.create(new ReactiveSqlExecutor() {
                @Override public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
                @Override public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.fromSupplier(() -> write(request)); }
                @Override public Mono<SqlWriteResult> rowsUpdatedReturningKeys(
                        SqlRequest request, SqlExecutionOptions options, String column) {
                    return Mono.fromSupplier(() -> { write(request); return keys; });
                }
                @Override public Mono<BatchExecutionEvidence> writeBatch(
                        BatchWriteRequest request, LongFunction<? extends Publisher<Void>> completed) {
                    return Flux.from(request.rows()).index().concatMap(row -> completed == null
                            ? Mono.just(row) : Mono.from(completed.apply(row.getT1())).thenReturn(row))
                            .count().map(BatchEvidenceFixtures::successful);
                }
            }, renderer());
        }
    }

    private static ConditionGroup where() {
        return ConditionGroup.and().where("id", "=", 7L).build();
    }

    private static DynamicForm form(Feature... features) {
        FeatureRegistry.Builder registry = FeatureRegistry.builder();
        for (Feature feature : features) registry.add(feature);
        return form(registry.build());
    }

    private static DynamicForm form(FeatureRegistry registry) {
        return DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR(128)"))
                .features(registry).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
