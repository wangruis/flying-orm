package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcResultProgressFactsTest {

    @TestFactory
    Stream<DynamicTest> preservesCountsAcrossResultsBeforeTheLastResultTerminates() {
        return Arrays.stream(Mode.values()).flatMap(mode -> Stream.of(false, true).flatMap(observed ->
                Arrays.stream(Terminal.values()).map(terminal -> DynamicTest.dynamicTest(
                        mode + "/observed=" + observed + "/" + terminal, () -> {
                            List<Result.Segment> last = new ArrayList<>();
                            if (mode.keys) last.add(key());
                            last.add(count(5L));
                            Fixture fixture = new Fixture(List.of(List.of(count(2L)), last));
                            verifyPending(fixture, mode, terminal, observed, 7L);
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> generatedRowsAreFallbackFactsButUpdateCountsRemainAuthoritative() {
        return Stream.of(Mode.KEYS, Mode.PROTECTED_KEYS).flatMap(mode ->
                Stream.of(-1L, 0L, 7L).flatMap(rows -> Arrays.stream(Terminal.values()).map(terminal ->
                        DynamicTest.dynamicTest(mode + "/count=" + rows + "/" + terminal, () -> {
                            List<Result.Segment> segments = new ArrayList<>(List.of(key()));
                            if (rows >= 0L) segments.add(count(rows));
                            verifyPending(new Fixture(List.of(segments)), mode, terminal, true,
                                    rows < 0L ? 1L : rows);
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> noFactsStillMeansZeroOnCancellationOrError() {
        return Arrays.stream(Mode.values()).flatMap(mode -> Stream.of(Terminal.CANCEL, Terminal.ERROR)
                .map(terminal -> DynamicTest.dynamicTest(mode + "/" + terminal, () ->
                        verifyPending(new Fixture(List.of(List.of())), mode, terminal, true, 0L))));
    }

    @TestFactory
    Stream<DynamicTest> overflowFailsWithoutLosingTheLastRepresentableCount() {
        return Arrays.stream(Mode.values()).flatMap(mode -> Stream.of(false, true).map(observed ->
                DynamicTest.dynamicTest(mode + "/observed=" + observed, () -> {
                    Fixture fixture = new Fixture(List.of(List.of(count(Long.MAX_VALUE), count(1L))));
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    fixture.operation(mode, observed).subscribe(ignored -> fail("overflow returned a result"), failure::set);
                    assertNotNull(failure.get());
                    assertEquals(1, fixture.releases.get());
                    if (observed) {
                        assertEquals(1, fixture.events.size());
                        assertEquals(SqlExecutionStatus.ERROR, fixture.events.getFirst().status());
                        assertEquals(Long.MAX_VALUE, fixture.events.getFirst().rows());
                    } else {
                        assertTrue(fixture.events.isEmpty());
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> repeatedSubscriptionsHaveIndependentCounts() {
        return Arrays.stream(Mode.values()).map(mode -> DynamicTest.dynamicTest(mode.name(), () -> {
            Fixture fixture = new Fixture(List.of(mode.keys ? List.of(key(), count(7L)) : List.of(count(7L))));
            fixture.tail.tryEmitEmpty();
            Mono<?> operation = fixture.operation(mode, true);
            for (int index = 0; index < 2; index++) {
                AtomicReference<Object> result = new AtomicReference<>();
                operation.subscribe(result::set, error -> fail(error));
                assertNotNull(result.get());
            }
            assertEquals(List.of(7L, 7L), fixture.events.stream().map(SqlExecutionObservation::rows).toList());
            assertEquals(2, fixture.releases.get());
        }));
    }

    private static void verifyPending(Fixture fixture, Mode mode, Terminal terminal,
                                      boolean observed, long expectedRows) {
        AtomicReference<Object> output = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Disposable subscription = fixture.operation(mode, observed).subscribe(output::set, failure::set);
        assertTrue(fixture.atTail.get(), "all supplied segments must be consumed before termination");
        assertEquals(0, fixture.releases.get());
        assertTrue(fixture.events.isEmpty());
        assertNull(output.get());
        assertNull(failure.get());
        switch (terminal) {
            case CANCEL -> subscription.dispose();
            case ERROR -> fixture.tail.emitError(new IllegalStateException("result tail failed"), Sinks.EmitFailureHandler.FAIL_FAST);
            case COMPLETE -> fixture.tail.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
        }
        assertEquals(1, fixture.releases.get(), "one release per subscription");
        assertEquals(terminal == Terminal.ERROR, failure.get() != null);
        assertEquals(terminal == Terminal.COMPLETE, output.get() != null);
        if (output.get() != null) {
            long rows = output.get() instanceof SqlWriteResult write ? write.affectedRows() : (Long) output.get();
            assertEquals(expectedRows, rows);
            if (mode.keys) assertEquals(1L, ((SqlWriteResult) output.get()).generatedKeys().getFirst().value(0));
        }
        if (observed) {
            assertEquals(1, fixture.events.size(), "one terminal event per subscription");
            assertEquals(terminal.status, fixture.events.getFirst().status());
            assertEquals(expectedRows, fixture.events.getFirst().rows());
        } else {
            assertTrue(fixture.events.isEmpty());
        }
    }

    private enum Mode {
        ORDINARY(false), KEYS(true), PROTECTED(false), PROTECTED_KEYS(true);
        private final boolean keys;
        Mode(boolean keys) { this.keys = keys; }
    }

    private enum Terminal {
        CANCEL(SqlExecutionStatus.CANCELLED), ERROR(SqlExecutionStatus.ERROR), COMPLETE(SqlExecutionStatus.SUCCESS);
        private final SqlExecutionStatus status;
        Terminal(SqlExecutionStatus status) { this.status = status; }
    }

    private static final class Fixture implements R2dbcConnectionAccess {
        private final Sinks.Empty<Void> tail = Sinks.empty();
        private final AtomicBoolean atTail = new AtomicBoolean();
        private final AtomicInteger releases = new AtomicInteger();
        private final List<SqlExecutionObservation> events = new ArrayList<>();
        private final List<List<Result.Segment>> segments;

        private Fixture(List<List<Result.Segment>> segments) { this.segments = segments; }

        private Mono<?> operation(Mode mode, boolean observed) {
            R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(this, RdbDialect.postgresql());
            if (observed) executor = executor.withObserver(events::add);
            SqlRequest request = new SqlRequest("insert into sample(payload) values (?)", List.of("value"));
            return switch (mode) {
                case ORDINARY -> executor.rowsUpdated(request);
                case KEYS -> executor.rowsUpdatedReturningKeys(request, SqlExecutionOptions.safeDefaults());
                case PROTECTED, PROTECTED_KEYS -> executor.protectedWrite(new ProtectedWriteWork(
                        ProtectedWriteWork.Kind.INSERT, request, null, List.of("id"),
                        mode.keys ? Map.of() : Map.of("id", 1L), "id = ?",
                        "delete from sample_tokens where id = ? and field = ?",
                        "insert into sample_tokens(id, field, token) values (?, ?, ?)",
                        List.of(new ProtectedWriteWork.FieldTokens("payload", List.of(new byte[]{1})))),
                        SqlExecutionOptions.safeDefaults());
            };
        }

        @Override
        public Publisher<? extends Connection> getConnection(SqlRequest request) {
            return Mono.just(proxy(Connection.class, (self, method, args) -> {
                if (method.getName().equals("createStatement")) return statement((String) args[0]);
                throw new AssertionError("unexpected connection method: " + method.getName());
            }));
        }

        @Override
        public Publisher<Void> releaseConnection(SignalType signal, Connection connection, SqlRequest request) {
            return Mono.fromRunnable(releases::incrementAndGet);
        }

        private Statement statement(String sql) {
            return proxy(Statement.class, (self, method, args) -> switch (method.getName()) {
                case "execute" -> {
                    if (sql.startsWith("insert into sample_tokens")) yield Flux.just(result(List.of(count(1L)), false));
                    yield Flux.range(0, segments.size()).map(index -> result(segments.get(index), index == segments.size() - 1));
                }
                case "returnGeneratedValues", "fetchSize", "bind", "bindNull" -> self;
                default -> throw new AssertionError("unexpected statement method: " + method.getName());
            });
        }

        private Result result(List<Result.Segment> values, boolean pending) {
            AtomicBoolean consumed = new AtomicBoolean();
            Flux<Result.Segment> source = Flux.defer(() -> {
                assertTrue(consumed.compareAndSet(false, true), "Result must only be consumed once");
                return Flux.fromIterable(values);
            });
            if (pending) source = source.concatWith(tail.asMono().doOnSubscribe(ignored -> atTail.set(true)).thenMany(Flux.empty()));
            Flux<Result.Segment> stream = source;
            return proxy(Result.class, (self, method, args) -> switch (method.getName()) {
                case "getRowsUpdated" -> stream.ofType(Result.UpdateCount.class).map(Result.UpdateCount::value);
                case "flatMap" -> {
                    @SuppressWarnings("unchecked")
                    Function<Result.Segment, Publisher<?>> mapper = (Function<Result.Segment, Publisher<?>>) args[0];
                    yield stream.concatMap(segment -> Flux.from(mapper.apply(segment)), 1);
                }
                default -> throw new AssertionError("unexpected result method: " + method.getName());
            });
        }
    }

    private static Result.UpdateCount count(long rows) { return () -> rows; }

    private static Result.RowSegment key() {
        ColumnMetadata column = proxy(ColumnMetadata.class, (self, method, args) -> switch (method.getName()) {
            case "getName" -> "id";
            case "getType" -> R2dbcType.BIGINT;
            case "getJavaType" -> Long.class;
            default -> throw new AssertionError(method.getName());
        });
        RowMetadata metadata = proxy(RowMetadata.class, (self, method, args) -> switch (method.getName()) {
            case "getColumnMetadatas" -> List.of(column);
            case "getColumnMetadata" -> column;
            default -> throw new AssertionError(method.getName());
        });
        Row row = proxy(Row.class, (self, method, args) -> switch (method.getName()) {
            case "getMetadata" -> metadata;
            case "get" -> 1L;
            default -> throw new AssertionError(method.getName());
        });
        return () -> row;
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
