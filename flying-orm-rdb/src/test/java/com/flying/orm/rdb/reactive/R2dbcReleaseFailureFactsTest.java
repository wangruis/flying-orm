package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcReleaseFailureFactsTest {

    @Test
    void queryResourceBoundaryUnwrapsTheDriverReleaseFailure() {
        Fixture fixture = new Fixture();
        R2dbcExecutionSession session = new R2dbcExecutionSession(
                fixture, R2dbcBindMarkers.from(RdbDialect.h2()), ignored -> { }, null);

        RuntimeException failure = assertThrows(RuntimeException.class, () -> session.withPreparedStatement(
                fixture.request,
                List.of(),
                SqlExecutionOptions.safeDefaults(),
                SqlExecutionOperation.QUERY,
                (statement, largeObjects) -> Flux.just(1),
                Function.identity()).blockLast(Duration.ofSeconds(2)));

        assertSame(fixture.releaseFailure, failure);
    }

    @Test
    void releaseFailureKeepsConfirmedRowsForOrdinaryAndGeneratedKeyWrites() {
        Fixture fixture = new Fixture();
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        assertInstanceOf(RdbException.class, assertThrows(RuntimeException.class,
                () -> executor.rowsUpdated(fixture.request).block(Duration.ofSeconds(2))));
        assertInstanceOf(RdbException.class, assertThrows(RuntimeException.class,
                () -> executor.rowsUpdatedReturningKeys(
                        fixture.request, SqlExecutionOptions.safeDefaults()).block(Duration.ofSeconds(2))));

        assertEquals(List.of(SqlExecutionStatus.ERROR, SqlExecutionStatus.ERROR),
                events.stream().map(SqlExecutionObservation::status).toList());
        assertEquals(List.of(7L, 7L),
                events.stream().map(SqlExecutionObservation::rows).toList());
    }

    @Test
    void releaseFailureKeepsConfirmedRowsForProtectedWrites() {
        Fixture fixture = new Fixture();
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);
        SqlRequest write = new SqlRequest("insert into sample(id) values (?)", List.of(1L));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT, write, null,
                List.of("id"), java.util.Map.of("id", 1L), "id = ?",
                "delete from sample_tokens where id = ? and field = ?",
                "insert into sample_tokens(id, field, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("payload", List.of(new byte[]{1}))));

        assertInstanceOf(RdbException.class, assertThrows(RuntimeException.class,
                () -> executor.protectedWrite(work, SqlExecutionOptions.safeDefaults())
                        .block(Duration.ofSeconds(2))));

        assertEquals(1, events.size());
        assertEquals(SqlExecutionStatus.ERROR, events.getFirst().status());
        assertEquals(7L, events.getFirst().rows());
    }

    @Test
    void cancellationDuringPendingReleaseKeepsConfirmedRowsForOrdinaryWrite() {
        Fixture fixture = new Fixture(true);
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        Disposable subscription = executor.rowsUpdated(fixture.request).subscribe();
        assertEquals(1, fixture.releaseCalls.get(), "release must be pending after the driver confirms the write");
        subscription.dispose();

        assertCancelledWriteFacts(fixture, events);
    }

    @Test
    void cancellationDuringPendingReleaseKeepsConfirmedRowsForGeneratedKeyWrite() {
        Fixture fixture = new Fixture(true);
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        Disposable subscription = executor.rowsUpdatedReturningKeys(
                fixture.request, SqlExecutionOptions.safeDefaults()).subscribe();
        assertEquals(1, fixture.releaseCalls.get(), "release must be pending after the driver confirms the write");
        subscription.dispose();

        assertCancelledWriteFacts(fixture, events);
    }

    @Test
    void cancellationDuringPendingReleaseKeepsConfirmedRowsForProtectedWrite() {
        Fixture fixture = new Fixture(true);
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        Disposable subscription = executor.protectedWrite(
                protectedWork(), SqlExecutionOptions.safeDefaults()).subscribe();
        assertEquals(1, fixture.releaseCalls.get(), "release must be pending after the driver confirms the write");
        subscription.dispose();

        assertCancelledWriteFacts(fixture, events);
    }

    private static void assertCancelledWriteFacts(Fixture fixture,
                                                  List<SqlExecutionObservation> events) {
        assertEquals(1, fixture.releaseCalls.get(), "cancellation must not release the connection twice");
        assertEquals(1, events.size(), "one subscription must publish one SQL terminal event");
        assertEquals(SqlExecutionStatus.CANCELLED, events.getFirst().status());
        assertEquals(7L, events.getFirst().rows());
    }

    private static ProtectedWriteWork protectedWork() {
        SqlRequest write = new SqlRequest("insert into sample(id) values (?)", List.of(1L));
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT, write, null,
                List.of("id"), java.util.Map.of("id", 1L), "id = ?",
                "delete from sample_tokens where id = ? and field = ?",
                "insert into sample_tokens(id, field, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("payload", List.of(new byte[]{1}))));
    }

    private static final class Fixture implements R2dbcConnectionAccess {
        private final SqlRequest request = new SqlRequest("update sample set active = true", List.of());
        private final R2dbcNonTransientResourceException releaseFailure =
                new R2dbcNonTransientResourceException("release failed", "08006", 0);
        private final boolean pendingRelease;
        private final AtomicInteger releaseCalls = new AtomicInteger();
        private final Connection connection = proxy(Connection.class, (ignored, method, arguments) ->
                switch (method.getName()) {
                    case "createStatement" -> statement((String) arguments[0]);
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        private Fixture() {
            this(false);
        }

        private Fixture(boolean pendingRelease) {
            this.pendingRelease = pendingRelease;
        }

        @Override
        public Publisher<? extends Connection> getConnection(SqlRequest ignored) {
            return Mono.just(connection);
        }

        @Override
        public Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                                                 Connection ignored,
                                                 SqlRequest request) {
            releaseCalls.incrementAndGet();
            return pendingRelease ? Mono.never() : Mono.error(releaseFailure);
        }

        private Statement statement(String sql) {
            return proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "execute" -> Flux.just(result(sql.startsWith("insert into sample_tokens") ? 1L : 7L));
                case "returnGeneratedValues", "fetchSize", "bind", "bindNull" -> self;
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }

        private Result result(long rows) {
            Result.UpdateCount count = () -> rows;
            return proxy(Result.class, (ignored, method, arguments) -> switch (method.getName()) {
                case "getRowsUpdated" -> Mono.just(rows);
                case "flatMap" -> {
                    @SuppressWarnings("unchecked")
                    Function<Result.Segment, Publisher<?>> consumer =
                            (Function<Result.Segment, Publisher<?>>) arguments[0];
                    yield Flux.from(consumer.apply(count));
                }
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
