package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcUpdateResultConsumptionTest {
    private static final SqlRequest REQUEST = new SqlRequest("update sample set state = 1", List.of());

    @Test
    void finishesEachResultBeforeConsumingTheNext() throws Exception {
        for (boolean observed : new boolean[]{false, true}) finishesEachResult(observed);
    }

    private void finishesEachResult(boolean observed) throws Exception {
        Sinks.One<Long> first = Sinks.one();
        AtomicInteger reads = new AtomicInteger();
        Fixture fixture = new Fixture(observed, Flux.just(result(first.asMono(), reads),
                result(Flux.empty(), reads), result(Flux.just(2L, 3L), reads)).hide());
        Mono<Long> execution = fixture.executor.rowsUpdated(REQUEST);
        assertEquals(0, fixture.acquisitions);
        CompletableFuture<Long> pending = execution.toFuture();
        try {
            assertEquals(1, reads.get(), "only the active Result may be consumed");
            assertEquals(Sinks.EmitResult.OK, first.tryEmitValue(1L));
            assertEquals(6L, pending.get(2, TimeUnit.SECONDS));
        } finally {
            pending.cancel(true);
        }
        assertEquals(3, reads.get());
        fixture.assertTerminal(observed, SignalType.ON_COMPLETE, SqlExecutionStatus.SUCCESS, 6L);
    }

    @Test
    void countFailurePreservesConfirmedPrefixAndReleases() {
        for (boolean observed : new boolean[]{false, true}) countFailure(observed);
    }

    private void countFailure(boolean observed) {
        RuntimeException failure = new IllegalStateException("count failure");
        AtomicInteger reads = new AtomicInteger();
        Fixture fixture = new Fixture(observed, Flux.just(
                result(Flux.concat(Flux.just(2L, 3L), Flux.error(failure)), reads),
                result(Mono.just(99L), reads)).hide());
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> fixture.executor.rowsUpdated(REQUEST).block(Duration.ofSeconds(2))));
        assertEquals(1, reads.get());
        fixture.assertTerminal(observed, SignalType.ON_ERROR, SqlExecutionStatus.ERROR, 5L);
    }

    @Test
    void cancellationDoesNotConsumeLaterResults() {
        for (boolean observed : new boolean[]{false, true}) cancellation(observed);
    }

    private void cancellation(boolean observed) {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        Flux<Long> pending = Flux.concat(Mono.just(3L), Mono.<Long>never())
                .doOnCancel(cancellations::incrementAndGet);
        Fixture fixture = new Fixture(observed, Flux.just(result(pending, reads), result(Mono.just(99L), reads)).hide());
        CompletableFuture<Long> execution = fixture.executor.rowsUpdated(REQUEST).toFuture();
        try {
            assertEquals(1, reads.get());
        } finally {
            execution.cancel(true);
        }
        assertEquals(1, cancellations.get());
        fixture.assertTerminal(observed, SignalType.CANCEL, SqlExecutionStatus.CANCELLED, 3L);
    }

    @Test
    void rejectsOverflowWithoutErasingTheConfirmedCount() {
        for (boolean observed : new boolean[]{false, true}) overflow(observed);
    }

    private void overflow(boolean observed) {
        Fixture fixture = new Fixture(observed,
                Flux.just(result(Flux.just(Long.MAX_VALUE, 1L), new AtomicInteger())).hide());
        RdbException failure = assertThrows(RdbException.class,
                () -> fixture.executor.rowsUpdated(REQUEST).block(Duration.ofSeconds(2)));
        assertInstanceOf(ArithmeticException.class, failure.getCause());
        fixture.assertTerminal(observed, SignalType.ON_ERROR, SqlExecutionStatus.ERROR, Long.MAX_VALUE);
    }

    @Test
    void emptyExecutionReturnsZeroAndCanBeResubscribed() {
        for (boolean observed : new boolean[]{false, true}) emptyExecution(observed);
    }

    private void emptyExecution(boolean observed) {
        Fixture fixture = new Fixture(observed, Flux.empty());
        Mono<Long> execution = fixture.executor.rowsUpdated(REQUEST);
        assertEquals(0L, execution.block(Duration.ofSeconds(2)));
        fixture.assertTerminal(observed, SignalType.ON_COMPLETE, SqlExecutionStatus.SUCCESS, 0L);
        assertEquals(0L, execution.block(Duration.ofSeconds(2)));
        assertEquals(2, fixture.acquisitions);
        assertEquals(List.of(SignalType.ON_COMPLETE, SignalType.ON_COMPLETE), fixture.releases);
    }

    private static Result result(Publisher<Long> counts, AtomicInteger reads) {
        return proxy(Result.class, (instance, method, arguments) -> {
            if (method.getName().equals("getRowsUpdated")) {
                reads.incrementAndGet();
                return counts;
            }
            throw new AssertionError(method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static final class Fixture {
        private final List<SignalType> releases = new ArrayList<>();
        private final List<SqlExecutionObservation> observations = new ArrayList<>();
        private final R2dbcSqlExecutor executor;
        private int acquisitions;

        private Fixture(boolean observed, Publisher<Result> results) {
            Statement statement = proxy(Statement.class, (instance, method, arguments) -> {
                if (method.getName().equals("execute")) return results;
                throw new AssertionError(method.getName());
            });
            Connection connection = proxy(Connection.class, (instance, method, arguments) -> {
                if (method.getName().equals("createStatement")) return statement;
                throw new AssertionError(method.getName());
            });
            R2dbcSqlExecutor configured = R2dbcSqlExecutor.create(R2dbcConnectionAccess.of(
                    request -> Mono.fromSupplier(() -> { acquisitions++; return connection; }),
                    (signal, acquired, request) -> {
                        assertSame(connection, acquired);
                        releases.add(signal);
                        return Mono.empty();
                    }), RdbDialect.postgresql());
            executor = observed ? configured.withObserver(observations::add) : configured;
        }

        private void assertTerminal(boolean observed, SignalType signal, SqlExecutionStatus status, long rows) {
            assertEquals(1, acquisitions);
            assertEquals(List.of(signal), releases);
            assertEquals(observed ? 1 : 0, observations.size());
            if (observed) {
                assertEquals(status, observations.getFirst().status());
                assertEquals(rows, observations.getFirst().rows());
            }
        }
    }
}
