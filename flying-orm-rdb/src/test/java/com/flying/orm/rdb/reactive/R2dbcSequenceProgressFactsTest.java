package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcSequenceProgressFactsTest {

    @TestFactory
    Stream<DynamicTest> preservesConsumedCountsInEveryPhase() {
        return Arrays.stream(SqlExecutionPhase.values()).flatMap(phase ->
                Arrays.stream(Terminal.values()).map(terminal -> DynamicTest.dynamicTest(
                        phase + "/" + terminal, () -> verify(phase, terminal, true, List.of(2L, 5L)))));
    }

    @TestFactory
    Stream<DynamicTest> disabledObservationPreservesResultsAndRelease() {
        return Arrays.stream(Terminal.values()).map(terminal -> DynamicTest.dynamicTest(
                terminal.name(), () -> verify(SqlExecutionPhase.WORK, terminal, false, List.of(2L, 5L))));
    }

    @TestFactory
    Stream<DynamicTest> noConsumedCountsRemainZero() {
        return Stream.of(Terminal.ERROR, Terminal.CANCEL).map(terminal -> DynamicTest.dynamicTest(
                terminal.name(), () -> verify(SqlExecutionPhase.WORK, terminal, true, List.of())));
    }

    @Test
    void repeatedSubscriptionsHaveIndependentStepCounts() {
        Fixture fixture = new Fixture(SqlExecutionPhase.WORK, List.of(2L, 5L));
        fixture.tail.tryEmitEmpty();
        Mono<SqlExecutionSequenceResult> execution = fixture.operation(true);
        for (int index = 0; index < 2; index++) {
            AtomicReference<SqlExecutionSequenceResult> result = new AtomicReference<>();
            execution.subscribe(result::set, error -> fail(error));
            assertEquals(7L, result.get().rowsUpdated());
        }
        assertEquals(List.of(7L, 7L), fixture.targetEvents().stream().map(SqlExecutionObservation::rows).toList());
        assertEquals(2, fixture.releases.get());
        assertEquals(2, fixture.cleanups.get());
    }

    @TestFactory
    Stream<DynamicTest> overflowRetainsLastRepresentableFact() {
        return Arrays.stream(SqlExecutionPhase.values()).map(phase -> DynamicTest.dynamicTest(phase.name(), () -> {
            Fixture fixture = new Fixture(phase, List.of(Long.MAX_VALUE, 1L));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            fixture.operation(true).subscribe(ignored -> fail("overflow returned a result"), failure::set);
            assertInstanceOf(SqlExecutionSequenceException.class, failure.get());
            assertEquals(1, fixture.targetEvents().size());
            assertEquals(SqlExecutionStatus.ERROR, fixture.targetEvents().getFirst().status());
            assertEquals(Long.MAX_VALUE, fixture.targetEvents().getFirst().rows());
            assertEquals(1, fixture.releases.get());
            assertEquals(1, fixture.cleanups.get());
        }));
    }

    private static void verify(SqlExecutionPhase phase, Terminal terminal, boolean observed, List<Long> counts) {
        Fixture fixture = new Fixture(phase, counts);
        AtomicReference<SqlExecutionSequenceResult> output = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Disposable subscription = fixture.operation(observed).subscribe(output::set, failure::set);
        assertTrue(fixture.atTail.get(), "driver counts must be consumed before termination");
        assertTrue(fixture.targetEvents().isEmpty());
        assertEquals(0, fixture.releases.get());
        assertNull(output.get());
        assertNull(failure.get());
        switch (terminal) {
            case ERROR -> fixture.tail.emitError(new IllegalStateException("result tail failed"), Sinks.EmitFailureHandler.FAIL_FAST);
            case COMPLETE -> fixture.tail.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
            case CANCEL -> {
                subscription.dispose();
                if (phase == SqlExecutionPhase.CLEANUP) {
                    assertEquals(0, fixture.releases.get(), "started cleanup still owns its completion");
                    assertTrue(fixture.targetEvents().isEmpty());
                    fixture.tail.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
                }
            }
        }
        assertEquals(1, fixture.releases.get());
        assertEquals(1, fixture.cleanups.get());
        assertEquals(terminal == Terminal.COMPLETE, output.get() != null);
        if (terminal == Terminal.ERROR) {
            SqlExecutionSequenceException error = assertInstanceOf(SqlExecutionSequenceException.class, failure.get());
            assertEquals(phase, error.phase());
        } else {
            assertNull(failure.get());
        }
        long rows = counts.stream().mapToLong(Long::longValue).sum();
        if (output.get() != null) assertEquals(phase == SqlExecutionPhase.WORK ? rows : 1L, output.get().rowsUpdated());
        if (observed) {
            assertEquals(1, fixture.targetEvents().size());
            SqlExecutionObservation event = fixture.targetEvents().getFirst();
            assertEquals(terminal == Terminal.CANCEL && phase == SqlExecutionPhase.CLEANUP
                    ? SqlExecutionStatus.SUCCESS : terminal.status, event.status());
            assertEquals(rows, event.rows());
        } else {
            assertTrue(fixture.events.isEmpty());
        }
    }

    private enum Terminal {
        CANCEL(SqlExecutionStatus.CANCELLED), ERROR(SqlExecutionStatus.ERROR), COMPLETE(SqlExecutionStatus.SUCCESS);
        private final SqlExecutionStatus status;
        Terminal(SqlExecutionStatus status) { this.status = status; }
    }

    private static final class Fixture implements R2dbcConnectionAccess {
        private final SqlExecutionPhase phase;
        private final List<Long> counts;
        private final Sinks.Empty<Void> tail = Sinks.empty();
        private final AtomicBoolean atTail = new AtomicBoolean();
        private final AtomicInteger releases = new AtomicInteger();
        private final AtomicInteger cleanups = new AtomicInteger();
        private final List<SqlExecutionObservation> events = new ArrayList<>();

        private Fixture(SqlExecutionPhase phase, List<Long> counts) {
            this.phase = phase;
            this.counts = counts;
        }

        private Mono<SqlExecutionSequenceResult> operation(boolean observed) {
            R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(this, RdbDialect.postgresql());
            if (observed) executor = executor.withObserver(events::add);
            return executor.executeInConnection(new SqlExecutionSequence(
                    List.of(request(SqlExecutionPhase.SETUP)), List.of(request(SqlExecutionPhase.WORK)),
                    List.of(request(SqlExecutionPhase.CLEANUP))), SqlExecutionOptions.safeDefaults());
        }

        private List<SqlExecutionObservation> targetEvents() {
            return events.stream().filter(event -> event.sql().equals(request(phase).sql())).toList();
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
            return proxy(Statement.class, (self, method, args) -> {
                if (!method.getName().equals("execute")) throw new AssertionError(method.getName());
                if (sql.equals(request(SqlExecutionPhase.CLEANUP).sql())) cleanups.incrementAndGet();
                List<Long> resultCounts = sql.equals(request(phase).sql()) ? counts : List.of(1L);
                Flux<Long> values = Flux.fromIterable(resultCounts);
                if (sql.equals(request(phase).sql())) {
                    values = values.concatWith(tail.asMono().doOnSubscribe(ignored -> atTail.set(true)).thenMany(Flux.empty()));
                }
                Flux<Long> stream = values;
                return Flux.just(proxy(Result.class, (result, resultMethod, resultArgs) -> {
                    if (resultMethod.getName().equals("getRowsUpdated")) return stream;
                    throw new AssertionError(resultMethod.getName());
                }));
            });
        }
    }

    private static SqlRequest request(SqlExecutionPhase phase) {
        return new SqlRequest("update sample set value = 1 /* " + phase + " */", List.of());
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
