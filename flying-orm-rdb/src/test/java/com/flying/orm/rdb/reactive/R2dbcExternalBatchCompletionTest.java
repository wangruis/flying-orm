package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionLogObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises external completion through the public executor without owning the transaction. */
class R2dbcExternalBatchCompletionTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

    @Test
    void disabledObserverIsNotInvokedWhenExternalTransactionCompletes() {
        for (TransactionOutcome outcome : TransactionOutcome.values()) {
            AtomicInteger observations = new AtomicInteger();
            Fixture fixture = new Fixture(disabledObserver(observations, false));

            fixture.writeEnlisted();
            assertEquals(0, observations.get());
            assertEquals(0, fixture.completions.size());

            fixture.complete(outcome);
            fixture.complete(TransactionOutcome.UNKNOWN);

            assertEquals(0, observations.get(), "disabled observer must stay unused after " + outcome);
            assertFinalResult(fixture, BatchWriteResult.Status.valueOf(outcome.name()));
        }
    }

    @Test
    void disabledObserverFailureCannotSkipBusinessCompletion() {
        for (TransactionOutcome outcome : TransactionOutcome.values()) {
            AtomicInteger observations = new AtomicInteger();
            Fixture fixture = new Fixture(disabledObserver(observations, true));
            fixture.writeEnlisted();

            assertDoesNotThrow(() -> fixture.complete(outcome));

            assertEquals(0, observations.get());
            assertFinalResult(fixture, BatchWriteResult.Status.valueOf(outcome.name()));
        }
    }

    @Test
    void enabledObserverReceivesOneFinalSummaryForEachTransactionOutcome() {
        for (TransactionOutcome outcome : TransactionOutcome.values()) {
            List<BatchExecutionObservation> observations = new ArrayList<>();
            List<SqlTransactionSource> transactionSources = new ArrayList<>();
            Fixture fixture = new Fixture(new BatchExecutionObserver() {
                @Override
                public void onExecution(BatchExecutionObservation observation) {
                    onExecution(observation, null);
                }

                @Override
                public void onExecution(BatchExecutionObservation observation, SqlTransactionSource source) {
                    observations.add(observation);
                    transactionSources.add(source);
                }
            });
            fixture.writeEnlisted();
            assertEquals(2, observations.size());
            assertEquals(BatchChunkResult.Status.ENLISTED,
                         assertInstanceOf(BatchExecutionObservation.Chunk.class, observations.getFirst()).status());
            assertEquals(BatchWriteResult.Status.ENLISTED,
                         assertInstanceOf(BatchExecutionObservation.Summary.class, observations.getLast()).status());
            assertEquals(0, fixture.completions.size());

            fixture.complete(outcome);
            fixture.complete(TransactionOutcome.UNKNOWN);

            assertEquals(3, observations.size());
            BatchExecutionObservation.Summary summary = assertInstanceOf(
                    BatchExecutionObservation.Summary.class, observations.getLast());
            assertEquals(BatchWriteResult.Status.valueOf(outcome.name()), summary.status());
            assertFinalResult(fixture, summary.status());
            assertEquals(fixture.completions.getFirst().affectedRows(), summary.affectedRows());
            assertAll(
                    () -> assertEquals(List.of(SqlExecutionBackend.R2DBC, SqlExecutionBackend.R2DBC,
                                                SqlExecutionBackend.R2DBC),
                                       observations.stream().map(BatchExecutionObservation::backend).toList()),
                    () -> assertEquals(List.of(SqlTransactionSource.EXTERNAL, SqlTransactionSource.EXTERNAL,
                                                SqlTransactionSource.EXTERNAL), transactionSources));
        }
    }

    @Test
    void finalSummaryLogRetainsExternalR2dbcContextForEachTransactionOutcome() {
        for (TransactionOutcome outcome : TransactionOutcome.values()) {
            List<String> messages = new ArrayList<>();
            Fixture fixture = new Fixture(SqlExecutionLogObserver.create(
                    SqlExecutionLogOptions.defaults(), messages::add));
            fixture.writeEnlisted();

            fixture.complete(outcome);
            fixture.complete(TransactionOutcome.UNKNOWN);

            assertEquals(3, messages.size());
            String finalMessage = messages.getLast();
            assertAll(
                    () -> assertTrue(finalMessage.contains("eventType=SUMMARY"), finalMessage),
                    () -> assertTrue(finalMessage.contains("status=" + outcome.name()), finalMessage),
                    () -> assertTrue(finalMessage.contains("backend=R2DBC"), finalMessage),
                    () -> assertTrue(finalMessage.contains("transactionSource=EXTERNAL"), finalMessage));
        }
    }

    @Test
    void enabledObserverRuntimeFailureDoesNotSkipBusinessCompletion() {
        AtomicInteger observations = new AtomicInteger();
        Fixture fixture = new Fixture(ignored -> {
            observations.incrementAndGet();
            throw new IllegalStateException("observer unavailable");
        });
        fixture.writeEnlisted();

        assertDoesNotThrow(() -> fixture.complete(TransactionOutcome.COMMITTED));
        fixture.complete(TransactionOutcome.ROLLED_BACK);

        assertEquals(3, observations.get());
        assertFinalResult(fixture, BatchWriteResult.Status.COMMITTED);
    }

    @Test
    void enabledObserverDirectErrorStillPropagatesFromFinalNotification() {
        AssertionError failure = new AssertionError("observer failed");
        Fixture fixture = new Fixture(observation -> {
            if (observation instanceof BatchExecutionObservation.Summary summary
                    && summary.status() == BatchWriteResult.Status.COMMITTED) {
                throw failure;
            }
        });
        fixture.writeEnlisted();

        RuntimeException propagated = assertThrows(RuntimeException.class,
                () -> fixture.complete(TransactionOutcome.COMMITTED));
        assertSame(failure, Exceptions.unwrap(propagated));
        fixture.complete(TransactionOutcome.COMMITTED);

        assertEquals(0, fixture.completions.size());
    }

    @Test
    void unavailableRegistrationStillRunsUnknownCompletionWithoutObservation() {
        AtomicInteger observations = new AtomicInteger();
        Fixture fixture = new Fixture(disabledObserver(observations, true));
        fixture.registrationAvailable = false;

        fixture.writeEnlisted();

        assertEquals(0, observations.get());
        assertFinalResult(fixture, BatchWriteResult.Status.UNKNOWN);
    }

    @Test
    void failedRegistrationStillRunsUnknownCompletionWithoutObservation() {
        AtomicInteger observations = new AtomicInteger();
        Fixture fixture = new Fixture(disabledObserver(observations, true));
        fixture.registrationFailure = new IllegalStateException("completion registration unavailable");

        fixture.writeEnlisted();

        assertEquals(0, observations.get());
        assertFinalResult(fixture, BatchWriteResult.Status.UNKNOWN);
    }

    private static void assertFinalResult(Fixture fixture, BatchWriteResult.Status expected) {
        assertEquals(1, fixture.registrations.get());
        assertEquals(1, fixture.executions.get());
        assertEquals(1, fixture.completions.size());
        BatchWriteResult result = fixture.completions.getFirst();
        assertEquals(expected, result.status());
        assertEquals(1L, result.inputCount());
        assertEquals(expected == BatchWriteResult.Status.COMMITTED ? 1L : 0L, result.affectedRows());
        assertEquals(1, result.chunks().size());
        assertEquals(BatchChunkResult.Status.valueOf(expected.name()), result.chunks().getFirst().status());
        assertEquals(0, result.chunks().getFirst().chunkIndex());
        assertEquals(0L, result.chunks().getFirst().startOffset());
        if (expected == BatchWriteResult.Status.UNKNOWN) {
            assertNotNull(result.chunks().getFirst().failure());
        }
    }

    private static BatchExecutionObserver disabledObserver(AtomicInteger observations, boolean throwing) {
        return new BatchExecutionObserver() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public void onExecution(BatchExecutionObservation observation) {
                observations.incrementAndGet();
                if (throwing) {
                    throw new IllegalStateException("disabled observer must not run");
                }
            }
        };
    }

    private static final class Fixture {
        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicInteger registrations = new AtomicInteger();
        private final List<R2dbcTransactionCompletion.Listener> listeners = new ArrayList<>();
        private final List<BatchWriteResult> completions = new ArrayList<>();
        private final R2dbcSqlExecutor executor;
        private boolean registrationAvailable = true;
        private RuntimeException registrationFailure;

        private Fixture(BatchExecutionObserver observer) {
            Result result = proxy(Result.class, (self, method, arguments) -> switch (method.getName()) {
                case "getRowsUpdated" -> Mono.just(1L);
                default -> throw new AssertionError("unexpected result call: " + method.getName());
            });
            Statement statement = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "bind", "bindNull", "add" -> self;
                case "execute" -> {
                    executions.incrementAndGet();
                    yield Flux.just(result);
                }
                default -> throw new AssertionError("unexpected statement call: " + method.getName());
            });
            Connection connection = proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                case "createStatement" -> statement;
                default -> throw new AssertionError("external connection must not be managed: " + method.getName());
            });
            R2dbcTransactionContext transaction = R2dbcTransactionContext.external(connection, listener -> {
                registrations.incrementAndGet();
                if (registrationFailure != null) {
                    throw registrationFailure;
                }
                if (registrationAvailable) {
                    listeners.add(listener);
                }
                return registrationAvailable;
            });
            executor = R2dbcSqlExecutor.create(new ConnectionFactory() {
                @Override
                public Publisher<? extends Connection> create() {
                    throw new AssertionError("external transaction must not acquire another connection");
                }

                @Override
                public ConnectionFactoryMetadata getMetadata() {
                    return () -> "H2";
                }
            }).withTransactionParticipant(() -> Mono.just(transaction)).withBatchObserver(observer);
        }

        private void writeEnlisted() {
            BatchWriteRequest request = new BatchWriteRequest(SqlStatementPlan.canonical(
                    "insert into sample(value_col) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                    List.of(Integer.class), Flux.<Object[]>just(new Object[]{1}), BatchWriteOptions.atomic(1),
                    BatchRowCountPolicy.EXACTLY_ONE, BatchGeneratedKeys.none(),
                    result -> Mono.fromRunnable(() -> completions.add(result)));
            BatchWriteResult result = executor.writeBatch(request).block(WAIT);

            assertNotNull(result);
            assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
            assertEquals(0L, result.affectedRows());
            assertEquals(1, registrations.get());
            assertEquals(1, executions.get());
        }

        private void complete(TransactionOutcome outcome) {
            assertEquals(1, listeners.size());
            Mono.from(listeners.getFirst().afterCompletion(outcome)).block(WAIT);
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
