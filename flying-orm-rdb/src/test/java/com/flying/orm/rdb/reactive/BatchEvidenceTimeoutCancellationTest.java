package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchEvidenceTimeoutCancellationTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

    @Test
    void timeoutThrowsStablePartialEvidenceWithoutTouchingTheExternalTransaction() {
        CountDownLatch executionStarted = new CountDownLatch(1);
        R2dbcSqlExecutor executor = executor(stalledExternalConnection(executionStarted),
                                             BatchExecutionObserver.noop());

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request(Duration.ofMillis(25))).block(WAIT));

        assertTrue(await(executionStarted));
        assertTerminalEvidence(failure.evidence(), BatchExecutionState.TIMED_OUT);
    }

    @Test
    void cancellationPublishesEvidenceOnlyThroughTheInstalledObserver() {
        CountDownLatch executionStarted = new CountDownLatch(1);
        CountDownLatch evidenceObserved = new CountDownLatch(1);
        AtomicReference<BatchExecutionEvidence> observed = new AtomicReference<>();
        AtomicReference<Throwable> downstreamError = new AtomicReference<>();
        BatchExecutionObserver observer = new BatchExecutionObserver() {
            @Override
            public void onExecution(BatchExecutionObservation observation) {
                // 这个测试只关心 evidence 旁路。
            }

            @Override
            public void onExecutionEvidence(BatchExecutionEvidence evidence) {
                observed.set(evidence);
                evidenceObserved.countDown();
            }
        };
        R2dbcSqlExecutor executor = executor(stalledExternalConnection(executionStarted), observer);

        Disposable subscription = executor.writeBatchEvidence(request(Duration.ZERO))
                .subscribe(ignored -> {
                }, downstreamError::set);
        assertTrue(await(executionStarted));
        subscription.dispose();

        assertTrue(await(evidenceObserved));
        assertFalse(subscription.isDisposed() && downstreamError.get() != null,
                    "cancelled subscriber must not receive an error");
        assertTerminalEvidence(observed.get(), BatchExecutionState.CANCELLED);
    }

    @Test
    void timeoutRetainsRowsCompletedBeforeTheActivePerRowWriteStalls() {
        CountDownLatch secondExecutionStarted = new CountDownLatch(1);
        R2dbcSqlExecutor executor = executor(
                firstRowSucceedsAndSecondStalls(secondExecutionStarted),
                BatchExecutionObserver.noop());

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(exactRequest(Duration.ofMillis(25))).block(WAIT));

        assertTrue(await(secondExecutionStarted));
        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(List.of(0L), failure.evidence().chunks().getFirst().successfulOffsets());
        assertEquals(BatchExecutionState.TIMED_OUT,
                     failure.evidence().chunks().getFirst().state());
        assertTrue(failure.evidence().chunks().getFirst().affectedRows().isKnown());
        assertEquals(1L, failure.evidence().chunks().getFirst().affectedRows().value());
    }

    private static void assertTerminalEvidence(BatchExecutionEvidence evidence,
                                               BatchExecutionState expectedState) {
        assertEquals(expectedState, evidence.state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertEquals(1L, evidence.inputCount());
        assertFalse(evidence.affectedRows().isKnown());
        assertEquals(1, evidence.chunks().size());
        assertEquals(expectedState, evidence.chunks().getFirst().state());
        assertFalse(evidence.chunks().getFirst().affectedRows().isKnown());
    }

    private static R2dbcSqlExecutor executor(Connection connection, BatchExecutionObserver observer) {
        return R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)))
                .withBatchObserver(observer);
    }

    private static BatchWriteRequest request(Duration timeout) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.atomic(1).withTimeout(timeout),
                BatchRowCountPolicy.ANY);
    }

    private static BatchWriteRequest exactRequest(Duration timeout) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}),
                BatchWriteOptions.atomic(2).withTimeout(timeout),
                BatchRowCountPolicy.EXACTLY_ONE);
    }

    private static ConnectionFactory unusedConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new AssertionError("external transaction must not acquire another connection");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection stalledExternalConnection(CountDownLatch executionStarted) {
        Statement statement = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull", "add" -> self;
            case "execute" -> {
                executionStarted.countDown();
                yield Flux.never();
            }
            default -> throw new AssertionError("unexpected statement call: " + method.getName());
        });
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> statement;
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static Connection firstRowSucceedsAndSecondStalls(CountDownLatch secondExecutionStarted) {
        AtomicInteger executions = new AtomicInteger();
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> {
                Statement[] holder = new Statement[1];
                holder[0] = proxy(Statement.class, (statement, statementMethod, statementArguments) ->
                        switch (statementMethod.getName()) {
                            case "bind", "bindNull" -> holder[0];
                            case "execute" -> {
                                if (executions.incrementAndGet() == 1) {
                                    yield Flux.just(proxy(Result.class,
                                            (result, resultMethod, resultArguments) ->
                                                    switch (resultMethod.getName()) {
                                                        case "getRowsUpdated" -> Mono.just(1L);
                                                        default -> throw new AssertionError(
                                                                "unexpected result call: "
                                                                        + resultMethod.getName());
                                                    }));
                                }
                                secondExecutionStarted.countDown();
                                yield Flux.never();
                            }
                            default -> throw new AssertionError(
                                    "unexpected statement call: " + statementMethod.getName());
                        });
                yield holder[0];
            }
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
