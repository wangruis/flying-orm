package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchEvidenceTimeoutCancellationTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

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

        Disposable subscription = executor.writeBatchEvidence(request())
                .subscribe(ignored -> {
                }, downstreamError::set);
        assertTrue(await(executionStarted));
        subscription.dispose();

        assertTrue(await(evidenceObserved));
        assertFalse(subscription.isDisposed() && downstreamError.get() != null,
                    "cancelled subscriber must not receive an error");
        assertTerminalEvidence(observed.get(), BatchExecutionState.CANCELLED);
    }

    private static void assertTerminalEvidence(BatchExecutionEvidence evidence,
                                               BatchExecutionState expectedState) {
        assertEquals(expectedState, evidence.state());
        assertEquals(1L, evidence.inputCount());
        assertFalse(evidence.affectedRows().isKnown());
        assertFalse(evidence.affectedRows().isKnown());
    }

    private static R2dbcSqlExecutor executor(Connection connection, BatchExecutionObserver observer) {
        return R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2())
                .withBatchObserver(observer);
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY);
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
