package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CancellationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcExternalBatchExecutionEvidenceTest {

    @Test
    void returnsTwoChunkFactsWithoutOwningOrWaitingForTheExternalTransaction() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnection(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());

        BatchExecutionEvidence evidence = executor.writeBatchEvidence(request()).block(Duration.ofSeconds(2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(4L, evidence.inputCount());
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(4L, evidence.affectedRows().value());
        assertEquals(2, executions.get());
        assertEquals(List.of(0L, 1L, 2L, 3L), evidence.successfulOffsets().boxed().toList());
    }

    @Test
    void keepsAffectedRowsUnknownWhenTheDriverPublishesNoCounts() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnection(executions, Mono.empty());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());

        BatchExecutionEvidence evidence = executor.writeBatchEvidence(request()).block(Duration.ofSeconds(2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(4L, evidence.inputCount());
        assertEquals(4L, evidence.successfulCount());
        assertTrue(!evidence.affectedRows().isKnown());
    }

    @Test
    void keepsProvenRowFactsWhenALaterPerRowWriteFails() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalPerRowConnectionThatFailsSecond(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}),
                BatchWriteOptions.of(2),
                BatchRowCountPolicy.EXACTLY_ONE);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(2L, failure.evidence().inputCount());
        assertEquals(1L, failure.evidence().successfulCount());
        assertEquals(0L, failure.evidence().failedCount());
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
        // The second SQL was executed but never completed: its contribution is unknown,
        // while the first row's completed SQL remains a proven successful position.
        assertFalse(failure.evidence().affectedRows().isKnown());
        assertEquals(2, executions.get());
    }

    @Test
    void preservesCancellationClassificationAcrossTheChunkFailureBoundary() {
        Connection connection = externalPerRowConnectionThatFailsFirst(
                new CancellationException("driver cancelled"));
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.of(1),
                BatchRowCountPolicy.EXACTLY_ONE);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(BatchExecutionState.CANCELLED, failure.evidence().state());
        assertEquals(BatchExecutionState.CANCELLED,
                     failure.evidence().state());
        assertEquals(com.flying.orm.rdb.exception.RdbErrorKind.CANCELLED,
                     failure.evidence().failure().kind());
    }

    @Test
    void bindingFailsBeforeAnyDatabaseExecution() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnectionThatFailsBinding(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.h2());
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(0, executions.get());
        assertEquals(0, failure.evidence().successfulCount());
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4}),
                BatchWriteOptions.of(2),
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

    private static Connection externalConnection(AtomicInteger executions) {
        return externalConnection(executions, Mono.just(2L));
    }

    private static Connection externalConnection(AtomicInteger executions,
                                                 Publisher<Long> rowsUpdated) {
        Result result = proxy(Result.class, (self, method, arguments) -> switch (method.getName()) {
            case "getRowsUpdated" -> rowsUpdated;
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
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> statement;
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static Connection externalPerRowConnectionThatFailsSecond(AtomicInteger executions) {
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> {
                Statement[] holder = new Statement[1];
                holder[0] = proxy(Statement.class, (statement, statementMethod, statementArguments) ->
                        switch (statementMethod.getName()) {
                            case "bind", "bindNull" -> holder[0];
                            case "execute" -> executions.incrementAndGet() == 1
                                    ? Flux.just(proxy(Result.class, (result, resultMethod, resultArguments) ->
                                            switch (resultMethod.getName()) {
                                                case "getRowsUpdated" -> Mono.just(1L);
                                                default -> throw new AssertionError(
                                                        "unexpected result call: " + resultMethod.getName());
                                            }))
                                    : Flux.error(new IllegalStateException("second row failed"));
                            default -> throw new AssertionError(
                                    "unexpected statement call: " + statementMethod.getName());
                        });
                yield holder[0];
            }
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static Connection externalPerRowConnectionThatFailsFirst(Throwable failure) {
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> {
                Statement[] holder = new Statement[1];
                holder[0] = proxy(Statement.class, (statement, statementMethod, statementArguments) ->
                        switch (statementMethod.getName()) {
                            case "bind", "bindNull" -> holder[0];
                            case "execute" -> Flux.error(failure);
                            default -> throw new AssertionError(
                                    "unexpected statement call: " + statementMethod.getName());
                        });
                yield holder[0];
            }
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static Connection externalConnectionThatFailsBinding(AtomicInteger executions) {
        Statement statement = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull" -> throw new IllegalArgumentException("bind failed");
            case "execute" -> {
                executions.incrementAndGet();
                yield Flux.empty();
            }
            default -> throw new AssertionError("unexpected statement call: " + method.getName());
        });
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> statement;
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
