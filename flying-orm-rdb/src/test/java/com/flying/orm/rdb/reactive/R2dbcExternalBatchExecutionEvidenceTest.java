package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcExternalBatchExecutionEvidenceTest {

    @Test
    void returnsTwoChunkFactsWithoutOwningOrWaitingForTheExternalTransaction() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnection(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));

        BatchExecutionEvidence evidence = executor.writeBatchEvidence(request()).block(Duration.ofSeconds(2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertEquals(4L, evidence.inputCount());
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(4L, evidence.affectedRows().value());
        assertEquals(2, executions.get());
        assertEquals(2, evidence.chunks().size());
        assertChunk(evidence.chunks().get(0), 0, 0L, List.of(0L, 1L));
        assertChunk(evidence.chunks().get(1), 1, 2L, List.of(2L, 3L));
    }

    @Test
    void keepsAffectedRowsUnknownWhenTheDriverPublishesNoCounts() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnection(executions, Mono.empty());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));

        BatchExecutionEvidence evidence = executor.writeBatchEvidence(request()).block(Duration.ofSeconds(2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertEquals(4L, evidence.inputCount());
        assertEquals(2, evidence.chunks().size());
        assertTrue(evidence.chunks().stream().allMatch(fact -> !fact.affectedRows().isKnown()));
        assertTrue(!evidence.affectedRows().isKnown());
    }

    @Test
    void keepsProvenRowFactsWhenALaterPerRowWriteFails() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalPerRowConnectionThatFailsSecond(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}),
                BatchWriteOptions.atomic(2),
                BatchRowCountPolicy.EXACTLY_ONE);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, failure.evidence().commitFact());
        assertEquals(List.of(0L), failure.evidence().chunks().getFirst().successfulOffsets());
        assertTrue(failure.evidence().chunks().getFirst().affectedRows().isKnown());
        assertEquals(1L, failure.evidence().chunks().getFirst().affectedRows().value());
        assertEquals(2, executions.get());
    }

    @Test
    void preservesCancellationClassificationAcrossTheChunkFailureBoundary() {
        Connection connection = externalPerRowConnectionThatFailsFirst(
                new CancellationException("driver cancelled"));
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.EXACTLY_ONE);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(BatchExecutionState.CANCELLED, failure.evidence().state());
        assertEquals(BatchExecutionState.CANCELLED,
                     failure.evidence().chunks().getFirst().state());
        assertEquals(com.flying.orm.rdb.exception.RdbErrorKind.CANCELLED,
                     failure.evidence().chunks().getFirst().failure().kind());
    }

    @Test
    void reportsNoCommitFactWhenBindingFailsBeforeAnyDatabaseExecution() {
        AtomicInteger executions = new AtomicInteger();
        Connection connection = externalConnectionThatFailsBinding(executions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.ANY);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request).block(Duration.ofSeconds(2)));

        assertEquals(0, executions.get());
        assertEquals(BatchCommitFact.NOT_APPLICABLE, failure.evidence().commitFact());
    }

    private static void assertChunk(BatchChunkExecutionFact fact,
                                    int chunkIndex,
                                    long startOffset,
                                    List<Long> successfulOffsets) {
        assertEquals(chunkIndex, fact.chunkIndex());
        assertEquals(startOffset, fact.startOffset());
        assertEquals(2, fact.inputCount());
        assertEquals(successfulOffsets, fact.successfulOffsets());
        assertEquals(List.of(), fact.failedOffsets());
        assertEquals(BatchExecutionState.SUCCESS, fact.state());
        assertTrue(fact.affectedRows().isKnown());
        assertEquals(2L, fact.affectedRows().value());
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}, new Object[]{3}, new Object[]{4}),
                BatchWriteOptions.atomic(2),
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
