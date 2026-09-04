package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcAtomicBatchTimeoutInputCountTest {

    @Test
    void classifiesInputFailureBeforeTheFirstChunkWithoutAcquiringAConnection() {
        assertInputFailureState(
                new TimeoutException("input timed out"), BatchExecutionState.TIMED_OUT);
        assertInputFailureState(
                new CancellationException("input cancelled"), BatchExecutionState.CANCELLED);
    }

    @Test
    void classifiesInputCancellationAfterASuccessfulChunk() {
        AtomicInteger rollbacks = new AtomicInteger();
        CancellationException cancellation = new CancellationException("input cancelled");
        BatchWriteRequest request = request(Flux.concat(
                Flux.just(
                        new Object[]{"name-0"},
                        new Object[]{"name-1"},
                        new Object[]{"name-2"}),
                Flux.error(cancellation)), 2);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(
                                successfulConnection(new AtomicInteger(), rollbacks)))
                        .writeBatchEvidence(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, rollbacks.get());
        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(BatchCommitFact.ROLLED_BACK, failure.evidence().commitFact());
        assertEquals(List.of(BatchExecutionState.SUCCESS, BatchExecutionState.CANCELLED),
                     failure.evidence().chunks().stream().map(fact -> fact.state()).toList());
        assertEquals(List.of(2, 1),
                     failure.evidence().chunks().stream().map(fact -> fact.inputCount()).toList());
    }

    @Test
    void evidenceReportsSuccessfulOwnedRollback() {
        AtomicInteger rollbacks = new AtomicInteger();
        BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{"name-0"}));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(
                                ownedConnectionThatFailsBusinessWrite(rollbacks)))
                        .writeBatchEvidence(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, rollbacks.get());
        assertEquals(BatchCommitFact.ROLLED_BACK, failure.evidence().commitFact());
    }

    @Test
    void evidenceKeepsUnknownWhenOwnedRollbackFails() {
        AtomicInteger rollbacks = new AtomicInteger();
        BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{"name-0"}));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(
                                ownedConnectionThatFailsBusinessWrite(rollbacks, true)))
                        .writeBatchEvidence(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, rollbacks.get());
        assertEquals(BatchCommitFact.UNKNOWN, failure.evidence().commitFact());
    }

    @Test
    void reportsAcceptedRowsWhenInputFailsBeforeTheFirstChunkExists() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("input failed");
        AtomicInteger connectionRequests = new AtomicInteger();
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                connectionRequests.incrementAndGet();
                return Mono.error(new AssertionError("input failure must not acquire a connection"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        BatchWriteRequest request = request(Flux.concat(
                Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(inputFailure)), 2);

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory).writeBatch(request).block(Duration.ofSeconds(2)));

        assertEquals(0, connectionRequests.get());
        assertEquals(1, error.result().inputCount());
        assertEquals(0, error.result().chunks().getFirst().chunkIndex());
        assertEquals(0, error.result().chunks().getFirst().startOffset());
        assertEquals(BatchChunkResult.Status.FAILED, error.result().chunks().getFirst().status());
        assertSame(inputFailure, error.getCause());
    }

    @Test
    void rollsBackOwnedTransactionWhenTheRowSourcePublishesABatchFailure() {
        BatchWriteException sourceFailure = publishedBatchFailure();
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        BatchWriteRequest request = request(Flux.concat(
                Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(sourceFailure)), 1);

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(successfulConnection(commits, rollbacks)))
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(0, commits.get());
        assertEquals(1, rollbacks.get());
        assertSame(sourceFailure, error.getCause());
        assertEquals(1, error.result().inputCount());
        assertEquals(List.of(BatchChunkResult.Status.ROLLED_BACK, BatchChunkResult.Status.FAILED),
                     error.result().chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void reportsAcceptedChunkSizeWhenDeadlineCancelsItsExecution() {
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"name-0"}),
                BatchWriteOptions.atomic(1).withTimeout(Duration.ofMillis(25)));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(stalledConnection()))
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, error.result().inputCount());
        assertEquals(BatchChunkResult.Status.FAILED,
                     error.result().chunks().getFirst().status());
    }

    @Test
    void reportsRowsAcceptedIntoAnIncompleteNextChunkBeforeTimeout() {
        Flux<Object[]> rows = Flux.concat(
                Flux.just(new Object[]{"name-0"}, new Object[]{"name-1"}, new Object[]{"name-2"}),
                Flux.never());
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.atomic(2).withTimeout(Duration.ofMillis(50)));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(successfulConnection()))
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(3, error.result().inputCount());
        assertEquals(2, error.result().chunks().size());
        assertEquals(1, error.result().chunks().get(1).inputCount());
    }

    @Test
    void reportsActiveChunkWhenExternalTransactionExecutionFails() {
        BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{"name-0"}));
        Connection connection = connectionThatFailsBusinessWrite();
        ConnectionFactory unusedFactory = connectionFactory(Mono.error(
                new AssertionError("external transaction must bypass the connection factory")));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(unusedFactory)
                        .withTransactionParticipant(() -> Mono.just(R2dbcTransactionContext.external(connection)))
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, error.result().inputCount());
        assertEquals(BatchChunkResult.Status.UNKNOWN, error.result().chunks().getFirst().status());
    }

    @Test
    void reportsActiveChunkWhenProtectedSideIndexCompletionFails() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into batch_people(name_col) values (?)", List.of("name-0")),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from batch_people_tokens where id = ? and field_tag = ?",
                "insert into batch_people_tokens(id, field_tag, token_value) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("name_col", List.of(new byte[]{1}))));
        BatchWriteRequest request = request(Flux.<Object[]>just(
                ProtectedBatchRows.extend(new Object[]{"name-0"}, work)));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory(connectionThatFailsSideIndexWrite()))
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, error.result().inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, error.result().chunks().getFirst().status());
    }

    @Test
    void doesNotStartASecondRollbackWhenTheBatchDeadlineCancelsTheFirstOne() throws Exception {
        AtomicInteger rollbacks = new AtomicInteger();
        CountDownLatch terminated = new CountDownLatch(1);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"name-0"}),
                BatchWriteOptions.atomic(1).withTimeout(Duration.ofMillis(25)));

        Disposable subscription = R2dbcSqlExecutor.create(
                        connectionFactory(connectionWithStalledRollback(rollbacks)))
                .writeBatch(request)
                .doFinally(ignored -> terminated.countDown())
                .subscribe(ignored -> { }, ignored -> { });
        try {
            terminated.await(500, TimeUnit.MILLISECONDS);
            assertEquals(1, rollbacks.get());
        } finally {
            subscription.dispose();
        }
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows) {
        return request(rows, 1);
    }

    private static void assertInputFailureState(
            Throwable failure, BatchExecutionState expectedState) {
        AtomicInteger connectionRequests = new AtomicInteger();
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                connectionRequests.incrementAndGet();
                return Mono.error(new AssertionError("input failure must not acquire a connection"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };

        BatchExecutionEvidenceException error = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory)
                        .writeBatchEvidence(request(Flux.error(failure), 2))
                        .block(Duration.ofSeconds(2)));

        assertEquals(0, connectionRequests.get());
        assertEquals(expectedState, error.evidence().state());
        assertEquals(BatchCommitFact.NOT_APPLICABLE, error.evidence().commitFact());
        assertEquals(0L, error.evidence().inputCount());
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows, int chunkSize) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.atomic(chunkSize));
    }

    private static ConnectionFactory connectionFactory(Connection connection) {
        return connectionFactory(Mono.just(connection));
    }

    private static ConnectionFactory connectionFactory(Publisher<? extends Connection> connections) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return connections;
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    private static Connection connectionThatFailsBusinessWrite() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> throw new IllegalStateException("business write failed");
                    case "toString" -> "failing-business-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection ownedConnectionThatFailsBusinessWrite(AtomicInteger rollbacks) {
        return ownedConnectionThatFailsBusinessWrite(rollbacks, false);
    }

    private static Connection ownedConnectionThatFailsBusinessWrite(AtomicInteger rollbacks,
                                                                     boolean failRollback) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "setAutoCommit", "close" -> Mono.empty();
                    case "rollbackTransaction" -> Mono.defer(() -> {
                        rollbacks.incrementAndGet();
                        return failRollback
                                ? Mono.error(new IllegalStateException("rollback failed"))
                                : Mono.empty();
                    });
                    case "createStatement" -> throw new IllegalStateException("business write failed");
                    case "toString" -> "owned-failing-business-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection connectionWithStalledRollback(AtomicInteger rollbacks) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "setAutoCommit", "close" -> Mono.empty();
                    case "rollbackTransaction" -> Mono.defer(() -> {
                        rollbacks.incrementAndGet();
                        return Mono.never();
                    });
                    case "createStatement" -> throw new IllegalStateException("business write failed");
                    case "toString" -> "stalled-rollback-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection connectionThatFailsSideIndexWrite() {
        AtomicInteger statements = new AtomicInteger();
        Statement[] statement = new Statement[1];
        statement[0] = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> statement[0];
                    case "execute" -> statements.get() == 1
                            ? Flux.just(rowsUpdated(1L))
                            : Flux.error(new IllegalStateException("side index write failed"));
                    case "toString" -> "protected-batch-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "rollbackTransaction", "setAutoCommit", "close" -> Mono.empty();
                    case "createStatement" -> {
                        statements.incrementAndGet();
                        yield statement[0];
                    }
                    case "toString" -> "protected-batch-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result rowsUpdated(long value) {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(value);
                    case "toString" -> "rows-updated-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection stalledConnection() {
        Statement[] statement = new Statement[1];
        statement[0] = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> statement[0];
                    case "execute" -> Flux.never();
                    case "toString" -> "stalled-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "rollbackTransaction", "setAutoCommit", "close" -> Mono.empty();
                    case "createStatement" -> statement[0];
                    case "toString" -> "stalled-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static BatchWriteException publishedBatchFailure() {
        return new BatchWriteException(
                "failure from another batch",
                new IllegalStateException("prior batch failed"),
                BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                        List.of(BatchChunkResult.committed(0, 0, 500, 500L))));
    }

    private static Connection successfulConnection() {
        return successfulConnection(new AtomicInteger(), new AtomicInteger());
    }

    private static Connection successfulConnection(AtomicInteger commits, AtomicInteger rollbacks) {
        Statement[] statement = new Statement[1];
        statement[0] = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> statement[0];
                    case "execute" -> Flux.just(rowsUpdated(2L));
                    case "toString" -> "successful-batch-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "setAutoCommit", "close" -> Mono.empty();
                    case "commitTransaction" -> Mono.fromRunnable(commits::incrementAndGet);
                    case "rollbackTransaction" -> Mono.fromRunnable(rollbacks::incrementAndGet);
                    case "createStatement" -> statement[0];
                    case "toString" -> "successful-batch-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
