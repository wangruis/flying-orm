package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcIndependentBatchResultIntegrityTest {

    @Test
    void preservesCommittedChunksWhenALaterConnectionCannotBeAcquired() {
        Connection first = connection(null, null);
        SQLException acquisitionFailure = new SQLException("second connection unavailable", "08001");
        AtomicInteger acquisitions = new AtomicInteger();
        DataSource dataSource = dataSource(() -> {
            if (acquisitions.getAndIncrement() == 0) {
                return first;
            }
            throw acquisitionFailure;
        });

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatch(request(1, 2)));

        BatchWriteResult result = error.result();
        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(2, result.inputCount());
        assertEquals(1, result.affectedRows());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED),
                     result.chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void preservesCommittedChunksWhenLaterInputFailsWithIllegalArgumentException() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("batch input failed");
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(inputFailure)),
                BatchWriteOptions.independent(1, 1));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource(() -> connection(null, null))).writeBatch(request));

        assertSame(inputFailure, error.getCause());
        assertEquals(BatchWriteResult.Status.PARTIAL, error.result().status());
        assertEquals(1, error.result().inputCount());
        assertEquals(1, error.result().affectedRows());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED),
                     error.result().chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void doesNotTrustBatchResultPublishedByTheRowSource() {
        BatchWriteException sourceFailure = new BatchWriteException(
                "failure from another batch",
                new IllegalStateException("prior batch failed"),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                        List.of(BatchChunkResult.committed(0, 0, 500, 500L))));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(sourceFailure)),
                BatchWriteOptions.independent(1, 1));

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource(() -> connection(null, null))).writeBatch(request));

        assertNotSame(sourceFailure, error);
        assertSame(sourceFailure, error.getCause());
        assertEquals(1, error.result().inputCount());
        assertEquals(1, error.result().affectedRows());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED),
                     error.result().chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void keepsUnknownCommitOutcomeWhenConnectionCloseAlsoFails() {
        SQLException commitFailure = new SQLException("commit acknowledgement lost", "08006");
        SQLException closeFailure = new SQLException("connection close failed", "08006");
        Connection connection = connection(commitFailure, closeFailure);

        BatchWriteResult result = JdbcBatchWriter.create(dataSource(() -> connection))
                .writeBatch(request(1, 1));

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(BatchChunkResult.Status.UNKNOWN, result.chunks().getFirst().status());
        assertArrayEquals(new Throwable[]{closeFailure}, commitFailure.getSuppressed());
    }

    @Test
    void reportsAcceptedChunkSizeWhenStatementPreparationTimesOut() {
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"name-0"}),
                BatchWriteOptions.independent(1, 1).withTimeout(Duration.ofMillis(5)));

        BatchWriteResult result = JdbcBatchWriter.create(dataSource(() -> connection(null, null, true)))
                .writeBatch(request);

        assertEquals(1, result.inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, result.chunks().getFirst().status());
    }

    @Test
    void rollsBackIndependentChunkInterruptedDuringBatchExecution() {
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();

        try {
            BatchWriteException failure = assertThrows(
                    BatchWriteException.class,
                    () -> JdbcBatchWriter.create(dataSource(
                                    () -> interruptedBatchConnection(commits, rollbacks)))
                            .writeBatch(request(1, 1)));
            BatchChunkResult chunk = failure.result().chunks().getFirst();

            assertEquals(BatchChunkResult.Status.FAILED, chunk.status());
            assertEquals(0L, chunk.affectedRows());
            assertEquals(RdbErrorKind.CANCELLED, chunk.failure().kind());
            assertEquals("HY008", chunk.failure().sqlState());
            assertEquals(0, commits.get());
            assertEquals(1, rollbacks.get());
        } finally {
            Thread.interrupted();
        }
    }

    private static BatchWriteRequest request(int chunkSize, int rows) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.range(0, rows).map(index -> new Object[]{"name-" + index}),
                BatchWriteOptions.independent(chunkSize, 1));
    }

    private static Connection connection(SQLException commitFailure, SQLException closeFailure) {
        return connection(commitFailure, closeFailure, false);
    }

    private static Connection connection(SQLException commitFailure,
                                         SQLException closeFailure,
                                         boolean slowExecution) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcIndependentBatchResultIntegrityTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    if ("executeBatch".equals(method.getName())) {
                        return new int[]{1};
                    }
                    return defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                JdbcIndependentBatchResultIntegrityTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "prepareStatement" -> {
                        if (slowExecution) {
                            Thread.sleep(25L);
                        }
                        yield statement;
                    }
                    case "commit" -> {
                        if (commitFailure != null) {
                            throw commitFailure;
                        }
                        yield null;
                    }
                    case "close" -> {
                        if (closeFailure != null) {
                            throw closeFailure;
                        }
                        yield null;
                    }
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Connection interruptedBatchConnection(AtomicInteger commits, AtomicInteger rollbacks) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcIndependentBatchResultIntegrityTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    if ("executeBatch".equals(method.getName())) {
                        Thread.currentThread().interrupt();
                        return new int[]{1};
                    }
                    return defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                JdbcIndependentBatchResultIntegrityTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "prepareStatement" -> statement;
                    case "commit" -> commits.incrementAndGet();
                    case "rollback" -> rollbacks.incrementAndGet();
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static DataSource dataSource(ConnectionSupplier connections) {
        return (DataSource) Proxy.newProxyInstance(
                JdbcIndependentBatchResultIntegrityTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connections.get() : defaultValue(method.getReturnType()));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }

    @FunctionalInterface
    private interface ConnectionSupplier {
        Connection get() throws SQLException;
    }
}
