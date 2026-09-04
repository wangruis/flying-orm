package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchCleanupObserverTest {

    @Test
    void skipsDisabledCleanupAfterAtomicCommit() {
        assertCommitted(BatchWriteOptions.atomic(1), false);
    }

    @Test
    void skipsDisabledCleanupAfterIndependentCommit() {
        assertCommitted(BatchWriteOptions.independent(1, 1), false);
    }

    @Test
    void isolatesEnabledCleanupRuntimeFailureAfterCommit() {
        assertCommitted(BatchWriteOptions.atomic(1), true);
    }

    @Test
    void preservesEnabledCleanupErrorAfterCommit() {
        AssertionError failure = new AssertionError("cleanup observer error");
        Observer observer = new Observer(true, failure);
        Fixture fixture = new Fixture(observer);

        assertSame(failure, assertThrows(AssertionError.class,
                () -> fixture.writer.writeBatch(request(BatchWriteOptions.atomic(1)))));
        fixture.assertCommittedAndClosed();
        assertEquals(1, observer.cleanupCalls);
    }

    private static void assertCommitted(BatchWriteOptions options, boolean enabled) {
        Observer observer = new Observer(enabled, new IllegalStateException("cleanup observer failure"));
        Fixture fixture = new Fixture(observer);

        BatchWriteResult result = assertDoesNotThrow(() -> fixture.writer.writeBatch(request(options)));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, result.affectedRows());
        fixture.assertCommittedAndClosed();
        assertEquals(enabled ? 1 : 0, observer.cleanupCalls);
        if (!enabled) {
            assertEquals(0, observer.executionCalls);
        }
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return BatchWriteRequests.request("insert into events(id) values (?)", 1, List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL, Flux.<Object[]>just(new Object[]{1}), options);
    }

    private static final class Fixture {
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger executions = new AtomicInteger();
        private final JdbcBatchWriter writer;

        private Fixture(Observer observer) {
            PreparedStatement statement = proxy(PreparedStatement.class, (self, method, arguments) ->
                    switch (method.getName()) {
                        case "setObject", "setNull", "addBatch", "close" -> null;
                        case "executeBatch" -> {
                            executions.incrementAndGet();
                            yield new int[]{1};
                        }
                        default -> throw new AssertionError("unexpected statement call: " + method.getName());
                    });
            Connection connection = proxy(Connection.class, (self, method, arguments) ->
                    switch (method.getName()) {
                        case "prepareStatement" -> statement;
                        case "getAutoCommit" -> true;
                        case "setAutoCommit" -> null;
                        case "commit" -> {
                            commits.incrementAndGet();
                            yield null;
                        }
                        case "rollback" -> {
                            rollbacks.incrementAndGet();
                            yield null;
                        }
                        case "close" -> {
                            closes.incrementAndGet();
                            throw new SQLException("connection close failed", "08006");
                        }
                        default -> throw new AssertionError("unexpected connection call: " + method.getName());
                    });
            DataSource source = proxy(DataSource.class, (self, method, arguments) -> {
                if (method.getName().equals("getConnection")) {
                    return connection;
                }
                throw new AssertionError("unexpected data source call: " + method.getName());
            });
            writer = JdbcBatchWriter.create(source).withBatchObserver(observer);
        }

        private void assertCommittedAndClosed() {
            assertEquals(1, executions.get());
            assertEquals(1, commits.get());
            assertEquals(0, rollbacks.get());
            assertEquals(1, closes.get());
        }
    }

    private static final class Observer implements BatchExecutionObserver, SqlExecutionObserver {
        private final boolean enabled;
        private final Throwable failure;
        private int cleanupCalls;
        private int executionCalls;

        private Observer(boolean enabled, Throwable failure) {
            this.enabled = enabled;
            this.failure = failure;
        }

        @Override public boolean enabled() { return enabled; }
        @Override public void onExecution(BatchExecutionObservation observation) { executionCalls++; }
        @Override public void onExecution(SqlExecutionObservation observation) { executionCalls++; }

        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            assertTrue(observation.outcomeConfirmed());
            cleanupCalls++;
            if (failure instanceof Error error) {
                throw error;
            }
            throw (RuntimeException) failure;
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
