package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import org.reactivestreams.Publisher;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

final class JdbcBatchEvidenceTestSupport {

    private JdbcBatchEvidenceTestSupport() {
    }

    static BatchWriteRequest request(Publisher<Object[]> rows, int chunkSize) {
        return BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.atomic(chunkSize));
    }

    static final class State {

        final AtomicInteger acquired = new AtomicInteger();
        final AtomicInteger autoCommitReads = new AtomicInteger();
        final AtomicInteger autoCommitWrites = new AtomicInteger();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger rollbacks = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final AtomicInteger executions = new AtomicInteger();
        private final Deque<Object> outcomes = new ArrayDeque<>();

        State outcome(int... counts) {
            outcomes.addLast(counts);
            return this;
        }

        State failure(Throwable failure) {
            outcomes.addLast(failure);
            return this;
        }

        DataSource dataSource() {
            return proxy(DataSource.class, (self, method, arguments) -> {
                if (!method.getName().equals("getConnection")) {
                    throw new AssertionError("unexpected DataSource SPI: " + method);
                }
                acquired.incrementAndGet();
                return connection();
            });
        }

        Connection connection() {
            return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                case "getAutoCommit" -> { autoCommitReads.incrementAndGet(); yield true; }
                case "setAutoCommit" -> { autoCommitWrites.incrementAndGet(); yield null; }
                case "prepareStatement" -> statement();
                case "commit" -> { commits.incrementAndGet(); yield null; }
                case "rollback" -> { rollbacks.incrementAndGet(); yield null; }
                case "close" -> { closes.incrementAndGet(); yield null; }
                default -> throw new AssertionError("unexpected Connection SPI: " + method);
            });
        }

        private PreparedStatement statement() {
            AtomicInteger rows = new AtomicInteger();
            return proxy(PreparedStatement.class, (self, method, arguments) -> switch (method.getName()) {
                case "setObject", "setNull", "setQueryTimeout", "cancel", "close" -> null;
                case "addBatch" -> { rows.incrementAndGet(); yield null; }
                case "executeBatch" -> {
                    executions.incrementAndGet();
                    Object outcome = outcomes.pollFirst();
                    if (outcome instanceof Throwable failure) {
                        throw failure;
                    }
                    if (outcome instanceof int[] counts) {
                        yield counts;
                    }
                    int[] counts = new int[rows.get()];
                    Arrays.fill(counts, 1);
                    yield counts;
                }
                default -> throw new AssertionError("unexpected PreparedStatement SPI: " + method);
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
