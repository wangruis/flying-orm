package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class JdbcOrdinaryBatchCompletionTest {
    @Test
    void strictConflictsPublishSparseCompletedRowsAfterStatementClose() {
        Harness harness = new Harness(new int[]{1, 0, 1});
        List<Long> completed = new ArrayList<>();
        var failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> harness.writer().writeBatch(request(3), offset -> {
                    assertEquals(1, harness.closes.get());
                    completed.add(offset);
                }));
        assertEquals(List.of(0L, 2L), completed);
        assertEquals(completed, failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(1, harness.releases.get());
    }

    @Test
    void partialDriverFailureRetainsSparseCompletionAndAggregatesPostFailures() {
        Harness harness = new Harness(new int[]{1, Statement.EXECUTE_FAILED, 1});
        harness.failure = new BatchUpdateException("partial update", "HY000", 0, harness.counts);
        var preexisting = new IllegalArgumentException("preexisting detail");
        harness.failure.addSuppressed(preexisting);
        List<Long> completed = new ArrayList<>();
        var postFailure = new IllegalStateException("post failed");
        var failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> harness.writer().writeBatch(request(3), offset -> {
                    completed.add(offset);
                    if (offset == 0) throw postFailure;
                }));
        assertEquals(List.of(0L, 2L), completed);
        assertSame(harness.failure, failure.getCause());
        assertTrue(List.of(harness.failure.getSuppressed()).contains(postFailure));
        assertTrue(List.of(harness.failure.getSuppressed()).contains(preexisting));
        assertEquals(1, harness.closes.get());
        assertEquals(1, harness.releases.get());
    }

    @Test
    void strictUnknownCountDoesNotQualifyButOtherKnownRowsDo() {
        Harness harness = new Harness(new int[]{1, Statement.SUCCESS_NO_INFO, 1});
        List<Long> completed = new ArrayList<>();
        var failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> harness.writer().writeBatch(request(3), completed::add));
        assertEquals(List.of(0L, 2L), completed);
        assertFalse(failure.evidence().affectedRows().isKnown());
        assertEquals(3, failure.evidence().successfulCount());
    }

    @Test
    void closeFailurePreventsEveryCompletionAndKeepsSqlError() {
        Harness harness = new Harness(new int[]{1, Statement.EXECUTE_FAILED, 1});
        harness.failure = new BatchUpdateException("partial update", "HY000", 0, harness.counts);
        harness.closeFailure = new SQLException("close failed");
        List<Long> completed = new ArrayList<>();
        var failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> harness.writer().writeBatch(request(3), completed::add));
        assertTrue(completed.isEmpty());
        assertSame(harness.failure, failure.getCause());
        assertTrue(List.of(harness.failure.getSuppressed()).contains(harness.closeFailure));
        assertEquals(1, harness.closes.get());
        assertEquals(1, harness.releases.get());
    }

    private static BatchWriteRequest request(int size) {
        var base = BatchWriteRequests.request("update samples set value_col = ?", 1,
                List.of(Integer.class), SqlBindMarkerStyle.CANONICAL,
                Flux.range(0, size).map(value -> new Object[]{value}), BatchWriteOptions.of(size));
        return new BatchWriteRequest(base.statement(), base.parameterTypes(), base.rows(), base.options(),
                BatchRowCountPolicy.EXACTLY_ONE, BatchGeneratedKeys.none());
    }

    private static final class Harness {
        final int[] counts;
        final AtomicInteger closes = new AtomicInteger();
        final AtomicInteger releases = new AtomicInteger();
        BatchUpdateException failure;
        SQLException closeFailure;

        Harness(int[] counts) { this.counts = counts; }

        JdbcBatchWriter writer() {
            PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "setObject", "addBatch", "clearParameters", "cancel" -> null;
                        case "executeBatch" -> {
                            if (failure != null) throw failure;
                            yield counts;
                        }
                        case "close" -> {
                            closes.incrementAndGet();
                            if (closeFailure != null) throw closeFailure;
                            yield null;
                        }
                        default -> throw new AssertionError(method.getName());
                    });
            Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        if (method.getName().equals("prepareStatement")) return statement;
                        throw new AssertionError(method.getName());
                    });
            return JdbcBatchWriter.create(new JdbcConnectionAccess() {
                @Override public Connection getConnection(SqlRequest request) { return connection; }
                @Override public void releaseConnection(Connection value, SqlRequest request) {
                    assertSame(connection, value);
                    releases.incrementAndGet();
                }
            }, RdbDialect.h2());
        }
    }
}
