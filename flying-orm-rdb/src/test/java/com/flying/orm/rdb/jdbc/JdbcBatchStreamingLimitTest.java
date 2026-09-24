package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBatchStreamingLimitTest {

    @Test
    void rejectedRowsDoNotEnterEvidenceButAcceptedUnexecutedTailDoes() {
        for (int maximum : new int[]{1, 2, 3}) {
            JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
            java.util.concurrent.atomic.AtomicInteger cancelled = new java.util.concurrent.atomic.AtomicInteger();
            BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                    "insert into samples(value) values (?)", 1, List.of(Integer.class),
                    SqlBindMarkerStyle.CANONICAL,
                    Flux.range(0, maximum + 1).map(value -> new Object[]{value})
                            .doOnCancel(cancelled::incrementAndGet),
                    BatchWriteOptions.of(2).withMaxRows(maximum));
            com.flying.orm.rdb.batch.BatchExecutionEvidenceException failure = assertThrows(
                    com.flying.orm.rdb.batch.BatchExecutionEvidenceException.class,
                    () -> state.writer().writeBatch(request));
            assertInstanceOf(BatchMemoryLimitExceededException.class, failure.getCause());
            assertEquals(maximum, failure.evidence().inputCount());
            assertEquals(maximum / 2 * 2, failure.evidence().successfulCount());
            assertEquals(maximum / 2, state.executions.get());
            assertEquals(maximum / 2, state.released.get());
            assertEquals(1, cancelled.get());
        }
    }

    @Test
    void unlimitedRowPolicyStillReadsBoundedChunks() throws Exception {
        BatchWriteRequest request = request(BatchWriteOptions.of(2).withMaxRows(0));

        try (JdbcBatchRows rows = rows(request)) {

            List<ProtectedBatchRows.RowView> first = JdbcBatchSupport.readBuffer(
                    rows, request);
            List<ProtectedBatchRows.RowView> second = JdbcBatchSupport.readBuffer(
                    rows, request);

            assertEquals(2, first.size());
            assertEquals(1, second.size());
        }
    }

    @Test
    void explicitTotalRowLimitRemainsEnforced() throws Exception {
        BatchWriteRequest request = request(BatchWriteOptions.of(2).withMaxRows(2));

        try (JdbcBatchRows rows = rows(request)) {

            assertEquals(2, JdbcBatchSupport.readBuffer(
                    rows, request).size());
            assertThrows(BatchMemoryLimitExceededException.class,
                         () -> JdbcBatchSupport.readBuffer(
                                 rows, request));
        }
    }

    @Test
    void startsANewChunkBeforeRequestingAnotherRowAtTheByteBudget() throws Exception {
        Object[] first = {new byte[64]};
        Object[] second = {new byte[64]};
        long oneRowBytes = BatchRowSnapshotter.snapshotAndEstimate(
                first, 1, Long.MAX_VALUE, "test budget").estimatedBytes();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(byte[].class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(first, second),
                BatchWriteOptions.of(2).withMemoryLimits(2, oneRowBytes)
                        .withMaxRowBytes(oneRowBytes));

        try (JdbcBatchRows rows = rows(request)) {

            List<ProtectedBatchRows.RowView> firstChunk = JdbcBatchSupport.readBuffer(
                    rows, request);
            List<ProtectedBatchRows.RowView> secondChunk = JdbcBatchSupport.readBuffer(
                    rows, request);

            assertEquals(1, firstChunk.size());
            assertEquals(1, secondChunk.size());
        }
    }

    @Test
    void carriesTheValidatedRowViewThroughReadChunkAndBinder() throws Exception {
        Object[] inputRow = {"value"};
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(inputRow),
                BatchWriteOptions.of(1));
        List<Object> bound = new ArrayList<>();

        try (JdbcBatchRows rows = rows(request)) {
            var chunk = JdbcBatchSupport.readBuffer(
                    rows, request);

            assertInstanceOf(ProtectedBatchRows.RowView.class, chunk.getFirst());
            new JdbcBatchChunkExecutor().execute(
                    connection(bound), request, 0L, chunk,
                    new JdbcBatchEvidenceSupport.Counts(0L, chunk.size(), request.rowCountPolicy()));
        }

        assertEquals(List.of("value"), bound);
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.range(0, 3).map(value -> new Object[]{value}),
                options);
    }

    private static JdbcBatchRows rows(BatchWriteRequest request) {
        return new JdbcBatchRows(
                request.rows(), request.parameterCount(), request.options().maxRowBytes(),
                request.options().maxRows(), new com.flying.orm.rdb.batch.BatchExecutionEvidence.Accumulator());
    }

    private static Connection connection(List<Object> bound) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "setObject" -> {
                        bound.add(arguments[1]);
                        yield null;
                    }
                    case "addBatch", "close", "setQueryTimeout" -> null;
                    case "executeBatch" -> new int[]{1};
                    case "toString" -> "capturing batch statement";
                    default -> defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "prepareStatement" -> statement;
                    case "toString" -> "capturing batch connection";
                    default -> defaultValue(method.getReturnType());
                });
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
}
