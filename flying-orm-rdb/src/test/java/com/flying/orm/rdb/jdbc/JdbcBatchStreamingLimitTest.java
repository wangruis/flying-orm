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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBatchStreamingLimitTest {

    @Test
    void unlimitedRowPolicyStillReadsBoundedChunks() throws Exception {
        BatchWriteRequest request = request(BatchWriteOptions.unlimitedAtomic(2));

        try (JdbcBatchRows rows = rows(request)) {
            JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
            List<ProtectedBatchRows.RowView> first = JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, deadline(), progress);
            List<ProtectedBatchRows.RowView> second = JdbcBatchSupport.readChunk(
                    rows, request, 2L, 1, deadline(), progress);

            assertEquals(2, first.size());
            assertEquals(1, second.size());
        }
    }

    @Test
    void explicitTotalRowLimitRemainsEnforced() throws Exception {
        BatchWriteRequest request = request(BatchWriteOptions.atomic(2).withMaxRows(2));

        try (JdbcBatchRows rows = rows(request)) {
            JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
            assertEquals(2, JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, deadline(), progress).size());
            assertThrows(BatchMemoryLimitExceededException.class,
                         () -> JdbcBatchSupport.readChunk(
                                 rows, request, 2L, 1, deadline(), progress));
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
                BatchWriteOptions.atomic(2).withMemoryLimits(2, oneRowBytes, 2)
                        .withMaxRowBytes(oneRowBytes));

        try (JdbcBatchRows rows = rows(request)) {
            JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
            List<ProtectedBatchRows.RowView> firstChunk = JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, deadline(), progress);
            List<ProtectedBatchRows.RowView> secondChunk = JdbcBatchSupport.readChunk(
                    rows, request, 1L, 1, deadline(), progress);

            assertEquals(1, firstChunk.size());
            assertEquals(1, secondChunk.size());
        }
    }

    @Test
    void carriesTheValidatedRowViewThroughReadChunkSideIndexAndBinder() throws Exception {
        Object[] protectedRow = ProtectedBatchRows.extend(
                new Object[]{"value"}, null, new Object[]{"stable-receipt"});
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(protectedRow),
                BatchWriteOptions.atomic(1));
        List<Object> bound = new ArrayList<>();

        try (JdbcBatchRows rows = rows(request)) {
            var chunk = JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, deadline(), new JdbcBatchSupport.ChunkReadProgress());

            assertInstanceOf(ProtectedBatchRows.RowView.class, chunk.getFirst());
            new JdbcBatchChunkExecutor().execute(
                    connection(bound), request, 0, 0L, chunk, deadline());
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
                request.rows(), request.parameterCount(), request.options().maxRowBytes());
    }

    private static JdbcBatchSupport.BatchDeadline deadline() {
        return JdbcBatchSupport.BatchDeadline.start(Duration.ZERO);
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
