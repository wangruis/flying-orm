package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class JdbcBatchChunkCapacityTest {
    private static final ThreadMXBean ALLOCATIONS =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @Test
    void byteDrainedChunksDoNotPreallocateUnusedLogicalCapacity() throws Exception {
        assumeTrue(ALLOCATIONS.isThreadAllocatedMemorySupported());
        ALLOCATIONS.setThreadAllocatedMemoryEnabled(true);

        drainByteLimitedChunks(16, 1);
        drainByteLimitedChunks(16, 500);
        long oneRowCapacity = drainByteLimitedChunks(128, 1);
        long largeLogicalCapacity = drainByteLimitedChunks(128, 500);

        assertTrue(largeLogicalCapacity <= oneRowCapacity + 100_000L,
                () -> "byte-drained JDBC chunks retained "
                        + (largeLogicalCapacity - oneRowCapacity) + " excess allocation bytes");
    }

    @Test
    void fullLogicalChunkRetainsItsSingleChunkShape() throws Exception {
        BatchWriteRequest request = request(500, regularOptions(500));
        try (JdbcBatchRows rows = rows(request)) {

            List<ProtectedBatchRows.RowView> full = JdbcBatchSupport.readBuffer(
                    rows, request);
            assertEquals(500, full.size());

            List<ProtectedBatchRows.RowView> eof = JdbcBatchSupport.readBuffer(
                    rows, request);

            assertTrue(eof.isEmpty());

        }
    }

    @Test
    void emptyInputRetainsItsEofShape() throws Exception {
        BatchWriteRequest request = request(0, regularOptions(500));
        try (JdbcBatchRows rows = rows(request)) {
            assertTrue(JdbcBatchSupport.readBuffer(rows, request).isEmpty());
        }
    }

    @Test
    void singleRowInputRetainsItsEofShape() throws Exception {
        BatchWriteRequest request = request(1, regularOptions(500));
        try (JdbcBatchRows rows = rows(request)) {

            assertEquals(1, JdbcBatchSupport.readBuffer(rows, request).size());

            assertTrue(JdbcBatchSupport.readBuffer(rows, request).isEmpty());
        }
    }

    private static long drainByteLimitedChunks(int rowCount, int chunkSize) throws Exception {
        BatchWriteRequest request = request(rowCount, byteLimitedOptions(rowCount, chunkSize));
        long threadId = Thread.currentThread().threadId();
        long before = ALLOCATIONS.getThreadAllocatedBytes(threadId);
        int accepted = 0;
        int chunkIndex = 0;
        try (JdbcBatchRows rows = rows(request)) {
            while (true) {
                List<ProtectedBatchRows.RowView> chunk = JdbcBatchSupport.readBuffer(rows, request);
                if (chunk.isEmpty()) {
                    break;
                }
                assertEquals(1, chunk.size());
                accepted += chunk.size();
                chunkIndex++;
            }
        }
        assertEquals(rowCount, accepted);
        return ALLOCATIONS.getThreadAllocatedBytes(threadId) - before;
    }

    private static BatchWriteOptions byteLimitedOptions(int rowCount, int chunkSize) {
        return BatchWriteOptions.of(chunkSize)
                .withMemoryLimits(rowCount, 8192L)
                .withMaxRowBytes(8192L);
    }

    private static BatchWriteOptions regularOptions(int chunkSize) {
        return BatchWriteOptions.of(chunkSize)
                .withMemoryLimits(500, 1_048_576L)
                .withMaxRowBytes(8192L);
    }

    private static BatchWriteRequest request(int rowCount, BatchWriteOptions options) {
        return BatchWriteRequests.request("insert into samples(value) values (?)", 1,
                List.of(Integer.class), SqlBindMarkerStyle.CANONICAL,
                Flux.range(0, rowCount).map(value -> new Object[]{value}), options);
    }

    private static JdbcBatchRows rows(BatchWriteRequest request) {
        return new JdbcBatchRows(request.rows(), request.parameterCount(), request.options().maxRowBytes());
    }

}
