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
import java.time.Duration;
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
            JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
            List<ProtectedBatchRows.RowView> full = JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, deadline(), progress);
            assertEquals(500, full.size());
            assertEquals(500, progress.acceptedRows());
            List<ProtectedBatchRows.RowView> eof = JdbcBatchSupport.readChunk(
                    rows, request, 500L, 1, deadline(), progress);

            assertTrue(eof.isEmpty());
            assertEquals(0, progress.acceptedRows());
        }
    }

    @Test
    void emptyInputRetainsItsEofShape() throws Exception {
        BatchWriteRequest request = request(0, regularOptions(500));
        try (JdbcBatchRows rows = rows(request)) {
            assertTrue(JdbcBatchSupport.readChunk(rows, request, 0L, 0, deadline(),
                    new JdbcBatchSupport.ChunkReadProgress()).isEmpty());
        }
    }

    @Test
    void singleRowInputRetainsItsEofShape() throws Exception {
        BatchWriteRequest request = request(1, regularOptions(500));
        try (JdbcBatchRows rows = rows(request)) {
            JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
            assertEquals(1, JdbcBatchSupport.readChunk(rows, request, 0L, 0, deadline(), progress).size());
            assertEquals(1, progress.acceptedRows());
            assertTrue(JdbcBatchSupport.readChunk(rows, request, 1L, 1, deadline(), progress).isEmpty());
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
                List<ProtectedBatchRows.RowView> chunk = JdbcBatchSupport.readChunk(rows, request, accepted,
                        chunkIndex, deadline(), new JdbcBatchSupport.ChunkReadProgress());
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
        return BatchWriteOptions.atomic(chunkSize)
                .withMemoryLimits(rowCount, 8192L, rowCount)
                .withMaxRowBytes(8192L);
    }

    private static BatchWriteOptions regularOptions(int chunkSize) {
        return BatchWriteOptions.atomic(chunkSize)
                .withMemoryLimits(500, 1_048_576L, 500)
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

    private static JdbcBatchSupport.BatchDeadline deadline() {
        return JdbcBatchSupport.BatchDeadline.start(Duration.ZERO);
    }
}
