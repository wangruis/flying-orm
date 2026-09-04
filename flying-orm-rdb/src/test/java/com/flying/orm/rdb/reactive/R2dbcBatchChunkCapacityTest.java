package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class R2dbcBatchChunkCapacityTest {
    private static final ThreadMXBean ALLOCATIONS =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();

    @Test
    void byteDrainedChunksDoNotPreallocateUnusedLogicalCapacity() {
        assumeTrue(ALLOCATIONS.isThreadAllocatedMemorySupported());
        ALLOCATIONS.setThreadAllocatedMemoryEnabled(true);

        drainByteLimitedChunks(16, 1);
        drainByteLimitedChunks(16, 500);
        long oneRowCapacity = drainByteLimitedChunks(128, 1);
        long largeLogicalCapacity = drainByteLimitedChunks(128, 500);

        assertTrue(largeLogicalCapacity <= oneRowCapacity + 100_000L,
                () -> "byte-drained R2DBC chunks retained "
                        + (largeLogicalCapacity - oneRowCapacity) + " excess allocation bytes");
    }

    @Test
    void fullLogicalChunkRetainsItsSingleChunkShape() {
        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request(500, regularOptions(500)))
                .collectList().block();

        assertEquals(1, chunks.size());
        assertEquals(500, chunks.getFirst().rows().size());
        assertEquals(0L, chunks.getFirst().startOffset());
    }

    @Test
    void emptyInputRetainsItsEofShape() {
        assertTrue(R2dbcBatchChunker.chunks(request(0, regularOptions(500))).collectList().block().isEmpty());
    }

    @Test
    void singleRowInputRetainsItsEofShape() {
        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request(1, regularOptions(500)))
                .collectList().block();

        assertEquals(1, chunks.size());
        assertEquals(1, chunks.getFirst().rows().size());
        assertEquals(0L, chunks.getFirst().startOffset());
    }

    private static long drainByteLimitedChunks(int rowCount, int chunkSize) {
        long threadId = Thread.currentThread().threadId();
        long before = ALLOCATIONS.getThreadAllocatedBytes(threadId);
        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(
                request(rowCount, byteLimitedOptions(rowCount, chunkSize))).collectList().block();

        assertEquals(rowCount, chunks.size());
        assertTrue(chunks.stream().allMatch(chunk -> chunk.rows().size() == 1));
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
}
