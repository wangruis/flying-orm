package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BatchWriteOptionsTest {
    @Test
    void defaultsRetainBoundedBatchCapacity() {
        assertEquals(500, BatchWriteOptions.defaults().bufferSize());
        assertEquals(100_000, BatchWriteOptions.defaults().maxRows());
        assertEquals(32L * 1024 * 1024, BatchWriteOptions.defaults().maxBufferedBytes());
        assertEquals(10_000, BatchMemoryLimits.defaults().maxBufferSize());
        assertEquals(256L * 1024 * 1024, BatchMemoryLimits.defaults().maxBufferedBytes());
    }

    @Test
    void unlimitedRowsKeepBufferAndByteLimits() {
        BatchWriteOptions options = BatchWriteOptions.of(64).withMaxRows(0);
        assertEquals(0, options.maxRows());
        assertEquals(64, options.bufferSize());
        assertTrue(options.maxBufferedBytes() > 0);
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRows(-1));
    }

    @Test
    void processMemoryLimitsCheckOnlyInputBufferCapacity() {
        BatchWriteOptions options = BatchWriteOptions.of(2).withMemoryLimits(0, 128);
        assertDoesNotThrow(() -> new BatchMemoryLimits(16, 256).check(options));
        assertThrows(BatchMemoryLimitExceededException.class, () -> new BatchMemoryLimits(16, 64).check(options));
        assertThrows(BatchMemoryLimitExceededException.class, () -> new BatchMemoryLimits(1, 256).check(options));
    }

    @Test
    void reservesHalfOfBufferUnlessItHasOneRow() {
        assertEquals(512, BatchWriteOptions.of(500).withMemoryLimits(0, 1024).maxRowBytes());
        assertEquals(1024, BatchWriteOptions.of(1).withMemoryLimits(0, 1024).maxRowBytes());
        assertEquals(1, BatchWriteOptions.of(500).withMemoryLimits(0, 1).maxRowBytes());
        assertEquals(Long.MAX_VALUE / 2, BatchWriteOptions.of(500)
                .withMemoryLimits(0, Long.MAX_VALUE).maxRowBytes());
    }

    @Test
    void explicitRowLimitSurvivesUnrelatedOptionChanges() {
        BatchWriteOptions options = BatchWriteOptions.of(500)
                .withMemoryLimits(0, 4096).withMaxRowBytes(1024).withMaxRows(100);
        assertEquals(1024, options.maxRowBytes());
        assertEquals(512, options.withMemoryLimits(100, 1024).maxRowBytes());
    }

    @Test
    void rejectsInvalidBufferOrRowLimits() {
        BatchWriteOptions options = BatchWriteOptions.of(500).withMemoryLimits(0, 1024);
        assertDoesNotThrow(() -> options.withMaxRowBytes(1024));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(1025));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(0));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> options.withMemoryLimits(0, 0));
        assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.of(0));
        assertThrows(IllegalArgumentException.class, () -> new BatchMemoryLimits(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new BatchMemoryLimits(1, 0));
    }
}
