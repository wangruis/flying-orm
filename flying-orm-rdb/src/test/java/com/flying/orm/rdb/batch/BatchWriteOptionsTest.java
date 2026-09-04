package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchWriteOptionsTest {

    @Test
    void leavesBatchLifetimeToTheCallerByDefault() {
        assertTrue(BatchWriteOptions.defaults().timeout().isZero());
    }

    @Test
    void unlimitedFactoriesRemoveOnlyTheTotalRowAndTimeLimits() {
        BatchWriteOptions atomic = BatchWriteOptions.unlimitedAtomic(64);
        BatchWriteOptions independent = BatchWriteOptions.unlimitedIndependent(64, 4);

        assertEquals(0L, atomic.maxRows());
        assertEquals(0L, independent.maxRows());
        assertTrue(atomic.timeout().isZero());
        assertTrue(independent.timeout().isZero());
        assertTrue(atomic.chunkSize() > 0);
        assertTrue(independent.concurrency() > 0);
        assertTrue(atomic.maxBufferedBytes() > 0L);
        assertTrue(independent.maxResultChunks() > 0);
    }

    @Test
    void zeroMeansNoTotalRowLimitButNegativeValuesRemainInvalid() {
        assertEquals(0L, BatchWriteOptions.atomic(32).withMaxRows(0).maxRows());
        assertThrows(IllegalArgumentException.class,
                     () -> BatchWriteOptions.atomic(32).withMaxRows(-1));
    }

    @Test
    void processMemoryLimitsDoNotMaintainASecondTotalRowPolicy() {
        BatchWriteOptions options = BatchWriteOptions.atomic(2)
                .withMemoryLimits(1_000, 128, 4);

        assertDoesNotThrow(() -> new BatchMemoryLimits(16, 4, 1, 256, 16).check(options));
        assertThrows(BatchMemoryLimitExceededException.class,
                     () -> new BatchMemoryLimits(16, 4, 10_000, 64, 16).check(options));
    }

    @Test
    void reservesHalfOfEachChunkByDefaultUnlessChunksHaveOneRow() {
        assertEquals(512, BatchWriteOptions.atomic(500).withMemoryLimits(0, 1024, 16).maxRowBytes());
        assertEquals(1024, BatchWriteOptions.atomic(1).withMemoryLimits(0, 1024, 16).maxRowBytes());
        assertEquals(128, BatchWriteOptions.independent(500, 4)
                .withMemoryLimits(0, 1024, 16).maxRowBytes());
        assertEquals(Long.MAX_VALUE / 2, BatchWriteOptions.atomic(500)
                .withMemoryLimits(0, Long.MAX_VALUE, 16).maxRowBytes());
    }

    @Test
    void explicitRowLimitSurvivesUnrelatedOptionChanges() {
        BatchWriteOptions options = BatchWriteOptions.independent(500, 4)
                .withMemoryLimits(0, 4096, 16).withMaxRowBytes(1024)
                .withMaxRows(100).withTimeout(Duration.ofSeconds(1)).withReceipt("budget-test");

        assertEquals(1024, options.maxRowBytes());
        assertEquals(128, options.withMemoryLimits(100, 1024, 16).maxRowBytes());
    }

    @Test
    void rejectsRowLimitsWhichCannotFitTheirConcurrentChunk() {
        BatchWriteOptions options = BatchWriteOptions.independent(500, 4)
                .withMemoryLimits(0, 1024, 16);

        assertDoesNotThrow(() -> options.withMaxRowBytes(256));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(257));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(0));
        assertThrows(IllegalArgumentException.class, () -> options.withMaxRowBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> options.withMemoryLimits(0, 3, 16));
        assertThrows(IllegalArgumentException.class, () -> BatchWriteOptions.independent(500, 0));
    }
}
