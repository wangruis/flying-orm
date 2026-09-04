package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchChunkExecutionFactCompressionTest {

    @Test
    void keepsContiguousSuccessCompressedAndMaterializesOffsetsOnlyOnRequest() {
        BatchChunkExecutionFact large = BatchChunkExecutionFact.allSuccessful(
                7, 4_000_000_000L, 1_000_000, BatchAffectedRows.unknown());

        assertEquals(1_000_000, large.successfulCount());
        assertEquals(0, large.failedCount());

        BatchChunkExecutionFact small = BatchChunkExecutionFact.allSuccessful(
                2, 4_000_000_000L, 3, BatchAffectedRows.known(3L));
        assertEquals(List.of(4_000_000_000L, 4_000_000_001L, 4_000_000_002L),
                     small.successfulOffsets());
        assertEquals(List.of(), small.failedOffsets());
    }
}
