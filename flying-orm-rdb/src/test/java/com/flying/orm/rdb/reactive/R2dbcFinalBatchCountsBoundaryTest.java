package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcFinalBatchCountsBoundaryTest {
    @Test void ownerReadAttemptIsNotAnUnfinishedBusinessWrite() {
        var counts = new R2dbcBatchEvidenceCounts(2, 0, BatchRowCountPolicy.EXACTLY_ONE, true);
        counts.markDatabaseWorkAttempted();
        var evidence = snapshot(counts, 2);
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(0, evidence.affectedRows().value());
        assertFalse(counts.hasReadyRows());
    }

    @Test void unexecutedTailDoesNotEraseCompletedBusinessFacts() {
        var counts = new R2dbcBatchEvidenceCounts(2, 0, BatchRowCountPolicy.EXACTLY_ONE, true);
        counts.startBusinessExecution();
        counts.recordRowCount(1);
        counts.completeRow(1);
        counts.rowReady();
        var evidence = snapshot(counts, 2);
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(1, evidence.affectedRows().value());
        assertTrue(counts.isRowReady(0));
        assertFalse(counts.isRowReady(1));
    }

    @Test void unknownStrictSqlCanBeSuccessfulWithoutPostEligibility() {
        var counts = new R2dbcBatchEvidenceCounts(3, 0, BatchRowCountPolicy.EXACTLY_ONE, true);
        for (long value : new long[]{1, -1, 1}) {
            counts.startBusinessExecution();
            counts.recordRowCount(value);
            counts.completeRow(value);
            counts.rowReady();
        }
        var evidence = snapshot(counts, 3);
        assertEquals(3, evidence.successfulCount());
        assertFalse(evidence.affectedRows().isKnown());
        assertTrue(counts.isRowReady(0));
        assertFalse(counts.isRowReady(1));
        assertTrue(counts.isRowReady(2));
    }

    @Test void noCallbackDoesNotTrackPartialEligibility() {
        var counts = new R2dbcBatchEvidenceCounts(3, 0, BatchRowCountPolicy.EXACTLY_ONE, false);
        for (long value : new long[]{1, 0, 1}) {
            counts.startBusinessExecution();
            counts.recordRowCount(value);
            counts.completeRow(value);
            counts.rowReady();
        }
        assertFalse(counts.hasReadyRows());
        assertEquals(2, snapshot(counts, 3).successfulCount());
    }

    private static BatchExecutionEvidence snapshot(R2dbcBatchEvidenceCounts counts, int size) {
        var accumulator = new BatchExecutionEvidence.Accumulator();
        accumulator.accept(size);
        counts.appendTo(accumulator, false);
        return accumulator.snapshot(BatchExecutionState.FAILED, null);
    }
}
