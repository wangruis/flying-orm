package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;
import java.util.BitSet;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class BatchExecutionEvidenceCompressionTest {
    @Test
    void successfulPrefixSupportsLongOffsetsWithoutMaterialization() {
        var facts = new BatchExecutionEvidence.Accumulator();
        long count = (long) Integer.MAX_VALUE + 2_000_001;
        facts.accept(count);
        facts.succeeded(count, BatchAffectedRows.unknown());
        var result = facts.snapshot(BatchExecutionState.SUCCESS, null);
        assertEquals(count, result.inputCount());
        assertEquals(count, result.successfulCount());
        assertTrue(result.isSuccessful(count - 1));
        assertFalse(result.isSuccessful(count));
        assertFalse(result.isSuccessful(-1));
        assertEquals(count, result.successfulOffsets().count());
        assertArrayEquals(new long[]{0, 1, 2}, result.successfulOffsets().limit(3).toArray());
        assertFalse(result.affectedRows().isKnown());
    }

    @Test
    void terminalSparseFactsPreserveAcceptedButUnprovenTailAndSnapshots() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(12);
        facts.succeeded(5, BatchAffectedRows.known(5));
        BitSet successes = bits(0, 2);
        BitSet failures = bits(1);
        facts.terminal(4, successes, failures, BatchAffectedRows.known(2),
                       List.of(BatchRowConflict.exactlyOne(6, 0)));
        successes.clear();
        failures.set(3);
        var result = facts.snapshot(BatchExecutionState.PARTIAL, null);
        assertEquals(12, result.inputCount());
        assertEquals(7, result.successfulCount());
        assertEquals(1, result.failedCount());
        assertEquals(7, result.affectedRows().value());
        assertArrayEquals(new long[]{0, 1, 2, 3, 4, 5, 7}, result.successfulOffsets().toArray());
        assertArrayEquals(new long[]{6}, result.failedOffsets().toArray());
        assertFalse(result.isSuccessful(8));
        assertFalse(result.isFailed(8));
        assertFalse(result.isFailed(11));
        assertThrows(UnsupportedOperationException.class, () -> result.conflicts().clear());
        facts.accept(1);
        assertEquals(12, result.inputCount());
        assertThrows(IllegalStateException.class, () -> facts.succeeded(1, BatchAffectedRows.known(1)));
    }

    @Test
    void successfulSqlRemainsProvenWhenSubsequentWorkFails() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(3);
        facts.succeeded(3, BatchAffectedRows.known(3));
        var failure = BatchExecutionEvidence.Failure.from(new IllegalStateException("private value"));
        var result = facts.snapshot(BatchExecutionState.FAILED, failure);
        assertEquals(3, result.successfulCount());
        assertSame(failure, result.failure());
        assertFalse(result.failure().message().contains("private"));
    }

    @Test
    void validatesCountsRangesOverlapAndConflictsBeforeChangingFacts() {
        var facts = new BatchExecutionEvidence.Accumulator();
        assertThrows(IllegalArgumentException.class, () -> facts.accept(-1));
        assertThrows(IllegalArgumentException.class, () -> facts.succeeded(-1, BatchAffectedRows.known(0)));
        assertThrows(IllegalArgumentException.class, () -> facts.succeeded(1, BatchAffectedRows.known(1)));
        facts.accept(4);
        assertThrows(IllegalArgumentException.class, () -> facts.terminal(5, bits(), bits(), BatchAffectedRows.known(0), List.of()));
        assertThrows(IllegalArgumentException.class, () -> facts.terminal(4, bits(1), bits(1), BatchAffectedRows.known(0), List.of()));
        assertThrows(IllegalArgumentException.class, () -> facts.terminal(4, bits(4), bits(), BatchAffectedRows.known(0), List.of()));
        assertThrows(IllegalArgumentException.class, () -> facts.terminal(4, bits(), bits(), BatchAffectedRows.known(0), List.of(BatchRowConflict.exactlyOne(4, 0))));
        facts.succeeded(4, BatchAffectedRows.known(4));
        assertEquals(4, facts.snapshot(BatchExecutionState.SUCCESS, null).successfulCount());
    }

    @Test
    void countOverflowFailsBeforeMutation() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(Long.MAX_VALUE);
        assertThrows(RuntimeException.class, () -> facts.accept(1));
        facts.succeeded(1, BatchAffectedRows.known(Long.MAX_VALUE));
        assertThrows(RuntimeException.class, () -> facts.succeeded(1, BatchAffectedRows.known(1)));
        var snapshot = facts.snapshot(BatchExecutionState.PARTIAL, null);
        assertEquals(Long.MAX_VALUE, snapshot.inputCount());
        assertEquals(1, snapshot.successfulCount());
        assertEquals(Long.MAX_VALUE, snapshot.affectedRows().value());
    }

    @Test
    void conflictsCannotRetainUnboundedDuplicatesForOnePosition() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(1);
        var conflict = BatchRowConflict.exactlyOne(0, 0);
        assertThrows(IllegalArgumentException.class, () -> facts.terminal(1, bits(), bits(0),
                BatchAffectedRows.known(0), List.of(conflict, conflict)));
    }

    @Test
    void evidenceExceptionReportsExecutionAndSupportsNoncyclicAttachments() {
        var evidence = new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.FAILED, null);
        var exception = new BatchExecutionEvidenceException("private detail", null, evidence);
        assertSame(evidence, exception.evidence());
        assertNull(exception.getCause());
        assertEquals("BATCH", exception.toErrorReport().category());
        assertEquals("EXECUTION_FAILED", exception.toErrorReport().code());
        assertEquals("EXECUTION", exception.toErrorReport().resource());
        assertEquals("batch execution failed", exception.toErrorReport().message());
    }

    @Test
    void terminalBitsAtLongOffsetsDoNotMaterializeThePrefix() {
        var facts = new BatchExecutionEvidence.Accumulator();
        long prefix = 4_000_000_000L;
        facts.accept(prefix + 4);
        facts.succeeded(prefix, BatchAffectedRows.unknown());
        facts.terminal(4, bits(1, 3), bits(2), BatchAffectedRows.unknown(), List.of());
        var result = facts.snapshot(BatchExecutionState.PARTIAL, null);
        assertEquals(prefix + 2, result.successfulCount());
        assertTrue(result.isSuccessful(prefix + 3));
        assertFalse(result.isSuccessful(prefix));
        assertTrue(result.isFailed(prefix + 2));
        assertArrayEquals(new long[]{prefix + 2}, result.failedOffsets().toArray());
        assertThrows(IllegalStateException.class, () -> facts.terminal(0, bits(), bits(), BatchAffectedRows.known(0), List.of()));
    }

    private static BitSet bits(int... offsets) {
        var result = new BitSet();
        for (int offset : offsets) result.set(offset);
        return result;
    }
}
