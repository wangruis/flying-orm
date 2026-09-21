package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcFinalBatchFactsAndPostTest {
    @Test void incompleteSecondSqlMakesTotalUnknownButCompletesFirstPost() {
        for (boolean hasCount : List.of(false, true)) {
            var driver = new IllegalStateException("second SQL incomplete");
            var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> index == 0 ? Flux.just(1L)
                    : hasCount ? Flux.concat(Flux.just(1L), Flux.error(driver)) : Flux.error(driver));
            var posted = new ArrayList<Long>();
            var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                    R2dbcOrdinaryBatchConnectionTest.request(2, 2, BatchRowCountPolicy.EXACTLY_ONE),
                    offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
            assertFalse(error.evidence().affectedRows().isKnown());
            assertArrayEquals(new long[]{0}, error.evidence().successfulOffsets().toArray());
            assertEquals(2, error.evidence().inputCount());
            assertSame(driver, error.getCause());
            assertEquals(List.of(0L), posted);
            assertEquals(1, h.releases.size());
        }
    }

    @Test void cancelledSecondSqlMakesTotalUnknownWithoutPublishingPost() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> index == 0 ? Flux.just(1L)
                : Flux.concat(Flux.just(1L), Flux.never()));
        var terminal = new AtomicReference<BatchExecutionEvidence>();
        var posted = new ArrayList<Long>();
        var executor = h.executor().withBatchObserver(new BatchExecutionObserver() {
            public void onExecution(BatchExecutionObservation event) { }
            public void onExecutionEvidence(BatchExecutionEvidence evidence) { terminal.set(evidence); }
        });
        var subscription = executor.writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(2, 2, BatchRowCountPolicy.EXACTLY_ONE),
                offset -> Mono.fromRunnable(() -> posted.add(offset))).subscribe();
        subscription.dispose();
        assertNotNull(terminal.get());
        assertFalse(terminal.get().affectedRows().isKnown());
        assertArrayEquals(new long[]{0}, terminal.get().successfulOffsets().toArray());
        assertTrue(posted.isEmpty());
        assertEquals(1, h.releases.size());
    }

    @Test void laterCreateFailureDoesNotEraseKnownCompletedCount() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Flux.just(1L));
        h.failBeforeExecutionAt = 1;
        var posted = new ArrayList<Long>();
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(2, 2, BatchRowCountPolicy.EXACTLY_ONE),
                offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertTrue(error.evidence().affectedRows().isKnown());
        assertEquals(1, error.evidence().affectedRows().value());
        assertEquals(List.of(0L), posted);
    }

    @Test void sparseStrictRowsCompleteTheirAbsoluteOffsetsDespitePostFailure() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Flux.just(index == 4 ? 0L : 1L));
        var listener = new IllegalStateException("POST");
        var posted = new ArrayList<Long>();
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(9, 3, BatchRowCountPolicy.EXACTLY_ONE), offset -> {
                    posted.add(offset);
                    return offset == 3 ? Mono.error(listener) : Mono.empty();
                }).block());
        assertEquals(List.of(0L, 1L, 2L, 3L, 5L), posted);
        assertArrayEquals(new long[]{0, 1, 2, 3, 5}, error.evidence().successfulOffsets().toArray());
        assertEquals(6, h.executions);
        assertEquals(5, error.evidence().affectedRows().value());
        assertTrue(List.of(error.getCause().getSuppressed()).contains(listener));
    }

    @Test void unknownZeroAndMultipleStrictCountsNeverGainPostEligibility() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> switch (index) {
            case 1 -> Flux.empty();
            case 2 -> Flux.just(-1L);
            case 3 -> Flux.just(0L);
            case 4 -> Flux.just(2L);
            default -> Flux.just(1L);
        });
        var posted = new ArrayList<Long>();
        assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(6, 6, BatchRowCountPolicy.EXACTLY_ONE),
                offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertEquals(List.of(0L, 5L), posted);
    }
}
