package com.flying.orm.rdb.reactive;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import reactor.core.publisher.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import com.flying.orm.rdb.lifecycle.*;
class R2dbcIndependentBatchConcurrencyResultTest {
    @Test void notifiesEachCompletedRowOnlyOnceEvenWhenTheListenerFails() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(2L));
        List<Long> called = new ArrayList<>();
        var first = new EntityPostWriteException(EntityLifecyclePhase.POST_PERSIST, 0L,
                new IllegalStateException("first"));
        var second = new EntityPostWriteException(EntityLifecyclePhase.POST_PERSIST, 1L,
                new IllegalStateException("second"));
        assertSame(first, assertThrows(EntityPostWriteException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(4, 2, BatchRowCountPolicy.ANY), offset -> {
                    called.add(offset);
                    return Mono.error(offset == 0 ? first : second);
                }).block()));
        assertEquals(List.of(0L, 1L), called);
        assertSame(second, first.getSuppressed()[0]);
        assertEquals(1, h.executions);
    }

    @Test void reportsRowsAcceptedBeforeAnIncompleteWindowInputFailure() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(2L));
        var input = Flux.<Object[]>just(new Object[]{1}, new Object[]{2}, new Object[]{3})
                .concatWith(Flux.error(new IllegalArgumentException("input")));
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(input, 2, BatchRowCountPolicy.ANY)).block());
        assertEquals(3, error.evidence().inputCount());
        assertEquals(2, error.evidence().successfulCount());
        assertEquals(0, error.evidence().failedCount());
        assertEquals(1, h.gets);
        assertEquals(1, h.releases.size());
    }

    @Test void doesNotTrustExecutionEvidencePublishedByTheRowSource() {
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(1L));
        var fake = new BatchExecutionEvidence.Accumulator();
        fake.accept(99);
        fake.succeeded(99, BatchAffectedRows.known(99));
        var fakeError = new BatchExecutionEvidenceException("fake", null,
                fake.snapshot(BatchExecutionState.PARTIAL, null));
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                R2dbcOrdinaryBatchConnectionTest.request(Flux.error(fakeError), 1, BatchRowCountPolicy.ANY)).block());
        assertEquals(0, error.evidence().inputCount());
        assertEquals(0, error.evidence().successfulCount());
        assertEquals(0, h.gets);
    }
}
