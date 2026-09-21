package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.*;
import reactor.core.publisher.Flux;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcAtomicBatchAdmissionTest {
    @Test void leavesConnectionUnacquiredWhileWaitingForFirstBuffer() throws Exception {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var input = new GatedInput();
        try (var worker = Executors.newSingleThreadExecutor()) {
            var future = worker.submit(() -> state.writer().writeBatch(JdbcBatchEvidenceTestSupport.request(input, 2)));
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            assertEquals(0, state.acquired.get());
            input.release();
            var fact = future.get(5, TimeUnit.SECONDS);
            assertEquals(1, fact.inputCount());
            assertEquals(1, fact.successfulCount());
            assertEquals(1, state.acquired.get());
            assertEquals(1, state.released.get());
            assertEquals(0, state.closes.get());
        }
    }

    @Test void emptyInputUsesNoConnection() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var fact = state.writer().writeBatch(JdbcBatchEvidenceTestSupport.request(Flux.empty(), 2));
        assertEquals(BatchExecutionState.SUCCESS, fact.state());
        assertEquals(0, fact.inputCount());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.released.get());
    }

    @Test void firstBufferInputFailureKeepsAcceptedRows() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var inputFailure = new IllegalStateException("input stopped");
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(inputFailure)), 2)));
        assertSame(inputFailure, failure.getCause());
        assertEquals(1, failure.evidence().inputCount());
        assertEquals(0, failure.evidence().successfulCount());
        assertEquals(0, failure.evidence().failedCount());
        assertEquals(0, failure.evidence().affectedRows().value());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.released.get());
    }

    @Test void interruptedFirstBufferWaitCancelsAndRestoresInterrupt() throws Exception {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var input = new GatedInput();
        var executing = new AtomicReference<Thread>();
        var interrupted = new AtomicBoolean();
        try (var worker = Executors.newSingleThreadExecutor()) {
            var future = worker.submit(() -> {
                executing.set(Thread.currentThread());
                var error = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                        JdbcBatchEvidenceTestSupport.request(input, 2)));
                interrupted.set(Thread.currentThread().isInterrupted());
                return error;
            });
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            executing.get().interrupt();
            var error = future.get(5, TimeUnit.SECONDS);
            assertInstanceOf(InterruptedException.class, error.getCause());
            assertEquals(0, error.evidence().inputCount());
            assertTrue(interrupted.get());
            assertEquals(1, input.cancelled.get());
            assertEquals(0, state.acquired.get());
        }
    }

    @Test void acquisitionFailureDoesNotRelease() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.acquisitionFailure = new SQLException("unavailable");
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0, 5).map(i -> new Object[]{i}), 2)));
        assertSame(state.acquisitionFailure, failure.getCause());
        assertEquals(2, failure.evidence().inputCount());
        assertEquals(1, state.acquired.get());
        assertEquals(0, state.released.get());
        assertEquals(0, state.executions.get());
    }

    @Test void buffersRemainBoundedOnOneConnection() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var emitted = new AtomicInteger();
        var atExecution = new ArrayList<Integer>();
        state.onExecute = () -> atExecution.add(emitted.get());
        var fact = state.writer().writeBatch(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 5).map(i -> new Object[]{i}).doOnNext(row -> emitted.incrementAndGet()), 2));
        assertEquals(List.of(2,4,5), atExecution);
        assertEquals(5, fact.successfulCount());
        assertEquals(5, fact.affectedRows().value());
        assertEquals(List.of(0), state.requests.getFirst().parameters());
        assertEquals(1, state.acquired.get());
        assertEquals(1, state.released.get());
        assertEquals(0, state.autoCommitReads.get());
        assertEquals(0, state.autoCommitWrites.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closes.get());
    }

    private static final class GatedInput implements Publisher<Object[]> {
        private final CountDownLatch requested = new CountDownLatch(1);
        private final AtomicInteger cancelled = new AtomicInteger();
        private final AtomicBoolean released = new AtomicBoolean();
        private volatile Subscriber<? super Object[]> subscriber;

        @Override
        public void subscribe(Subscriber<? super Object[]> target) {
            subscriber = target;
            target.onSubscribe(new Subscription() {
                @Override
                public void request(long count) {
                    assertEquals(1, count);
                    requested.countDown();
                }

                @Override
                public void cancel() {
                    cancelled.incrementAndGet();
                }
            });
        }

        void release() {
            Subscriber<? super Object[]> target = subscriber;
            if (target != null && requested.getCount() == 0 && released.compareAndSet(false, true)) {
                target.onNext(new Object[]{1});
                target.onComplete();
            }
        }
    }


}
