package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.exception.RdbErrorKind;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchEvidenceInterruptionTest {

    @Test
    void connectionAcquisitionFailureCancelsPrefetchedInput() {
        SQLException acquisitionFailure = new SQLException("connection unavailable", "08001");
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("getConnection")) {
                        throw acquisitionFailure;
                    }
                    throw new AssertionError("unexpected DataSource SPI: " + method);
                });
        AtomicInteger cancelled = new AtomicInteger();
        Flux<Object[]> input = Flux.concat(
                Flux.<Object[]>just(new Object[]{1}), Flux.never())
                .doOnCancel(cancelled::incrementAndGet);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> JdbcBatchWriter.create(dataSource).writeBatchEvidence(
                        JdbcBatchEvidenceTestSupport.request(input, 1)));

        assertSame(acquisitionFailure, failure.getCause());
        assertEquals(BatchCommitFact.NOT_APPLICABLE, failure.evidence().commitFact());
        assertEquals(1L, failure.evidence().inputCount());
        assertEquals(1, cancelled.get());
    }

    @Test
    void interruptedInputWaitIsReportedAsCancellation() throws Exception {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
        GatedInput input = new GatedInput();
        AtomicReference<Thread> executing = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<BatchExecutionEvidenceException> result = worker.submit(() -> {
            executing.set(Thread.currentThread());
            BatchExecutionEvidenceException failure = assertThrows(
                    BatchExecutionEvidenceException.class,
                    () -> JdbcBatchWriter.create(state.dataSource()).writeBatchEvidence(
                            JdbcBatchEvidenceTestSupport.request(input, 2)));
            interrupted.set(Thread.currentThread().isInterrupted());
            return failure;
        });
        try {
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            executing.get().interrupt();
            BatchExecutionEvidenceException failure = result.get(5, TimeUnit.SECONDS);

            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertEquals(BatchExecutionState.CANCELLED, failure.evidence().state());
            assertEquals(BatchExecutionState.CANCELLED,
                         failure.evidence().chunks().getFirst().state());
            assertEquals(RdbErrorKind.CANCELLED,
                         failure.evidence().chunks().getFirst().failure().kind());
            assertTrue(interrupted.get());
            assertEquals(1, input.cancelled.get());
            assertEquals(0, state.acquired.get());
        } finally {
            input.release();
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        }
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
                    requested.countDown();
                }

                @Override
                public void cancel() {
                    cancelled.incrementAndGet();
                }
            });
        }

        private void release() {
            Subscriber<? super Object[]> target = subscriber;
            if (target != null && requested.getCount() == 0 && released.compareAndSet(false, true)) {
                target.onNext(new Object[]{1});
                target.onComplete();
            }
        }
    }
}
