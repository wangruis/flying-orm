package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcAtomicBatchAdmissionTest {

    @Test
    void leavesTheConnectionUnacquiredWhileWaitingForTheFirstChunk() throws Exception {
        JdbcState state = new JdbcState();
        GatedInput input = new GatedInput();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<BatchWriteResult> result = worker.submit(() -> writer(state).writeBatch(
                request(input, BatchWriteOptions.atomic(2))));
        try {
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            assertEquals(0, state.acquired.get(), "input wait must not hold a DataSource lease");
            assertEquals(0, state.begins.get());
            assertEquals(0, state.executions.get());
            input.release();
            assertCommitted(result.get(5, TimeUnit.SECONDS), 1);
            assertOwnedLifecycle(state);
        } finally {
            input.release();
            stop(worker);
        }
    }

    @Test
    void emptyOwnedInputDoesNotAcquireOrBeginAConnection() {
        JdbcState state = new JdbcState();
        BatchWriteResult result = writer(state).writeBatch(request(Flux.empty(), BatchWriteOptions.atomic(2)));

        assertCommitted(result, 0);
        assertTrue(result.chunks().isEmpty());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.begins.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.closed.get());
    }

    @Test
    void firstChunkInputFailureKeepsAcceptedRowsWithoutAcquiringOrRollingBack() {
        JdbcState state = new JdbcState();
        IllegalStateException inputFailure = new IllegalStateException("input stopped");
        AtomicInteger cancelled = new AtomicInteger();
        Publisher<Object[]> input = subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean sent;

            @Override
            public void request(long count) {
                if (!sent) {
                    sent = true;
                    subscriber.onNext(new Object[]{1});
                    subscriber.onError(inputFailure);
                }
            }

            @Override
            public void cancel() {
                cancelled.incrementAndGet();
            }
        });

        BatchWriteException failure = assertThrows(BatchWriteException.class, () -> writer(state)
                .writeBatch(request(input, BatchWriteOptions.atomic(2))));

        assertSame(inputFailure, failure.getCause());
        assertFailedBeforeExecution(failure, 1);
        assertEquals(1, cancelled.get());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closed.get());
    }

    @Test
    void interruptedFirstChunkWaitCancelsInputAndRestoresInterruptWithoutAcquiring() throws Exception {
        JdbcState state = new JdbcState();
        GatedInput input = new GatedInput();
        AtomicReference<Thread> executing = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<BatchWriteException> result = worker.submit(() -> {
            executing.set(Thread.currentThread());
            BatchWriteException failure = assertThrows(BatchWriteException.class, () -> writer(state)
                    .writeBatch(request(input, BatchWriteOptions.atomic(2))));
            interrupted.set(Thread.currentThread().isInterrupted());
            return failure;
        });
        try {
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            executing.get().interrupt();
            BatchWriteException failure = result.get(5, TimeUnit.SECONDS);
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertFailedBeforeExecution(failure, 0);
            assertTrue(interrupted.get());
            assertEquals(1, input.cancelled.get());
            assertEquals(0, state.acquired.get());
            assertEquals(0, state.rollbacks.get());
        } finally {
            input.release();
            stop(worker);
        }
    }

    @Test
    void acquisitionFailureReportsTheAlreadyAdmittedFirstChunkAndCancelsInput() {
        JdbcState state = new JdbcState();
        SQLException acquisitionFailure = new SQLException("connection unavailable");
        state.acquisitionFailure = acquisitionFailure;
        AtomicInteger cancelled = new AtomicInteger();
        Publisher<Object[]> input = Flux.range(0, 5).map(index -> new Object[]{index})
                .doOnCancel(cancelled::incrementAndGet);

        BatchWriteException failure = assertThrows(BatchWriteException.class, () -> writer(state)
                .writeBatch(request(input, BatchWriteOptions.atomic(2))));

        assertSame(acquisitionFailure, failure.getCause());
        assertFailedBeforeExecution(failure, 2);
        assertEquals(1, cancelled.get());
        assertEquals(0, state.begins.get());
        assertEquals(0, state.closed.get());
    }

    @Test
    void firstChunkIsBoundedAndSubsequentChunksStayInTheSameOwnedTransaction() {
        JdbcState state = new JdbcState();
        AtomicInteger emitted = new AtomicInteger();
        List<Integer> admittedAtExecution = new ArrayList<>();
        AtomicInteger admittedAtAcquisition = new AtomicInteger();
        state.onAcquire = () -> admittedAtAcquisition.set(emitted.get());
        state.onExecute = () -> admittedAtExecution.add(emitted.get());
        Publisher<Object[]> input = Flux.range(0, 5).map(index -> new Object[]{index})
                .doOnNext(ignored -> emitted.incrementAndGet());

        BatchWriteResult result = writer(state).writeBatch(request(input, BatchWriteOptions.atomic(2)));

        assertEquals(2, admittedAtAcquisition.get());
        assertEquals(List.of(2, 4, 5), admittedAtExecution);
        assertEquals(List.of(2, 2, 1), result.chunks().stream().map(BatchChunkResult::inputCount).toList());
        assertEquals(List.of(0L, 2L, 4L), result.chunks().stream().map(BatchChunkResult::startOffset).toList());
        assertCommitted(result, 5);
        assertOwnedLifecycle(state);
    }

    @Test
    void firstChunkWaitIsOutsideTheOwnedConnectionTimeout() throws Exception {
        JdbcState state = new JdbcState();
        GatedInput input = new GatedInput();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<BatchWriteResult> result = worker.submit(() -> writer(state).writeBatch(request(input,
                BatchWriteOptions.atomic(2).withTimeout(Duration.ofSeconds(1)))));
        try {
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> result.get(1200, TimeUnit.MILLISECONDS),
                    "the batch must still be waiting for input after its SQL timeout duration");
            assertFalse(result.isDone());
            input.release();
            assertCommitted(result.get(5, TimeUnit.SECONDS), 1);
            assertOwnedLifecycle(state);
        } finally {
            input.release();
            stop(worker);
        }
    }

    @Test
    void borrowedTransactionIsResolvedBeforeInputAndRemainsExternallyOwned() {
        JdbcState state = new JdbcState();
        AtomicInteger resolutions = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection(), listener -> {
            completions.incrementAndGet();
            return true;
        });
        Publisher<Object[]> input = Flux.range(0, 3).map(index -> new Object[]{index})
                .doOnSubscribe(ignored -> assertEquals(1, resolutions.get()));
        JdbcBatchWriter writer = writer(state).withTransactionParticipant(() -> {
            resolutions.incrementAndGet();
            return Optional.of(transaction);
        });

        BatchWriteResult result = writer.writeBatch(request(input, BatchWriteOptions.atomic(2)));

        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
        assertEquals(3, result.inputCount());
        assertEquals(0, result.affectedRows());
        assertEquals(1, resolutions.get());
        assertEquals(1, completions.get());
        assertEquals(2, state.executions.get());
        assertNoOwnedLifecycle(state);
    }

    @Test
    void emptyBorrowedInputStillRegistersItsCompletionWithoutOwningTheConnection() {
        JdbcState state = new JdbcState();
        AtomicInteger completions = new AtomicInteger();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection(), listener -> {
            completions.incrementAndGet();
            return true;
        });

        BatchWriteResult result = writer(state).withTransactionParticipant(() -> Optional.of(transaction))
                .writeBatch(request(Flux.empty(), BatchWriteOptions.atomic(2)));

        assertCommitted(result, 0);
        assertEquals(1, completions.get());
        assertNoOwnedLifecycle(state);
    }

    @Test
    void borrowedInputFailureKeepsItsUnknownOutcomeWithoutOwningTheConnection() {
        JdbcState state = new JdbcState();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        IllegalStateException inputFailure = new IllegalStateException("external input stopped");
        Publisher<Object[]> input = Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(inputFailure));

        BatchWriteException failure = assertThrows(BatchWriteException.class, () -> writer(state)
                .withTransactionParticipant(() -> Optional.of(transaction))
                .writeBatch(request(input, BatchWriteOptions.atomic(2))));

        assertSame(inputFailure, failure.getCause());
        assertEquals(BatchWriteResult.Status.UNKNOWN, failure.result().status());
        assertEquals(1, failure.result().inputCount());
        assertEquals(BatchChunkResult.Status.UNKNOWN, failure.result().chunks().getFirst().status());
        assertNoOwnedLifecycle(state);
    }

    @Test
    void borrowedFirstChunkWaitKeepsTheExistingConnectionDeadline() throws Exception {
        JdbcState state = new JdbcState();
        GatedInput input = new GatedInput();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<BatchWriteException> result = worker.submit(() -> assertThrows(BatchWriteException.class,
                () -> writer(state).withTransactionParticipant(() -> Optional.of(transaction))
                        .writeBatch(request(input, BatchWriteOptions.atomic(2).withTimeout(Duration.ofMillis(100))))));
        try {
            assertTrue(input.requested.await(5, TimeUnit.SECONDS));
            BatchWriteException failure = result.get(5, TimeUnit.SECONDS);
            assertInstanceOf(TimeoutException.class, failure.getCause());
            assertEquals(BatchWriteResult.Status.UNKNOWN, failure.result().status());
            assertEquals(0, failure.result().inputCount());
            assertEquals(1, input.cancelled.get());
            assertNoOwnedLifecycle(state);
        } finally {
            input.release();
            stop(worker);
        }
    }

    @Test
    void emptyInputCancelFailureDoesNotClaimACommitOrUnknownTransaction() {
        JdbcState state = new JdbcState();
        IllegalStateException cancelFailure = new IllegalStateException("input cancel failed");
        Publisher<Object[]> input = subscriber -> {
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long count) {
                    subscriber.onComplete();
                }

                @Override
                public void cancel() {
                    throw cancelFailure;
                }
            });
        };

        BatchWriteException failure = assertThrows(BatchWriteException.class, () -> writer(state)
                .writeBatch(request(input, BatchWriteOptions.atomic(2))));

        assertSame(cancelFailure, failure.getCause());
        assertFailedBeforeExecution(failure, 0);
        assertNoOwnedLifecycle(state);
    }

    @Test
    void ownedAdmissionDoesNotResolveTheTransactionAgainAfterInputCallbacks() {
        JdbcState state = new JdbcState();
        AtomicInteger resolutions = new AtomicInteger();
        JdbcTransactionContext laterTransaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = writer(state).withTransactionParticipant(() ->
                resolutions.getAndIncrement() == 0 ? Optional.empty() : Optional.of(laterTransaction));

        BatchWriteResult result = writer.writeBatch(request(Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.atomic(2)));

        assertCommitted(result, 1);
        assertEquals(1, resolutions.get());
        assertOwnedLifecycle(state);
    }

    @Test
    void unsupportedReceiptIsRejectedBeforeInputAndConnectionResolution() {
        JdbcState state = new JdbcState();
        AtomicInteger subscribed = new AtomicInteger();
        AtomicInteger resolutions = new AtomicInteger();
        JdbcBatchWriter writer = writer(state).withTransactionParticipant(() -> {
            resolutions.incrementAndGet();
            return Optional.empty();
        });
        Publisher<Object[]> input = Flux.<Object[]>empty().doOnSubscribe(ignored -> subscribed.incrementAndGet());

        assertThrows(UnsupportedOperationException.class, () -> writer.writeBatch(
                request(input, BatchWriteOptions.atomic(2).withReceipt("unsupported"))));

        assertEquals(0, subscribed.get());
        assertEquals(0, resolutions.get());
        assertNoOwnedLifecycle(state);
    }

    private static JdbcBatchWriter writer(JdbcState state) {
        return JdbcBatchWriter.create(state.dataSource());
    }

    private static BatchWriteRequest request(Publisher<Object[]> input, BatchWriteOptions options) {
        return BatchWriteRequests.request("insert into samples(value) values (?)", 1,
                List.of(Integer.class), SqlBindMarkerStyle.CANONICAL, input, options);
    }

    private static void assertCommitted(BatchWriteResult result, int count) {
        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(count, result.inputCount());
        assertEquals(count, result.affectedRows());
    }

    private static void assertFailedBeforeExecution(BatchWriteException failure, int accepted) {
        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
        assertEquals(accepted, failure.result().inputCount());
        assertEquals(0, failure.result().affectedRows());
        assertEquals(1, failure.result().chunks().size());
        BatchChunkResult chunk = failure.result().chunks().getFirst();
        assertEquals(0, chunk.chunkIndex());
        assertEquals(0, chunk.startOffset());
        assertEquals(accepted, chunk.inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, chunk.status());
    }

    private static void assertOwnedLifecycle(JdbcState state) {
        assertEquals(1, state.acquired.get());
        assertEquals(1, state.begins.get());
        assertEquals(1, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(1, state.closed.get());
    }

    private static void assertNoOwnedLifecycle(JdbcState state) {
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.begins.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closed.get());
    }

    private static void stop(ExecutorService worker) throws InterruptedException {
        worker.shutdownNow();
        assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS), "JDBC test worker did not stop");
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

    private static final class JdbcState {
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger begins = new AtomicInteger();
        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private SQLException acquisitionFailure;
        private Runnable onAcquire = () -> { };
        private Runnable onExecute = () -> { };

        DataSource dataSource() {
            return proxy(DataSource.class, (self, method, arguments) -> {
                if (!method.getName().equals("getConnection")) {
                    throw new AssertionError("unexpected DataSource SPI: " + method);
                }
                acquired.incrementAndGet();
                onAcquire.run();
                if (acquisitionFailure != null) {
                    throw acquisitionFailure;
                }
                return connection();
            });
        }

        Connection connection() {
            return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                case "getAutoCommit" -> true;
                case "setAutoCommit" -> { begins.incrementAndGet(); yield null; }
                case "prepareStatement" -> statement();
                case "commit" -> { commits.incrementAndGet(); yield null; }
                case "rollback" -> { rollbacks.incrementAndGet(); yield null; }
                case "close" -> { closed.incrementAndGet(); yield null; }
                default -> throw new AssertionError("unexpected Connection SPI: " + method);
            });
        }

        PreparedStatement statement() {
            AtomicInteger rows = new AtomicInteger();
            return proxy(PreparedStatement.class, (self, method, arguments) -> switch (method.getName()) {
                case "setObject", "setNull", "setQueryTimeout", "cancel", "close" -> null;
                case "addBatch" -> { rows.incrementAndGet(); yield null; }
                case "executeBatch" -> {
                    executions.incrementAndGet();
                    onExecute.run();
                    int[] result = new int[rows.get()];
                    Arrays.fill(result, 1);
                    yield result;
                }
                default -> throw new AssertionError("unexpected PreparedStatement SPI: " + method);
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
