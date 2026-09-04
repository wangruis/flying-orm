package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real batch entry points; the SPI records ownership boundaries, not heap usage. */
class R2dbcBatchAdmissionBudgetTest {
    private static final int ROW_COUNT = 32;
    private static final long ROW_BYTES = 998L; // One owned Object[] containing byte[950].

    @Test
    void independentAdmissionHasNoExtraOwnedRowWithoutResultDemandAndCancelsEveryStartedTransaction() {
        for (int concurrency : List.of(1, 4, 16)) {
            Fixture fixture = new Fixture();
            RecordingSubscriber<BatchChunkResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatchChunks(fixture.request(independent(concurrency))).subscribe(output);

                assertEquals(concurrency, fixture.emitted.get(), "accepted rows at concurrency " + concurrency);
                assertEquals(concurrency, fixture.started.get());
                assertEquals(concurrency, fixture.active.get());
                assertEquals(0, output.values.size());
                assertEquals(0, fixture.commits.get());
                assertTrue(fixture.emitted.get() * ROW_BYTES <= 1024L * concurrency);
                assertNull(output.error);
            } finally {
                output.cancel();
            }
            assertEquals(1, fixture.inputCancels.get());
            assertEquals(fixture.started.get(), fixture.executeCancels.get());
            assertEquals(fixture.started.get(), fixture.rollbacks.get());
            fixture.assertReleased();
        }
    }

    @Test
    void independentCompletedResultsWaitForDemandWithoutOpeningAnotherAdmissionWindow() {
        for (int concurrency : List.of(1, 4, 16)) {
            Fixture fixture = new Fixture();
            RecordingSubscriber<BatchChunkResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatchChunks(fixture.request(independent(concurrency))).subscribe(output);
                for (int index = 0; index < concurrency; index++) {
                    fixture.completeNext();
                }
                assertEquals(concurrency, fixture.emitted.get());
                assertEquals(concurrency, fixture.commits.get());
                assertEquals(0, output.values.size());

                for (int delivered = 1; delivered <= 3; delivered++) {
                    output.request(1);
                    assertEquals(delivered, output.values.size());
                    assertEquals(concurrency + delivered, fixture.emitted.get());
                    fixture.completeNext();
                    assertEquals(concurrency, fixture.commits.get() - output.values.size());
                }
                assertNull(output.error);
                assertEquals(0, fixture.active.get());
            } finally {
                output.cancel();
            }
            assertEquals(1, fixture.inputCancels.get());
            assertEquals(0, fixture.rollbacks.get());
            fixture.assertReleased();
        }
    }

    @Test
    void independentSuccessKeepsTheBudgetAndInputOffsetsAcrossEveryWindow() throws InterruptedException {
        for (int concurrency : List.of(1, 4, 16)) {
            Fixture fixture = new Fixture();
            RecordingSubscriber<BatchChunkResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatchChunks(fixture.request(independent(concurrency))).subscribe(output);
                output.request(Long.MAX_VALUE);
                for (int index = 0; index < ROW_COUNT; index++) {
                    assertTrue((fixture.emitted.get() - fixture.closed.get()) * ROW_BYTES <= 1024L * concurrency,
                            "owned input estimate before completing chunk " + index);
                    fixture.completeNext();
                }
                output.awaitTerminal();

                assertNull(output.error);
                assertEquals(ROW_COUNT, fixture.emitted.get());
                assertEquals(ROW_COUNT, fixture.commits.get());
                assertEquals(0, fixture.rollbacks.get());
                assertEquals(ROW_COUNT, output.values.size());
                assertEquals(ROW_COUNT, output.values.stream().mapToLong(BatchChunkResult::inputCount).sum());
                assertEquals(ROW_COUNT, output.values.stream().mapToLong(BatchChunkResult::affectedRows).sum());
                fixture.assertEveryInputBoundInOrder();
                for (int index = 0; index < ROW_COUNT; index++) {
                    assertEquals(index, output.values.get(index).chunkIndex());
                    assertEquals(index, output.values.get(index).startOffset());
                    assertEquals(BatchChunkResult.Status.COMMITTED, output.values.get(index).status());
                }
                fixture.assertReleased();
            } finally {
                output.cancel();
            }
        }
    }

    @Test
    void independentStopsAdmissionOnAcquireFailureAndSettlesEveryStartedSibling() throws InterruptedException {
        for (int concurrency : List.of(1, 4, 16)) {
            Fixture fixture = new Fixture(concurrency);
            RecordingSubscriber<BatchChunkResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatchChunks(fixture.request(independent(concurrency))).subscribe(output);
                output.request(Long.MAX_VALUE);
                assertEquals(concurrency, fixture.emitted.get());
                assertEquals(concurrency - 1, fixture.started.get());
                assertEquals(1, fixture.inputCancels.get());
                for (int index = 0; index < concurrency - 1; index++) {
                    fixture.completeNext();
                }
                output.awaitTerminal();

                assertInstanceOf(BatchWriteException.class, output.error);
                assertEquals(concurrency, fixture.emitted.get());
                assertEquals(concurrency - 1, fixture.commits.get());
                assertEquals(0, fixture.executeCancels.get());
                assertEquals(concurrency, output.values.size());
                assertEquals(1L, output.values.stream()
                        .filter(result -> result.status() == BatchChunkResult.Status.FAILED).count());
                fixture.assertReleased();
            } finally {
                output.cancel();
            }
        }
    }

    @Test
    void atomicDoesNotPrefetchASecondChunkWithOrWithoutReceiptsAndCancelsItsTransaction() {
        for (boolean receipt : List.of(false, true)) {
            Fixture fixture = new Fixture();
            RecordingSubscriber<BatchWriteResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatch(fixture.request(atomic(receipt))).subscribe(output);
                output.request(Long.MAX_VALUE);

                assertNull(output.error, "initial execution error with receipt=" + receipt);
                assertEquals(1, fixture.emitted.get(), "accepted rows with receipt=" + receipt);
                assertEquals(1, fixture.started.get());
                assertEquals(1, fixture.active.get());
                assertEquals(0, fixture.commits.get());
            } finally {
                output.cancel();
            }
            assertEquals(1, fixture.inputCancels.get());
            assertEquals(1, fixture.executeCancels.get());
            assertEquals(1, fixture.rollbacks.get());
            fixture.assertReleased();
        }
    }

    @Test
    void atomicAdvancesOnlyAfterTheActiveChunkCompletesAndCommitsOnceIncludingReceipts()
            throws InterruptedException {
        for (boolean receipt : List.of(false, true)) {
            Fixture fixture = new Fixture();
            RecordingSubscriber<BatchWriteResult> output = new RecordingSubscriber<>();
            try {
                fixture.executor().writeBatch(fixture.request(atomic(receipt))).subscribe(output);
                output.request(Long.MAX_VALUE);
                assertNull(output.error, "initial execution error with receipt=" + receipt);
                for (int index = 0; index < ROW_COUNT; index++) {
                    assertEquals(index + 1, fixture.emitted.get());
                    assertEquals(1, fixture.active.get());
                    assertEquals(0, fixture.commits.get());
                    fixture.completeNext();
                }
                output.awaitTerminal();

                assertNull(output.error);
                assertEquals(1, fixture.commits.get());
                assertEquals(0, fixture.rollbacks.get());
                assertEquals(receipt ? 2 : 1, fixture.acquired.get());
                assertEquals(1, output.values.size());
                BatchWriteResult result = output.values.getFirst();
                assertEquals(ROW_COUNT, result.inputCount());
                assertEquals(ROW_COUNT, result.affectedRows());
                assertEquals(ROW_COUNT, result.chunks().size());
                fixture.assertEveryInputBoundInOrder();
                for (int index = 0; index < ROW_COUNT; index++) {
                    assertEquals(index, result.chunks().get(index).startOffset());
                    assertEquals(BatchChunkResult.Status.COMMITTED, result.chunks().get(index).status());
                }
                fixture.assertReleased();
            } finally {
                output.cancel();
            }
        }
    }

    private static BatchWriteOptions independent(int concurrency) {
        return BatchWriteOptions.independent(8, concurrency)
                .withMemoryLimits(ROW_COUNT, 1024L * concurrency, ROW_COUNT).withMaxRowBytes(1024);
    }

    private static BatchWriteOptions atomic(boolean receipt) {
        BatchWriteOptions options = BatchWriteOptions.atomic(8)
                .withMemoryLimits(ROW_COUNT, 1024, ROW_COUNT).withMaxRowBytes(1024);
        return receipt ? options.withReceipt("admission-budget") : options;
    }

    private static final class RecordingSubscriber<T> extends BaseSubscriber<T> {
        private final List<T> values = new ArrayList<>();
        private final CountDownLatch terminal = new CountDownLatch(1);
        private Throwable error;

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            // Deliberately start with zero result demand; each test requests explicitly.
        }

        @Override
        protected void hookOnNext(T value) {
            values.add(value);
        }

        @Override
        protected void hookOnError(Throwable failure) {
            error = failure;
            terminal.countDown();
        }

        @Override
        protected void hookOnComplete() {
            terminal.countDown();
        }

        private void awaitTerminal() throws InterruptedException {
            assertTrue(terminal.await(2, TimeUnit.SECONDS), "batch did not reach its terminal signal");
        }
    }

    /** Parameters are counted when bound, never retained by this driver substitute. */
    private static final class Fixture implements ConnectionFactory {
        private final AtomicInteger emitted = new AtomicInteger();
        private final AtomicInteger inputCancels = new AtomicInteger();
        private final AtomicInteger attempts = new AtomicInteger();
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger executeCancels = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final List<Integer> boundValues = new ArrayList<>();
        private final Queue<Sinks.One<Result>> pending = new ArrayDeque<>();
        private final int failAcquisition;

        private Fixture() {
            this(0);
        }

        private Fixture(int failAcquisition) {
            this.failAcquisition = failAcquisition;
        }

        private R2dbcSqlExecutor executor() {
            return R2dbcSqlExecutor.create(this);
        }

        private BatchWriteRequest request(BatchWriteOptions options) {
            return BatchWriteRequests.request("insert into admission_sample(value_col) values (?)",
                    1, List.of(byte[].class), SqlBindMarkerStyle.CANONICAL,
                    Flux.range(0, ROW_COUNT).map(index -> {
                                byte[] value = new byte[950];
                                value[0] = (byte) index.intValue();
                                return new Object[]{value};
                            })
                            .doOnNext(ignored -> emitted.incrementAndGet())
                            .doOnCancel(inputCancels::incrementAndGet), options);
        }

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.defer(() -> {
                if (attempts.incrementAndGet() == failAcquisition) {
                    return Mono.error(new IllegalStateException("connection acquisition failed"));
                }
                acquired.incrementAndGet();
                return Mono.just((Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                        new Class<?>[]{Connection.class}, (proxy, method, args) -> switch (method.getName()) {
                            case "isAutoCommit" -> true;
                            case "beginTransaction", "setAutoCommit" -> Mono.empty();
                            case "commitTransaction" -> Mono.fromRunnable(commits::incrementAndGet);
                            case "rollbackTransaction" -> Mono.fromRunnable(rollbacks::incrementAndGet);
                            case "close" -> Mono.fromRunnable(closed::incrementAndGet);
                            case "createStatement" -> statement((String) args[0]);
                            case "toString" -> "admission-connection";
                            default -> throw new UnsupportedOperationException(method.getName());
                        }));
            });
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "H2";
        }

        private Statement statement(String sql) {
            String normalizedSql = sql.toLowerCase(Locale.ROOT);
            boolean receipt = normalizedSql.contains("flying_orm_batch_receipt");
            AtomicInteger boundRows = new AtomicInteger();
            return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                    new Class<?>[]{Statement.class}, (proxy, method, args) -> switch (method.getName()) {
                        case "bind", "bindNull" -> {
                            if (!receipt && Integer.valueOf(0).equals(args[0])) {
                                boundRows.incrementAndGet();
                                boundValues.add(((byte[]) args[1])[0] & 0xff);
                            }
                            yield proxy;
                        }
                        case "add", "fetchSize", "returnGeneratedValues" -> proxy;
                        case "execute" -> receipt
                                ? Flux.just(result(normalizedSql.startsWith("select") ? 0 : 1))
                                : pendingExecution(boundRows.get());
                        case "toString" -> "admission-statement";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private Mono<Result> pendingExecution(int boundRows) {
            return Mono.defer(() -> {
                assertEquals(1, boundRows, "this explicit admission limit produces one row per chunk");
                Sinks.One<Result> completion = Sinks.one();
                pending.add(completion);
                started.incrementAndGet();
                active.incrementAndGet();
                AtomicBoolean ended = new AtomicBoolean();
                return completion.asMono()
                        .doOnSuccess(ignored -> {
                            if (ended.compareAndSet(false, true)) {
                                active.decrementAndGet();
                            }
                        })
                        .doOnCancel(() -> {
                            if (ended.compareAndSet(false, true)) {
                                active.decrementAndGet();
                                executeCancels.incrementAndGet();
                            }
                        });
            });
        }

        private void completeNext() {
            Sinks.One<Result> completion = pending.poll();
            assertNotNull(completion, "no business execution is waiting for its result");
            assertEquals(Sinks.EmitResult.OK, completion.tryEmitValue(result(1)));
        }

        private void assertReleased() {
            assertEquals(0, active.get());
            assertEquals(acquired.get(), closed.get());
        }

        private void assertEveryInputBoundInOrder() {
            assertEquals(IntStream.range(0, ROW_COUNT).boxed().toList(), boundValues);
        }

        private static Result result(long updatedRows) {
            return (Result) Proxy.newProxyInstance(Result.class.getClassLoader(), new Class<?>[]{Result.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getRowsUpdated" -> Mono.just(updatedRows);
                        case "map" -> Flux.empty(); // Receipt lookup finds no previously committed operation.
                        case "toString" -> "admission-result";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
