package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchAdmissionBudgetTest {

    @Test
    void doesNotOwnACarryRowWhileTheFirstLargeRowIsExecuting() throws Exception {
        assertAdmissionAtFirstExecution(950, 950, 950);
    }

    @Test
    void reservesTheDeclaredMaximumEvenAfterASmallRow() throws Exception {
        assertAdmissionAtFirstExecution(0, 976, 0, 976);
    }

    @Test
    void smallRowsStillFillFiveHundredRowChunks() throws Exception {
        AtomicInteger emitted = new AtomicInteger();
        BatchWriteRequest request = request(
                Flux.range(0, 1001).map(index -> new Object[]{new byte[0]})
                        .doOnNext(ignored -> emitted.incrementAndGet()),
                BatchWriteOptions.atomic(500).withMemoryLimits(1001, 64 * 1024, 3)
                        .withMaxRowBytes(128));
        JdbcBatchSupport.BatchDeadline deadline = JdbcBatchSupport.BatchDeadline.start(Duration.ZERO);
        JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
        try (JdbcBatchRows rows = new JdbcBatchRows(
                request.rows(), request.parameterCount(), request.options().maxRowBytes())) {
            assertEquals(500, JdbcBatchSupport.readChunk(rows, request, 0L, 0, deadline, progress).size());
            assertEquals(500, emitted.get());
            assertEquals(500, JdbcBatchSupport.readChunk(rows, request, 500L, 1, deadline, progress).size());
            assertEquals(1000, emitted.get());
            assertEquals(1, JdbcBatchSupport.readChunk(rows, request, 1000L, 2, deadline, progress).size());
            assertEquals(1001, emitted.get());
            assertTrue(JdbcBatchSupport.readChunk(rows, request, 1001L, 3, deadline, progress).isEmpty());
        }
    }

    @Test
    void rejectsARowAboveItsDeclaredMaximumBeforeAcquiringAConnection() {
        JdbcState state = new JdbcState(false);
        AtomicInteger cancelled = new AtomicInteger();
        BatchWriteRequest request = request(
                Flux.range(0, 2).map(index -> new Object[]{new byte[950]})
                        .doOnCancel(cancelled::incrementAndGet),
                BatchWriteOptions.independent(8, 1).withMemoryLimits(2, 4096, 2)
                        .withMaxRowBytes(128));

        BatchWriteException failure = assertThrows(BatchWriteException.class,
                () -> JdbcBatchWriter.create(state.dataSource()).writeBatchChunks(request));

        BatchMemoryLimitExceededException limit = assertInstanceOf(
                BatchMemoryLimitExceededException.class, failure.getCause());
        assertEquals(128, limit.limit());
        assertEquals(BatchMemoryBudget.estimateRowBytes(new Object[]{new byte[950]}), limit.actual());
        assertEquals(1, cancelled.get());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.closed.get());
        assertEquals(0, state.commits.get());
    }

    private static void assertAdmissionAtFirstExecution(int... payloadLengths) throws Exception {
        JdbcState state = new JdbcState(true);
        AtomicInteger emitted = new AtomicInteger();
        AtomicLong emittedWeight = new AtomicLong();
        Flux<Object[]> input = Flux.range(0, payloadLengths.length)
                .map(index -> new Object[]{new byte[payloadLengths[index]]})
                .doOnNext(row -> {
                    emitted.incrementAndGet();
                    emittedWeight.addAndGet(BatchMemoryBudget.estimateRowBytes(row));
                });
        BatchWriteRequest request = request(input,
                BatchWriteOptions.independent(8, 1).withMemoryLimits(payloadLengths.length, 1024,
                        payloadLengths.length).withMaxRowBytes(1024));
        ExecutorService worker = Executors.newSingleThreadExecutor();
        Future<List<BatchChunkResult>> future = worker.submit(
                () -> JdbcBatchWriter.create(state.dataSource()).writeBatchChunks(request));
        try {
            assertTrue(state.entered.await(5, TimeUnit.SECONDS), "first executeBatch was not reached");
            assertEquals(0, state.commits.get());
            assertEquals(0, state.closed.get());
            assertEquals(1, state.boundRows.get());
            assertEquals(1, emitted.get(), "the next row must not be admitted before this chunk retires");
            assertTrue(emittedWeight.get() <= request.options().maxBufferedBytes(),
                    "all emitted rows are still ORM-owned before the first execution completes");

            state.release.countDown();
            List<BatchChunkResult> results = future.get(5, TimeUnit.SECONDS);
            assertEquals(payloadLengths.length, results.size());
            for (int index = 0; index < results.size(); index++) {
                BatchChunkResult result = results.get(index);
                assertEquals(index, result.chunkIndex());
                assertEquals(index, result.startOffset());
                assertEquals(1, result.inputCount());
                assertEquals(1L, result.affectedRows());
                assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
            }
            assertEquals(payloadLengths.length, emitted.get());
            assertEquals(payloadLengths.length, state.boundRows.get());
            assertEquals(Arrays.stream(payloadLengths).boxed().toList(), state.boundLengths);
            assertEquals(payloadLengths.length, state.commits.get());
            assertEquals(0, state.rollbacks.get());
            assertEquals(payloadLengths.length, state.acquired.get());
            assertEquals(state.acquired.get(), state.closed.get());
            assertEquals(payloadLengths.length, state.statementsClosed.get());
        } finally {
            state.release.countDown();
            if (!future.isDone()) {
                future.cancel(true);
            }
            worker.shutdownNow();
            assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS), "JDBC test worker did not stop");
        }
    }

    private static BatchWriteRequest request(Publisher<Object[]> input, BatchWriteOptions options) {
        return BatchWriteRequests.request("insert into samples(value) values (?)", 1,
                List.of(byte[].class), SqlBindMarkerStyle.CANONICAL, input, options);
    }

    private static final class JdbcState {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean pauseFirst;
        private final AtomicInteger acquired = new AtomicInteger();
        private final AtomicInteger boundRows = new AtomicInteger();
        private final List<Integer> boundLengths = new ArrayList<>();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger statementsClosed = new AtomicInteger();

        private JdbcState(boolean pauseFirst) {
            this.pauseFirst = new AtomicBoolean(pauseFirst);
        }

        private DataSource dataSource() {
            return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                    new Class<?>[]{DataSource.class}, (proxy, method, arguments) -> switch (method.getName()) {
                        case "getConnection" -> {
                            acquired.incrementAndGet();
                            yield connection();
                        }
                        case "toString" -> "admission budget data source";
                        default -> throw new AssertionError("unexpected DataSource SPI: " + method);
                    });
        }

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                        case "prepareStatement" -> statement();
                        case "getAutoCommit" -> true;
                        case "setAutoCommit" -> null;
                        case "commit" -> {
                            commits.incrementAndGet();
                            yield null;
                        }
                        case "rollback" -> {
                            rollbacks.incrementAndGet();
                            yield null;
                        }
                        case "close" -> {
                            closed.incrementAndGet();
                            yield null;
                        }
                        case "toString" -> "admission budget connection";
                        default -> throw new AssertionError("unexpected Connection SPI: " + method);
                    });
        }

        private PreparedStatement statement() {
            AtomicInteger rows = new AtomicInteger();
            return (PreparedStatement) Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> switch (method.getName()) {
                        case "setObject" -> {
                            boundRows.incrementAndGet();
                            boundLengths.add(((byte[]) arguments[1]).length);
                            yield null;
                        }
                        case "addBatch" -> {
                            rows.incrementAndGet();
                            yield null;
                        }
                        case "executeBatch" -> {
                            if (pauseFirst.compareAndSet(true, false)) {
                                entered.countDown();
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new AssertionError("JDBC execution latch timed out");
                                }
                            }
                            int[] result = new int[rows.get()];
                            Arrays.fill(result, 1);
                            yield result;
                        }
                        case "close" -> {
                            statementsClosed.incrementAndGet();
                            yield null;
                        }
                        case "cancel" -> null;
                        case "toString" -> "admission budget statement";
                        default -> throw new AssertionError("unexpected PreparedStatement SPI: " + method);
                    });
        }
    }
}
