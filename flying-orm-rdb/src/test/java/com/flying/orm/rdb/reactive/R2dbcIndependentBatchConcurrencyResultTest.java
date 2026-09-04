package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** INDEPENDENT 并发失败必须保留其它已启动分片已经确认的数据库结果。 */
class R2dbcIndependentBatchConcurrencyResultTest {

    @Test
    void keepsCommittedSiblingWhenAnotherConcurrentChunkCannotAcquireConnection() {
        Sinks.Empty<Void> committedCloseStarted = Sinks.empty();
        Sinks.Empty<Void> committedCloseRelease = Sinks.empty();
        AtomicInteger acquisitions = new AtomicInteger();
        Connection committedConnection = committedConnection(committedCloseStarted, committedCloseRelease);
        ConnectionFactory factory = connectionFactory(
                committedConnection, committedCloseStarted, committedCloseRelease, acquisitions);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}, new Object[]{3}),
                BatchWriteOptions.independent(1, 2));

        try {
            BatchWriteException failure = assertThrows(
                    BatchWriteException.class,
                    () -> R2dbcSqlExecutor.create(factory).writeBatch(request).block(Duration.ofSeconds(2)));

            assertEquals(2, failure.result().chunks().size());
            assertEquals(BatchChunkResult.Status.COMMITTED, failure.result().chunks().get(0).status());
            assertEquals(BatchChunkResult.Status.FAILED, failure.result().chunks().get(1).status());
            assertEquals(2, acquisitions.get());
        } finally {
            committedCloseRelease.tryEmitEmpty();
        }
    }

    @Test
    void chunkStreamEmitsEveryStartedOutcomeBeforeReportingTheStopFailure() {
        Sinks.Empty<Void> committedCloseStarted = Sinks.empty();
        Sinks.Empty<Void> committedCloseRelease = Sinks.empty();
        AtomicInteger acquisitions = new AtomicInteger();
        ConnectionFactory factory = connectionFactory(
                committedConnection(committedCloseStarted, committedCloseRelease),
                committedCloseStarted,
                committedCloseRelease,
                acquisitions);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}),
                BatchWriteOptions.independent(1, 2));

        List<Signal<BatchChunkResult>> signals = R2dbcSqlExecutor.create(factory)
                .writeBatchChunks(request)
                .materialize()
                .collectList()
                .block(Duration.ofSeconds(2));

        List<BatchChunkResult.Status> statuses = signals.stream()
                .filter(Signal::isOnNext)
                .map(Signal::get)
                .map(BatchChunkResult::status)
                .sorted()
                .toList();
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED), statuses);
        assertTrue(signals.get(signals.size() - 1).getThrowable() instanceof BatchWriteException);
        assertEquals(2, acquisitions.get());
    }

    @Test
    void waitsForActiveChunkOutcomeBeforeReportingInputFailure() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("batch input failed");
        Sinks.Empty<Void> committedRelease = Sinks.empty();
        AtomicBoolean activeChunkCancelled = new AtomicBoolean();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(
                        Flux.<Object[]>just(new Object[]{1}),
                        Flux.<Object[]>create(sink -> {
                            sink.error(inputFailure);
                            committedRelease.tryEmitEmpty();
                        })),
                BatchWriteOptions.independent(1, 2));
        R2dbcIndependentBatchFlow flow = new R2dbcIndependentBatchFlow(
                new R2dbcBatchWriterChunks(R2dbcBindMarkers.from(metadataOnlyFactory())),
                new R2dbcBatchResultAssembler());

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> flow.write(request, chunk -> committedRelease.asMono()
                                .thenReturn(BatchChunkResult.committed(
                                        chunk.chunkIndex(), chunk.startOffset(), chunk.rows().size(), 1L))
                                .doOnCancel(() -> activeChunkCancelled.set(true)))
                        .block(Duration.ofSeconds(2)));

        assertFalse(activeChunkCancelled.get());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED),
                     failure.result().chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void keepsChunkIdentityWhenExecutorThrowsBeforeReturningAPublisher() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(metadataOnlyFactory()));
        R2dbcIndependentBatchFlow flow = new R2dbcIndependentBatchFlow(
                chunks, new R2dbcBatchResultAssembler());
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.independent(1, 1));

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> flow.write(request, ignored -> {
                    throw new IllegalStateException("executor failed before publisher creation");
                }).block(Duration.ofSeconds(2)));

        assertEquals(1, failure.result().chunks().size());
        assertEquals(1, failure.result().chunks().get(0).inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, failure.result().chunks().get(0).status());
    }

    @Test
    void reportsRowsAcceptedBeforeAnIncompleteChunkInputFailure() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("batch input failed");
        AtomicBoolean executorCalled = new AtomicBoolean();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(inputFailure)),
                BatchWriteOptions.independent(2, 1));
        R2dbcIndependentBatchFlow flow = new R2dbcIndependentBatchFlow(
                new R2dbcBatchWriterChunks(R2dbcBindMarkers.from(metadataOnlyFactory())),
                new R2dbcBatchResultAssembler());

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> flow.write(request, ignored -> {
                    executorCalled.set(true);
                    return Mono.error(new AssertionError("incomplete chunk must not execute"));
                }).block(Duration.ofSeconds(2)));

        assertFalse(executorCalled.get());
        assertEquals(1, failure.result().inputCount());
        assertEquals(1, failure.result().chunks().size());
        assertEquals(1, failure.result().chunks().getFirst().inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, failure.result().chunks().getFirst().status());
    }

    @Test
    void doesNotTrustBatchResultPublishedByTheRowSource() {
        BatchWriteException sourceFailure = new BatchWriteException(
                "failure from another batch",
                new IllegalStateException("prior batch failed"),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                        List.of(BatchChunkResult.committed(0, 0, 500, 500L))));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(sourceFailure)),
                BatchWriteOptions.independent(1, 1));
        R2dbcIndependentBatchFlow flow = new R2dbcIndependentBatchFlow(
                new R2dbcBatchWriterChunks(R2dbcBindMarkers.from(metadataOnlyFactory())),
                new R2dbcBatchResultAssembler());

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> flow.write(request, chunk -> Mono.just(BatchChunkResult.committed(
                                chunk.chunkIndex(), chunk.startOffset(), chunk.rows().size(), 1L)))
                        .block(Duration.ofSeconds(2)));

        assertSame(sourceFailure, error.getCause());
        assertEquals(1, error.result().inputCount());
        assertEquals(1, error.result().affectedRows());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.FAILED),
                     error.result().chunks().stream().map(BatchChunkResult::status).toList());
    }

    @Test
    void chunkStreamDoesNotExposeBatchResultPublishedByTheRowSource() {
        BatchWriteException sourceFailure = new BatchWriteException(
                "failure from another batch",
                new IllegalStateException("prior batch failed"),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                        List.of(BatchChunkResult.committed(0, 0, 500, 500L))));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(sourceFailure)),
                BatchWriteOptions.independent(1, 1));
        R2dbcIndependentBatchFlow flow = new R2dbcIndependentBatchFlow(
                new R2dbcBatchWriterChunks(R2dbcBindMarkers.from(metadataOnlyFactory())),
                new R2dbcBatchResultAssembler());

        List<Signal<BatchChunkResult>> signals = flow.writeChunks(
                        request,
                        chunk -> Mono.just(BatchChunkResult.committed(
                                chunk.chunkIndex(), chunk.startOffset(), chunk.rows().size(), 1L)))
                .materialize()
                .collectList()
                .block(Duration.ofSeconds(2));

        List<BatchChunkResult> emitted = signals.stream()
                .filter(Signal::isOnNext)
                .map(Signal::get)
                .toList();
        assertEquals(1, emitted.size());
        assertEquals(1, emitted.getFirst().inputCount());
        assertEquals(1, emitted.getFirst().affectedRows());
        assertEquals(BatchChunkResult.Status.COMMITTED, emitted.getFirst().status());
        Throwable terminal = signals.getLast().getThrowable();
        assertFalse(terminal instanceof BatchWriteException);
        assertSame(sourceFailure, terminal.getCause());
    }

    private static ConnectionFactory metadataOnlyFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new AssertionError("connection must not be requested"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static ConnectionFactory connectionFactory(Connection committedConnection,
                                                       Sinks.Empty<Void> committedCloseStarted,
                                                       Sinks.Empty<Void> committedCloseRelease,
                                                       AtomicInteger acquisitions) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                if (acquisitions.getAndIncrement() == 0) {
                    return Mono.just(committedConnection);
                }
                return committedCloseStarted.asMono().then(Mono.create(sink -> {
                    sink.error(new IllegalStateException("second connection unavailable"));
                    committedCloseRelease.tryEmitEmpty();
                }));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection committedConnection(Sinks.Empty<Void> closeStarted,
                                                  Sinks.Empty<Void> closeRelease) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "commitTransaction", "setAutoCommit" -> Mono.empty();
                    case "createStatement" -> successfulStatement();
                    case "close" -> {
                        closeStarted.tryEmitEmpty();
                        yield closeRelease.asMono();
                    }
                    case "toString" -> "committed-test-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement successfulStatement() {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> proxy;
                    case "execute" -> Flux.just(successfulResult());
                    case "toString" -> "successful-test-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result successfulResult() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(1L);
                    case "toString" -> "successful-test-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
