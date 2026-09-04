package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveBatchExecutionObservationSupportTest {

    @Test
    void disabledBatchObserverIsNotInvokedBySqlOnlyMonoObservation() {
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        BatchExecutionObserver disabledBatchObserver = new BatchExecutionObserver() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public void onExecution(BatchExecutionObservation observation) {
                throw new AssertionError("disabled batch observer must not be invoked");
            }
        };
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                sqlEvents::add, disabledBatchObserver, Mono::empty);
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(
                BatchChunkResult.committed(0, 0L, 1, 1L)));

        assertSame(result, support.observeBatchResult(request(), Mono.just(result)).block());

        assertEquals(1, sqlEvents.size());
        assertEquals(SqlExecutionStatus.SUCCESS, sqlEvents.getFirst().status());
    }

    @Test
    void disabledSqlObserverIsNotInvokedByBatchOnlyChunkFluxObservation() {
        SqlExecutionObserver disabledSqlObserver = new SqlExecutionObserver() {
            @Override
            public boolean enabled() {
                return false;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                throw new AssertionError("disabled SQL observer must not be invoked");
            }
        };
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                disabledSqlObserver, batchEvents::add, Mono::empty);
        BatchChunkResult committed = BatchChunkResult.committed(0, 0L, 1, 1L);

        assertEquals(List.of(committed), support.observeBatchChunks(
                request(), Flux.just(committed)).collectList().block());

        assertEquals(2, batchEvents.size());
        assertInstanceOf(BatchExecutionObservation.Summary.class, batchEvents.getLast());
    }

    @Test
    void retainsObservedCountsWhenChunkStreamFailsAfterACompletedChunk() {
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = support(sqlEvents, batchEvents);
        BatchChunkResult committed = BatchChunkResult.committed(0, 0L, 2, 2L);
        IllegalStateException failure = new IllegalStateException("stream interrupted");

        assertThrows(IllegalStateException.class, () -> support.observeBatchChunks(
                request(), Flux.concat(Flux.just(committed), Flux.error(failure))).collectList().block());

        BatchExecutionObservation.Summary summary = summary(batchEvents);
        assertEquals(BatchWriteResult.Status.UNKNOWN, summary.status());
        assertEquals(2L, summary.inputCount());
        assertEquals(2L, summary.affectedRows());
        assertEquals(1L, summary.chunkCount());
        assertEquals(1L, summary.successfulChunkCount());
        assertEquals(0L, summary.failedChunkCount());
        assertEquals(SqlExecutionStatus.ERROR, sqlEvents.getLast().status());
        assertEquals(2L, sqlEvents.getLast().rows());
        assertEquals(2, sqlEvents.getLast().batchSize());
    }

    @Test
    void reportsBatchWriteExceptionPartialCountsToTheSqlObserver() {
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = support(sqlEvents, batchEvents);
        BatchWriteResult partial = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                BatchChunkResult.committed(0, 0L, 2, 2L),
                BatchChunkResult.failed(1, 2L, 2, new IllegalStateException("write failed"))));
        BatchWriteException failure = new BatchWriteException(
                "batch failed", new IllegalStateException("write failed"), partial);

        assertSame(failure, assertThrows(BatchWriteException.class,
                                         () -> support.observeBatchResult(request(), Mono.error(failure)).block()));

        BatchExecutionObservation.Summary summary = summary(batchEvents);
        assertEquals(BatchWriteResult.Status.PARTIAL, summary.status());
        assertEquals(4L, summary.inputCount());
        assertEquals(2L, summary.affectedRows());
        assertEquals(2L, summary.chunkCount());
        assertEquals(SqlExecutionStatus.ERROR, sqlEvents.getLast().status());
        assertEquals(2L, sqlEvents.getLast().rows());
        assertEquals(4, sqlEvents.getLast().batchSize());
    }

    @Test
    void matchesBatchWriteResultForMixedEnlistedChunksAndDoesNotReportSqlSuccess() {
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = support(sqlEvents, batchEvents);
        List<BatchChunkResult> chunks = List.of(
                BatchChunkResult.committed(0, 0L, 1, 1L),
                new BatchChunkResult(1, 1L, 1, 0L, BatchChunkResult.Status.ENLISTED, null, null, List.of()));
        BatchWriteResult expected = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, chunks);

        assertEquals(chunks, support.observeBatchChunks(request(), Flux.fromIterable(chunks)).collectList().block());

        assertEquals(BatchWriteResult.Status.UNKNOWN, expected.status());
        assertEquals(expected.status(), summary(batchEvents).status());
        assertEquals(SqlExecutionStatus.ERROR, sqlEvents.getLast().status());
    }

    @Test
    void keepsAnExplicitUnknownMonoResultUnknownAndReportsSqlError() {
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = support(sqlEvents, batchEvents);
        BatchWriteResult unknown = new BatchWriteResult(
                BatchWriteOptions.Mode.INDEPENDENT,
                BatchWriteResult.Status.UNKNOWN,
                0L,
                0L,
                List.of());

        assertSame(unknown, support.observeBatchResult(request(), Mono.just(unknown)).block());

        BatchExecutionObservation.Summary summary = summary(batchEvents);
        assertEquals(BatchWriteResult.Status.UNKNOWN, summary.status());
        assertEquals(0L, summary.chunkCount());
        assertEquals(SqlExecutionStatus.ERROR, sqlEvents.getLast().status());
    }

    @Test
    void cancelledMonoBatchUsesTheResolvedExternalTransactionSource() {
        TransactionSourceObserver sqlObserver = new TransactionSourceObserver();
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                sqlObserver, BatchExecutionObserver.noop(), Mono::empty);

        Disposable subscription = support.observeBatchResult(
                request(), Mono.just(externalResolution()), ignored -> Mono.<BatchWriteResult>never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), sqlObserver.statuses());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), sqlObserver.transactionSources);
    }

    @Test
    void cancelledChunkBatchUsesTheResolvedExternalTransactionSource() {
        TransactionSourceObserver sqlObserver = new TransactionSourceObserver();
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                sqlObserver, BatchExecutionObserver.noop(), Mono::empty);

        Disposable subscription = support.observeBatchChunks(
                request(), Mono.just(externalResolution()), ignored -> Flux.<BatchChunkResult>never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), sqlObserver.statuses());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), sqlObserver.transactionSources);
    }

    private static ReactiveSqlExecutionObservationSupport support(List<SqlExecutionObservation> sqlEvents,
                                                                   List<BatchExecutionObservation> batchEvents) {
        return ReactiveSqlExecutionObservationSupport.create(sqlEvents::add, batchEvents::add, Mono::empty);
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.independent(2, 1));
    }

    private static BatchExecutionObservation.Summary summary(List<BatchExecutionObservation> events) {
        return assertInstanceOf(BatchExecutionObservation.Summary.class, events.getLast());
    }

    private static ReactiveTransactionSourceResolver.Resolution externalResolution() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "observation-external-connection";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new ReactiveTransactionSourceResolver.Resolution(
                SqlTransactionSource.EXTERNAL, R2dbcTransactionContext.external(connection));
    }

    private static final class TransactionSourceObserver implements SqlExecutionObserver {
        private final List<SqlExecutionObservation> events = new ArrayList<>();
        private final List<SqlTransactionSource> transactionSources = new ArrayList<>();

        @Override
        public boolean requiresTransactionSource() {
            return true;
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            events.add(observation);
        }

        @Override
        public void onExecution(SqlExecutionObservation observation,
                                SqlTransactionSource transactionSource) {
            events.add(observation);
            transactionSources.add(transactionSource);
        }

        private List<SqlExecutionStatus> statuses() {
            return events.stream().map(SqlExecutionObservation::status).toList();
        }
    }
}
