package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveSqlExecutionObservationSupportTest {

    @Test
    void defaultStreamingProtectionReturnsTheOriginalPublisher() {
        Flux<Integer> rows = Flux.just(1);

        assertSame(rows, ReactiveSqlExecutionProtection.protectRows(
                rows, "select value from samples", SqlExecutionOptions.safeDefaults(), null));
    }

    @Test
    void explicitStreamingRowLimitRemainsEnforced() {
        Flux<Integer> rows = Flux.just(1, 2);

        assertThrows(SqlRowLimitExceededException.class,
                     () -> ReactiveSqlExecutionProtection.protectRows(
                                     rows,
                                     "select value from samples",
                                     SqlExecutionOptions.safeDefaults().withMaxRows(1),
                                     null)
                             .collectList()
                             .block());
    }

    @Test
    void explicitStreamingResultBudgetRemainsEnforced() {
        Flux<Integer> rows = Flux.just(1);

        assertThrows(SqlResultMemoryLimitExceededException.class,
                     () -> ReactiveSqlExecutionProtection.protectRows(
                                     rows,
                                     "select value from samples",
                                     SqlExecutionOptions.safeDefaults().withMaxResultBytes(1),
                                     ignored -> 2L)
                             .collectList()
                             .block());
    }

    @Test
    void returnsTheOriginalPublishersWhenObservationIsDisabled() {
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                SqlExecutionObserver.noop(), BatchExecutionObserver.noop(), Mono::empty);
        Flux<Integer> rows = Flux.just(1);
        Mono<Long> update = Mono.just(1L);
        BatchWriteRequest batchRequest = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.defaults());
        Mono<BatchWriteResult> batch = Mono.never();
        Flux<BatchChunkResult> chunks = Flux.never();

        assertSame(rows, support.observeFlux(
                SqlExecutionOperation.QUERY, new SqlRequest("select 1", List.of()), 0, rows));
        assertSame(update, support.observeMono(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("update users set active = true", List.of()),
                0,
                update));
        assertSame(batch, support.observeBatchResult(batchRequest, batch));
        assertSame(chunks, support.observeBatchChunks(batchRequest, chunks));
    }

    @Test
    void doesNotCreateOrStackExecutorDecoratorsForDisabledObservation() {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.just(0L);
            }
        };

        assertSame(executor, executor.withObservers(
                SqlExecutionObserver.noop(), BatchExecutionObserver.noop()));

        ReactiveSqlExecutor observed = executor.withObserver(ignored -> { })
                .withObserver(SqlExecutionObserver.noop());
        assertSame(executor, ((ForwardingReactiveSqlExecutor) observed).delegate());
    }

    @Test
    void doesNotReportNonSuccessfulBatchResultsAsSuccessfulSqlExecutions() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                events::add, ignored -> { }, Mono::empty);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.independent(1, 1));
        BatchWriteResult partial = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                BatchChunkResult.committed(0, 0L, 1, 1L),
                BatchChunkResult.failed(1, 1L, 1, new IllegalStateException("write failed"))));
        BatchWriteResult unknown = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                BatchChunkResult.unknown(0, 0L, 1, new IllegalStateException("commit unknown"))));
        BatchWriteResult rolledBack = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(
                BatchChunkResult.rolledBack(0, 0L, 1)));

        assertSame(partial, support.observeBatchResult(request, Mono.just(partial)).block());
        assertSame(unknown, support.observeBatchResult(request, Mono.just(unknown)).block());
        assertSame(rolledBack, support.observeBatchResult(request, Mono.just(rolledBack)).block());

        assertEquals(List.of(SqlExecutionStatus.ERROR, SqlExecutionStatus.ERROR, SqlExecutionStatus.ERROR),
                     events.stream().map(SqlExecutionObservation::status).toList());
    }

    @Test
    void doesNotReportFailedOrUnknownFluxChunksAsSuccessfulSqlExecutions() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                events::add, ignored -> { }, Mono::empty);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.independent(1, 1));

        BatchChunkResult committed = BatchChunkResult.committed(0, 0L, 1, 1L);
        BatchChunkResult failed = BatchChunkResult.failed(
                0, 0L, 1, new IllegalStateException("write failed"));
        BatchChunkResult unknown = BatchChunkResult.unknown(
                0, 0L, 1, new IllegalStateException("commit unknown"));
        BatchChunkResult conflicted = BatchChunkResult.conflicted(
                0, 0L, 1, List.of(BatchRowConflict.exactlyOne(0L, 0L)));
        BatchChunkResult rolledBack = BatchChunkResult.rolledBack(0, 0L, 1);

        assertEquals(List.of(committed), support.observeBatchChunks(request, Flux.just(committed))
                                                  .collectList()
                                                  .block());
        assertEquals(List.of(failed), support.observeBatchChunks(request, Flux.just(failed))
                                               .collectList()
                                               .block());
        assertEquals(List.of(unknown), support.observeBatchChunks(request, Flux.just(unknown))
                                                .collectList()
                                                .block());
        assertEquals(List.of(conflicted), support.observeBatchChunks(request, Flux.just(conflicted))
                                                    .collectList()
                                                    .block());
        assertEquals(List.of(rolledBack), support.observeBatchChunks(request, Flux.just(rolledBack))
                                                   .collectList()
                                                   .block());

        assertEquals(List.of(SqlExecutionStatus.SUCCESS,
                             SqlExecutionStatus.ERROR,
                             SqlExecutionStatus.ERROR,
                             SqlExecutionStatus.ERROR,
                             SqlExecutionStatus.ERROR),
                     events.stream().map(SqlExecutionObservation::status).toList());
    }
}
