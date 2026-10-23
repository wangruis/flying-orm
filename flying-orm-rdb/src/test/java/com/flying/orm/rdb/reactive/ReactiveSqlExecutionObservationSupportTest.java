package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
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
                rows, "select value from samples", SqlExecutionOptions.unlimited(), null));
    }

    @Test
    void explicitStreamingRowLimitRemainsEnforced() {
        Flux<Integer> rows = Flux.just(1, 2);

        assertThrows(SqlRowLimitExceededException.class,
                     () -> ReactiveSqlExecutionProtection.protectRows(
                                     rows,
                                     "select value from samples",
                                     SqlExecutionOptions.unlimited().withMaxRows(1),
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
                                     SqlExecutionOptions.unlimited().withMaxResultBytes(1),
                                     ignored -> 2L)
                             .collectList()
                             .block());
    }

    @Test
    void resultEstimationFailureKeepsItsOriginalCauseInsteadOfReportingAnExceededBudget() {
        IllegalStateException failure = new IllegalStateException("result value cannot be inspected");
        java.math.BigDecimal value = new java.math.BigDecimal("1") {
            @Override
            public int precision() { throw failure; }
        };
        DynamicRow row = DynamicRow.copyOf(java.util.Map.of("value", value));
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.just(row); }
            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
        };
        SqlRequest request = new SqlRequest("select value from samples", List.of());

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> executor.query(request, SqlExecutionOptions.unlimited().withMaxResultBytes(1024))
                        .collectList().block()));
        assertEquals(List.of(row), executor.query(request, SqlExecutionOptions.unlimited()).collectList().block());
    }

    @Test
    void returnsTheOriginalPublishersWhenObservationIsDisabled() {
        ReactiveSqlExecutionObservationSupport support = ReactiveSqlExecutionObservationSupport.create(
                SqlExecutionObserver.noop(), BatchExecutionObserver.noop());
        Flux<Integer> rows = Flux.just(1);
        Mono<Long> update = Mono.just(1L);
        BatchWriteRequest batchRequest = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.defaults());
        Mono<BatchExecutionEvidence> batch = Mono.never();

        assertSame(rows, support.observeFlux(
                SqlExecutionOperation.QUERY, new SqlRequest("select 1", List.of()), 0, rows));
        assertSame(update, support.observeMono(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("update users set active = true", List.of()),
                0,
                update));
        assertSame(batch, support.observeBatchResult(batchRequest, batch));
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
                events::add, BatchExecutionObserver.noop());
        BatchWriteRequest request = R2dbcOrdinaryBatchConnectionTest.request(0, 1,
                com.flying.orm.rdb.batch.BatchRowCountPolicy.ANY);
        for (BatchExecutionState state : List.of(BatchExecutionState.PARTIAL,
                BatchExecutionState.UNKNOWN, BatchExecutionState.FAILED)) {
            BatchExecutionEvidence evidence = new BatchExecutionEvidence.Accumulator().snapshot(state, null);
            assertSame(evidence, support.observeBatchResult(request, Mono.just(evidence)).block());
        }
        assertEquals(List.of(SqlExecutionStatus.ERROR, SqlExecutionStatus.ERROR, SqlExecutionStatus.ERROR),
                events.stream().map(SqlExecutionObservation::status).toList());
    }
}
