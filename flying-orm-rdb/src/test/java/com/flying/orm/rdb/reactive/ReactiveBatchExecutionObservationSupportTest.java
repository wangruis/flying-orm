package com.flying.orm.rdb.reactive;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import reactor.core.publisher.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ReactiveBatchExecutionObservationSupportTest {
    @Test void returnedConflictEvidenceKeepsBothObserverCategoriesConsistent() {
        BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(2);
        facts.succeeded(1, BatchAffectedRows.known(1));
        BitSet failed = new BitSet();
        failed.set(0);
        facts.terminal(1, new BitSet(), failed, BatchAffectedRows.known(0),
                List.of(BatchRowConflict.exactlyOne(1, 0)));
        BatchExecutionEvidence evidence = facts.snapshot(BatchExecutionState.PARTIAL, null);
        List<SqlExecutionObservation> sql = new ArrayList<>();
        List<BatchExecutionObservation> batch = new ArrayList<>();
        ReactiveSqlExecutionObservationSupport support =
                ReactiveSqlExecutionObservationSupport.create(sql::add, batch::add);
        assertSame(evidence, support.observeBatchResult(request(), Mono.just(evidence)).block());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, sql.getFirst().failureCategory());
        assertEquals(sql.getFirst().failureCategory(), batch.getFirst().failureCategory());
        assertEquals(1, sql.getFirst().rows());
        assertEquals(2, sql.getFirst().batchSize());
    }

    @Test void returnedFailureEvidenceKeepsBothObserverCategoriesConsistent() {
        for (String state : List.of("HYT00", "55P03", "23505")) {
            io.r2dbc.spi.R2dbcException driver = new io.r2dbc.spi.R2dbcException("driver", state, 0) { };
            BatchExecutionEvidence.Failure failure = BatchExecutionEvidence.Failure.from(driver);
            BatchExecutionEvidence evidence = new BatchExecutionEvidence.Accumulator().snapshot(
                    state.equals("23505") ? BatchExecutionState.FAILED : BatchExecutionState.TIMED_OUT, failure);
            List<SqlExecutionObservation> sql = new ArrayList<>();
            List<BatchExecutionObservation> batch = new ArrayList<>();
            ReactiveSqlExecutionObservationSupport support =
                    ReactiveSqlExecutionObservationSupport.create(sql::add, batch::add);
            assertSame(evidence, support.observeBatchResult(request(), Mono.just(evidence)).block());
            assertEquals(failure.kind().name(), sql.getFirst().failureCategory().name(), state);
            assertEquals(sql.getFirst().failureCategory(), batch.getFirst().failureCategory(), state);
        }
    }

    @Test void unwrappedEntryFailuresKeepBothObserverCategoriesAndBatchStateConsistent() {
        List<Throwable> errors = new ArrayList<>();
        for (String state : List.of("HYT00", "55P03", "23505", "57014")) {
            errors.add(new io.r2dbc.spi.R2dbcException("driver", state, 0) { });
        }
        errors.add(new java.util.concurrent.CancellationException());
        for (Throwable error : errors) {
            List<SqlExecutionObservation> sql = new ArrayList<>();
            List<BatchExecutionObservation> batch = new ArrayList<>();
            ReactiveSqlExecutionObservationSupport support =
                    ReactiveSqlExecutionObservationSupport.create(sql::add, batch::add);
            assertSame(error, assertThrows(RuntimeException.class,
                    () -> support.observeBatchResult(request(), Mono.error(error)).block()));
            BatchExecutionObservation summary = batch.getFirst();
            assertEquals(summary.failure().kind().name(), summary.failureCategory().name());
            assertEquals(summary.failureCategory(), sql.getFirst().failureCategory());
            assertEquals(switch (summary.failureCategory()) {
                case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
                case CANCELLED -> BatchExecutionState.CANCELLED;
                default -> BatchExecutionState.FAILED;
            }, summary.state());
        }
    }

    @Test void nativeDriverFailuresKeepBatchStateAndBothObserverCategoriesConsistent() {
        Map<String, SqlFailureCategory> cases = Map.of(
                "23505", SqlFailureCategory.DUPLICATE_KEY,
                "HYT00", SqlFailureCategory.TIMEOUT,
                "55P03", SqlFailureCategory.LOCK_TIMEOUT,
                "57014", SqlFailureCategory.CANCELLED);
        cases.forEach((state, category) -> {
            io.r2dbc.spi.R2dbcException driver = new io.r2dbc.spi.R2dbcException("driver", state, 0) { };
            R2dbcOrdinaryBatchConnectionTest.Harness harness =
                    new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.error(driver));
            List<SqlExecutionObservation> sql = new ArrayList<>();
            List<BatchExecutionObservation> batch = new ArrayList<>();
            BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                    () -> harness.executor().withObservers(sql::add, batch::add)
                            .writeBatch(R2dbcOrdinaryBatchConnectionTest.request(
                                    1, 1, BatchRowCountPolicy.EXACTLY_ONE)).block());
            BatchExecutionState expectedState = switch (category) {
                case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
                case CANCELLED -> BatchExecutionState.CANCELLED;
                default -> BatchExecutionState.FAILED;
            };
            assertEquals(expectedState, failure.evidence().state(), state);
            assertEquals(category.name(), failure.evidence().failure().kind().name(), state);
            assertEquals(category, sql.getFirst().failureCategory(), state);
            assertEquals(category, batch.getLast().failureCategory(), state);
            assertEquals(List.of(SignalType.ON_ERROR), harness.releases);
        });
    }

    @Test void rowCountConflictsHaveTheSameCategoryForSqlAndBatchObservers() {
        R2dbcOrdinaryBatchConnectionTest.Harness harness =
                new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(0L));
        List<SqlExecutionObservation> sql = new ArrayList<>();
        List<BatchExecutionObservation> batch = new ArrayList<>();
        assertThrows(BatchExecutionEvidenceException.class,
                () -> harness.executor().withObservers(sql::add, batch::add)
                        .writeBatch(R2dbcOrdinaryBatchConnectionTest.request(
                                1, 1, BatchRowCountPolicy.EXACTLY_ONE)).block());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, sql.getFirst().failureCategory());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, batch.getLast().failureCategory());
    }

    @Test void disabledBatchObserverIsNotInvokedBySqlOnlyMonoObservation() {
        List<SqlExecutionObservation> sql = new ArrayList<>();
        BatchExecutionObserver disabled = new BatchExecutionObserver() {
            public boolean enabled() { return false; }
            public void onExecution(BatchExecutionObservation event) { fail("disabled observer invoked"); }
        };
        var support = ReactiveSqlExecutionObservationSupport.create(sql::add, disabled);
        var evidence = new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null);
        assertSame(evidence, support.observeBatchResult(request(), Mono.just(evidence)).block());
        assertEquals(1, sql.size());
    }

    @Test void batchOnlyObservationDoesNotInvokeDisabledSqlObserver() {
        List<BatchExecutionObservation> events = new ArrayList<>();
        SqlExecutionObserver disabled = new SqlExecutionObserver() {
            public boolean enabled() { return false; }
            public void onExecution(SqlExecutionObservation event) { fail("disabled observer invoked"); }
        };
        var support = ReactiveSqlExecutionObservationSupport.create(disabled, events::add);
        support.observeBatchResult(request(), Mono.just(
                new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null))).block();
        assertEquals(1, events.size());
        assertEquals(BatchExecutionEventType.SUMMARY, events.getFirst().eventType());
    }

    @Test void sqlAndBatchObserversPreservePartialFactsOnError() {
        List<SqlExecutionObservation> sql = new ArrayList<>();
        List<BatchExecutionObservation> events = new ArrayList<>();
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(3);
        facts.succeeded(2, BatchAffectedRows.known(2));
        var evidence = facts.snapshot(BatchExecutionState.PARTIAL, null);
        var error = new BatchExecutionEvidenceException("execution failed", new IllegalStateException(), evidence);
        var support = ReactiveSqlExecutionObservationSupport.create(sql::add, events::add);
        assertSame(error, assertThrows(BatchExecutionEvidenceException.class,
                () -> support.observeBatchResult(request(), Mono.error(error)).block()));
        assertEquals(2, sql.getFirst().rows());
        assertEquals(3, sql.getFirst().batchSize());
        assertEquals(SqlExecutionStatus.ERROR, sql.getFirst().status());
        assertEquals(2, events.getFirst().successfulCount());
        assertEquals(3, events.getFirst().inputCount());
    }

    @Test void unknownEvidenceIsNotReportedAsSuccessfulSql() {
        List<SqlExecutionObservation> sql = new ArrayList<>();
        var support = ReactiveSqlExecutionObservationSupport.create(sql::add, BatchExecutionObserver.noop());
        var facts = new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.UNKNOWN, null);
        assertSame(facts, support.observeBatchResult(request(), Mono.just(facts)).block());
        assertEquals(SqlExecutionStatus.ERROR, sql.getFirst().status());
    }

    @Test void cancelledBatchPublishesSqlCancellation() {
        List<SqlExecutionObservation> sql = new ArrayList<>();
        var support = ReactiveSqlExecutionObservationSupport.create(sql::add, BatchExecutionObserver.noop());
        var subscription = support.observeBatchResult(request(), Mono.never()).subscribe();
        subscription.dispose();
        assertEquals(SqlExecutionStatus.CANCELLED, sql.getFirst().status());
    }

    private static BatchWriteRequest request() {
        return R2dbcOrdinaryBatchConnectionTest.request(0, 1, BatchRowCountPolicy.ANY);
    }
}
