package com.flying.orm.rdb.reactive;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import reactor.core.publisher.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ReactiveBatchExecutionObservationSupportTest {
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
