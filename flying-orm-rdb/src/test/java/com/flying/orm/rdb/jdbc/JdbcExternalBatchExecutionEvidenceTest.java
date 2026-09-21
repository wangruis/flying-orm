package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.exception.RdbErrorKind;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcExternalBatchExecutionEvidenceTest {

    @Test
    void reportsNoCommitFactWhenInputFailsBeforeAnyDatabaseExecution() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();


        JdbcBatchWriter writer = state.writer()
                ;

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.error(new IllegalStateException("input failed")), 2)));

        assertEquals(0, state.executions.get());
    }

    @Test
    void classifiesInputCancellationBeforeConnectionAcquisition() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
        JdbcBatchWriter writer = state.writer();
        CancellationException cancelled = new CancellationException("input cancelled");

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.error(cancelled), 2)));

        assertAll(
                () -> assertSame(cancelled, failure.getCause()),
                () -> assertEquals(0, state.acquired.get()),
                () -> assertEquals(BatchExecutionState.CANCELLED, failure.evidence().state()),
                () -> assertEquals(BatchExecutionState.CANCELLED,
                        failure.evidence().state()),
                () -> assertEquals(RdbErrorKind.CANCELLED,
                        failure.evidence().failure().kind()));
    }

    @Test
    void keepsCompletedExternalChunkWhenLaterInputIsCancelled() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);


        JdbcBatchWriter writer = state.writer()
                ;
        CancellationException cancelled = new CancellationException("input cancelled");

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.concat(Flux.<Object[]>just(new Object[]{1}), Flux.error(cancelled)), 1)));

        assertAll(
                () -> assertSame(cancelled, failure.getCause()),
                () -> assertEquals(1, state.executions.get()),
                () -> assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state()),
                () -> assertEquals(1, failure.evidence().successfulCount()),
                () -> assertEquals(1, failure.evidence().inputCount()));
    }

    @Test
    void classifiesCheckedInputTimeoutBeforeConnectionAcquisition() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
        JdbcBatchWriter writer = state.writer();
        TimeoutException timedOut = new TimeoutException("input timed out");

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.error(timedOut), 2)));

        assertAll(
                () -> assertSame(timedOut, failure.getCause()),
                () -> assertEquals(0, state.acquired.get()),
                () -> assertEquals(BatchExecutionState.TIMED_OUT, failure.evidence().state()),
                () -> assertEquals(BatchExecutionState.TIMED_OUT,
                        failure.evidence().state()),
                () -> assertEquals(RdbErrorKind.TIMEOUT,
                        failure.evidence().failure().kind()));
    }

    @Test
    void classifiesDatabaseLockTimeoutAsTimedOut() {
        BatchUpdateException lockTimeout = new BatchUpdateException(
                "lock unavailable", "55P03", 0, new int[0]);
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .failure(lockTimeout);
        JdbcBatchWriter writer = state.writer()
                ;

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.<Object[]>just(new Object[]{1}), 1)));

        assertAll(
                () -> assertSame(lockTimeout, failure.getCause()),
                () -> assertEquals(BatchExecutionState.TIMED_OUT, failure.evidence().state()),
                () -> assertEquals(BatchExecutionState.TIMED_OUT,
                        failure.evidence().state()),
                () -> assertEquals(RdbErrorKind.LOCK_TIMEOUT,
                        failure.evidence().failure().kind()));
    }

    @Test
    void retainsPendingExternalFactWhenInputFailsAfterDatabaseExecution() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);


        JdbcBatchWriter writer = state.writer()
                ;

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.concat(
                                Flux.<Object[]>just(new Object[]{1}),
                                Flux.error(new IllegalStateException("input failed"))),
                        1)));

        assertEquals(1, state.executions.get());
    }

    @Test
    void returnsCompletedChunkFactsWithoutWaitingForExternalCompletion() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, 1)
                .outcome(1);
        AtomicInteger completionRegistrations = new AtomicInteger();


        JdbcBatchWriter writer = state.writer()
                ;

        BatchExecutionEvidence evidence = writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 3).map(value -> new Object[]{value}), 2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(3L, evidence.affectedRows().value());
        assertEquals(3, evidence.inputCount());
        assertEquals(List.of(0L,1L,2L), evidence.successfulOffsets().boxed().toList());
        assertEquals(0, completionRegistrations.get());
    }

    @TestFactory
    Stream<DynamicTest> doesNotTreatSideIndexBatchCountsAsBusinessEvidence() {
        return Stream.of(false, true).map(continued ->
                DynamicTest.dynamicTest("external side-index continued=" + continued,
                        () -> assertSideIndexFailureEvidence(continued)));
    }

    private static void assertSideIndexFailureEvidence(boolean continued) {
        // One business row succeeded. These counts describe a separate, three-token batch.
        int[] tokenCounts = continued
                ? new int[]{Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED}
                : new int[]{1};
        BatchUpdateException tokenFailure = new BatchUpdateException(
                "side-index batch execution failed", "40001", 0, tokenCounts);
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1).failure(tokenFailure);


        JdbcBatchWriter writer = state.writer()
                ;
        JdbcBatchWriter executor = writer;
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> executor.writeProtectedBatchEvidence(protectedInsertRequest()));
        BatchExecutionEvidence evidence = failure.evidence();

        assertAll(
                () -> assertSame(tokenFailure, failure.getCause()),
                () -> assertEquals(BatchExecutionState.PARTIAL, evidence.state()),
                () -> assertEquals(1L, evidence.inputCount()),
                () -> assertEquals(List.of(0L), evidence.successfulOffsets().boxed().toList()),
                () -> assertEquals(List.of(), evidence.failedOffsets().boxed().toList()),
                () -> assertTrue(evidence.affectedRows().isKnown()),
                () -> assertEquals(1L, evidence.affectedRows().value()),
                () -> assertEquals(2, state.executions.get()),
                () -> assertEquals(0, state.commits.get()),
                () -> assertEquals(0, state.rollbacks.get()),
                () -> assertEquals(1, state.acquired.get()),
                () -> assertEquals(1, state.released.get()),
                () -> assertEquals(0, state.closes.get()));
    }

    private static BatchWriteRequest protectedInsertRequest() {
        SqlRequest insert = new SqlRequest("insert into samples(id, value) values (?, ?)",
                List.of(1L, "ciphertext"));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT, insert, null,
                List.of("id"), Map.of("id", 1L), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("value",
                        List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}))));
        Object[] row = ProtectedBatchRows.extend(insert.parameters().toArray(), work);
        return BatchWriteRequests.request(insert.sql(), 2, List.of(Long.class, String.class),
                SqlBindMarkerStyle.CANONICAL, Flux.<Object[]>just(row), BatchWriteOptions.of(1));
    }
}
