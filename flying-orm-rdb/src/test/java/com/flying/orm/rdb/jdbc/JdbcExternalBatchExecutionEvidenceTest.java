package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcExternalBatchExecutionEvidenceTest {

    @Test
    void reportsNoCommitFactWhenInputFailsBeforeAnyDatabaseExecution() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.error(new IllegalStateException("input failed")), 2)));

        assertEquals(0, state.executions.get());
        assertEquals(BatchCommitFact.NOT_APPLICABLE, failure.evidence().commitFact());
    }

    @Test
    void retainsPendingExternalFactWhenInputFailsAfterDatabaseExecution() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.concat(
                                Flux.<Object[]>just(new Object[]{1}),
                                Flux.error(new IllegalStateException("input failed"))),
                        1)));

        assertEquals(1, state.executions.get());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, failure.evidence().commitFact());
    }

    @Test
    void returnsCompletedChunkFactsWithoutWaitingForExternalCompletion() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, 1)
                .outcome(1);
        AtomicInteger completionRegistrations = new AtomicInteger();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(
                state.connection(), listener -> {
                    completionRegistrations.incrementAndGet();
                    return true;
                });
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidence evidence = writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 3).map(value -> new Object[]{value}), 2));

        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertTrue(evidence.affectedRows().isKnown());
        assertEquals(3L, evidence.affectedRows().value());
        assertEquals(List.of(2, 1), evidence.chunks().stream().map(chunk -> chunk.inputCount()).toList());
        assertEquals(List.of(0L, 2L), evidence.chunks().stream().map(chunk -> chunk.startOffset()).toList());
        assertEquals(0, completionRegistrations.get());
    }
}
