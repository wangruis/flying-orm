package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchPartialCountsTest {

    @Test
    void preservesProvableBatchUpdateExceptionCounts() {
        BatchUpdateException partial = new BatchUpdateException(
                "partial batch", "23000", 0,
                new int[]{1, Statement.SUCCESS_NO_INFO, Statement.EXECUTE_FAILED});
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().failure(partial);
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.range(0, 4).map(value -> new Object[]{value}), 4)));

        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(BatchCommitFact.PENDING_EXTERNAL, failure.evidence().commitFact());
        assertFalse(failure.evidence().affectedRows().isKnown());
        BatchChunkExecutionFact chunk = failure.evidence().chunks().getFirst();
        assertEquals(List.of(0L, 1L), chunk.successfulOffsets());
        assertEquals(List.of(2L), chunk.failedOffsets());
    }

    @Test
    void keepsUnreportedBatchUpdateExceptionTailUnknown() {
        BatchUpdateException partial = new BatchUpdateException(
                "partial batch", "23000", 0, new int[]{1});
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().failure(partial);
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                        Flux.range(0, 4).map(value -> new Object[]{value}), 4)));

        BatchChunkExecutionFact chunk = failure.evidence().chunks().getFirst();
        assertFalse(failure.evidence().affectedRows().isKnown());
        assertFalse(chunk.affectedRows().isKnown());
        assertEquals(List.of(0L), chunk.successfulOffsets());
        assertTrue(chunk.failedOffsets().isEmpty());
    }
}
