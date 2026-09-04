package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchEvidenceNoTransactionOwnershipTest {

    @Test
    void neverControlsOrClosesAnExternalConnection() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1, 1);
        JdbcTransactionContext transaction = JdbcTransactionContext.external(state.connection());
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchExecutionEvidence evidence = writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 2).map(value -> new Object[]{value}), 2));

        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertEquals(0, state.acquired.get());
        assertEquals(0, state.autoCommitReads.get());
        assertEquals(0, state.autoCommitWrites.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closes.get());
    }
}
