package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchEvidenceNoTransactionOwnershipTest {

    @Test
    void neverControlsOrClosesAnExternalConnection() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1, 1);


        JdbcBatchWriter writer = state.writer()
                ;

        BatchExecutionEvidence evidence = writer.writeBatchEvidence(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 2).map(value -> new Object[]{value}), 2));

        assertEquals(1, state.acquired.get());
        assertEquals(1, state.released.get());
        assertEquals(0, state.autoCommitReads.get());
        assertEquals(0, state.autoCommitWrites.get());
        assertEquals(0, state.commits.get());
        assertEquals(0, state.rollbacks.get());
        assertEquals(0, state.closes.get());
    }
}
