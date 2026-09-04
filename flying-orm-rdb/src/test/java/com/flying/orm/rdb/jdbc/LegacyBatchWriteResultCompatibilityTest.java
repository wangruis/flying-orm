package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LegacyBatchWriteResultCompatibilityTest {

    @Test
    void keepsTheExistingExternalTransactionResultAndCompletionContract() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1, 1);
        AtomicInteger completionRegistrations = new AtomicInteger();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(
                state.connection(), listener -> {
                    completionRegistrations.incrementAndGet();
                    return true;
                });
        JdbcBatchWriter writer = JdbcBatchWriter.create(state.dataSource())
                .withTransactionParticipant(() -> Optional.of(transaction));

        BatchWriteResult result = writer.writeBatch(JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, 2).map(value -> new Object[]{value}), 2));

        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
        assertEquals(2L, result.inputCount());
        assertEquals(0L, result.affectedRows());
        assertEquals(1, completionRegistrations.get());
    }
}
