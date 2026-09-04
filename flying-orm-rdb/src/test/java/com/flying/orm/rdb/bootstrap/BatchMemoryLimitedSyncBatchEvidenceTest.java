package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchMemoryLimitedSyncBatchEvidenceTest {

    @Test
    void forwardsBothEvidenceEntriesWithoutChangingTheRequestOrEvidence() {
        for (boolean protectedBatch : new boolean[]{false, true}) {
            AtomicInteger subscriptions = new AtomicInteger();
            BatchWriteRequest request = request(1, subscriptions);
            EvidenceExecutor delegate = new EvidenceExecutor(request, protectedBatch);
            SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(
                    delegate, BatchMemoryLimits.defaults());

            BatchExecutionEvidence evidence = protectedBatch
                    ? limited.writeProtectedBatchEvidence(request) : limited.writeBatchEvidence(request);

            assertSame(delegate.evidence, evidence);
            assertEquals(1, delegate.calls);
            assertEquals(0, subscriptions.get());
        }
    }

    @Test
    void checksLimitsBeforeDelegatingEitherEvidenceEntry() {
        for (boolean protectedBatch : new boolean[]{false, true}) {
            AtomicInteger subscriptions = new AtomicInteger();
            BatchWriteRequest request = request(BatchMemoryLimits.DEFAULT_MAX_CHUNK_SIZE + 1, subscriptions);
            EvidenceExecutor delegate = new EvidenceExecutor(request, protectedBatch);
            SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(
                    delegate, BatchMemoryLimits.defaults());

            assertThrows(BatchMemoryLimitExceededException.class, () -> {
                if (protectedBatch) {
                    limited.writeProtectedBatchEvidence(request);
                } else {
                    limited.writeBatchEvidence(request);
                }
            });
            assertEquals(0, delegate.calls);
            assertEquals(0, subscriptions.get());
        }
    }

    private static BatchWriteRequest request(int chunkSize, AtomicInteger subscriptions) {
        return BatchWriteRequests.request(
                "insert into samples(value) values (?)", 1, List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}).doOnSubscribe(ignored -> subscriptions.incrementAndGet()),
                BatchWriteOptions.atomic(chunkSize));
    }

    private static final class EvidenceExecutor implements SyncBatchExecutor {
        private final BatchWriteRequest request;
        private final boolean protectedBatch;
        private final BatchExecutionEvidence evidence = BatchExecutionEvidence.of(
                BatchWriteOptions.Mode.ATOMIC, BatchExecutionState.SUCCESS,
                BatchCommitFact.NOT_APPLICABLE, List.of());
        private int calls;

        private EvidenceExecutor(BatchWriteRequest request, boolean protectedBatch) {
            this.request = request;
            this.protectedBatch = protectedBatch;
        }

        @Override
        public BatchExecutionEvidence writeBatchEvidence(BatchWriteRequest actual) {
            assertEquals(false, protectedBatch);
            return evidence(actual);
        }

        @Override
        public BatchExecutionEvidence writeProtectedBatchEvidence(BatchWriteRequest actual) {
            assertEquals(true, protectedBatch);
            return evidence(actual);
        }

        private BatchExecutionEvidence evidence(BatchWriteRequest actual) {
            assertSame(request, actual);
            calls++;
            return evidence;
        }

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            throw new AssertionError("evidence must not use the legacy batch result");
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            throw new AssertionError("evidence must not use the legacy chunk result");
        }
    }
}
