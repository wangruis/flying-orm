package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;
import static org.junit.jupiter.api.Assertions.*;

class BatchMemoryLimitedSyncBatchEvidenceTest {
    @Test
    void forwardsOrdinaryAndEvidenceEntriesWithoutChangingTheRequestOrEvidence() {
        for (boolean protectedBatch : new boolean[]{false, true}) for (boolean evidenceAlias : new boolean[]{false, true}) {
            AtomicInteger subscriptions = new AtomicInteger();
            BatchWriteRequest request = request(1, subscriptions);
            EvidenceExecutor delegate = new EvidenceExecutor(request, protectedBatch);
            SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(delegate, BatchMemoryLimits.defaults());
            BatchExecutionEvidence result = invoke(limited, request, protectedBatch, evidenceAlias);
            assertSame(delegate.evidence, result);
            assertEquals(1, delegate.calls);
            assertNull(delegate.callback);
            assertEquals(0, subscriptions.get());
        }
    }

    @Test
    void forwardsRowCompletedCallbackUnchanged() {
        for (boolean protectedBatch : new boolean[]{false, true}) {
            BatchWriteRequest request = request(1, new AtomicInteger());
            EvidenceExecutor delegate = new EvidenceExecutor(request, protectedBatch);
            SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(delegate, BatchMemoryLimits.defaults());
            LongConsumer callback = offset -> fail("wrapper must not invoke the callback itself");
            BatchExecutionEvidence result = protectedBatch
                    ? limited.writeProtectedBatch(request, callback) : limited.writeBatch(request, callback);
            assertSame(delegate.evidence, result);
            assertSame(callback, delegate.callback);
            assertEquals(1, delegate.calls);
        }
    }

    @Test
    void checksLimitsBeforeDelegatingOrSubscribingEveryEntry() {
        for (boolean protectedBatch : new boolean[]{false, true}) for (boolean evidenceAlias : new boolean[]{false, true}) {
            AtomicInteger subscriptions = new AtomicInteger();
            BatchWriteRequest request = request(BatchMemoryLimits.DEFAULT_MAX_BUFFER_SIZE + 1, subscriptions);
            EvidenceExecutor delegate = new EvidenceExecutor(request, protectedBatch);
            SyncBatchExecutor limited = BatchMemoryLimitedSyncBatchExecutor.create(delegate, BatchMemoryLimits.defaults());
            assertThrows(BatchMemoryLimitExceededException.class,
                    () -> invoke(limited, request, protectedBatch, evidenceAlias));
            assertThrows(BatchMemoryLimitExceededException.class, () -> {
                if (protectedBatch) limited.writeProtectedBatch(request, offset -> fail());
                else limited.writeBatch(request, offset -> fail());
            });
            assertEquals(0, delegate.calls);
            assertEquals(0, subscriptions.get());
        }
    }

    private static BatchExecutionEvidence invoke(SyncBatchExecutor executor, BatchWriteRequest request,
                                                  boolean protectedBatch, boolean evidence) {
        if (protectedBatch) return evidence ? executor.writeProtectedBatchEvidence(request) : executor.writeProtectedBatch(request);
        return evidence ? executor.writeBatchEvidence(request) : executor.writeBatch(request);
    }

    private static BatchWriteRequest request(int bufferSize, AtomicInteger subscriptions) {
        return BatchWriteRequests.request("insert into samples(value) values (?)", 1, List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}).doOnSubscribe(ignored -> subscriptions.incrementAndGet()),
                BatchWriteOptions.of(bufferSize));
    }

    private static final class EvidenceExecutor implements SyncBatchExecutor {
        private final BatchWriteRequest request;
        private final boolean protectedBatch;
        private final BatchExecutionEvidence evidence =
                new BatchExecutionEvidence.Accumulator().snapshot(BatchExecutionState.SUCCESS, null);
        private int calls;
        private LongConsumer callback;

        private EvidenceExecutor(BatchWriteRequest request, boolean protectedBatch) {
            this.request = request;
            this.protectedBatch = protectedBatch;
        }

        public BatchExecutionEvidence writeBatch(BatchWriteRequest actual, LongConsumer rowCompleted) {
            assertFalse(protectedBatch);
            return evidence(actual, rowCompleted);
        }

        public BatchExecutionEvidence writeProtectedBatch(BatchWriteRequest actual, LongConsumer rowCompleted) {
            assertTrue(protectedBatch);
            return evidence(actual, rowCompleted);
        }

        private BatchExecutionEvidence evidence(BatchWriteRequest actual, LongConsumer rowCompleted) {
            assertSame(request, actual);
            callback = rowCompleted;
            calls++;
            return evidence;
        }
    }
}
