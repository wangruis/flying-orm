package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class R2dbcBatchReceiptAllocationTest {

    private static final int PARAMETER_COUNT = 4_096;

    private volatile String hashSink;

    @Test
    void workOnlyReceiptHashDoesNotAllocateParameterPrefixArrays() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }

        Object[] parameters = new Object[PARAMETER_COUNT];
        Arrays.fill(parameters, "value");
        ProtectedWriteWork work = work(parameters);
        R2dbcBatchWriterChunks.BatchChunk plain = chunk(
                ProtectedBatchRows.decode(parameters, PARAMETER_COUNT));
        R2dbcBatchWriterChunks.BatchChunk extended = chunk(ProtectedBatchRows.decode(
                ProtectedBatchRows.extend(parameters, work), PARAMETER_COUNT));
        R2dbcBatchReceiptSupport receipts = new R2dbcBatchReceiptSupport();

        assertEquals(receiptHashes(receipts, plain), receiptHashes(receipts, extended));
        for (int iteration = 0; iteration < 5; iteration++) {
            receiptHashes(receipts, plain);
            receiptHashes(receipts, extended);
        }

        long smallestExtraAllocation = Long.MAX_VALUE;
        for (int iteration = 0; iteration < 5; iteration++) {
            long plainBytes = allocatedBytes(allocationBean, receipts, plain);
            long extendedBytes = allocatedBytes(allocationBean, receipts, extended);
            smallestExtraAllocation = Math.min(smallestExtraAllocation, extendedBytes - plainBytes);
        }

        assertTrue(smallestExtraAllocation < PARAMETER_COUNT * 2L,
                   "work-only receipt hash retained a parameter-prefix array: extra bytes="
                           + smallestExtraAllocation);
    }

    private long allocatedBytes(com.sun.management.ThreadMXBean bean,
                                R2dbcBatchReceiptSupport receipts,
                                R2dbcBatchWriterChunks.BatchChunk chunk) {
        long threadId = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(threadId);
        hashSink = receiptHashes(receipts, chunk);
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static String receiptHashes(R2dbcBatchReceiptSupport receipts,
                                        R2dbcBatchWriterChunks.BatchChunk chunk) {
        String chunkHash = receipts.chunkPayloadHash(chunk);
        BatchReceiptDigest digest = receipts.newPayloadDigest();
        receipts.updatePayload(digest, chunk);
        return chunkHash + receipts.finishPayload(digest);
    }

    private static R2dbcBatchWriterChunks.BatchChunk chunk(ProtectedBatchRows.RowView row) {
        return new R2dbcBatchWriterChunks.BatchChunk(0, 0L, List.of(row), 0L);
    }

    private static ProtectedWriteWork work(Object[] parameters) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("update samples set value = ?", Arrays.asList(parameters)),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from sample_tokens where id = ?",
                "insert into sample_tokens(id, token) values (?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("value", List.of(new byte[]{3}))));
    }
}
