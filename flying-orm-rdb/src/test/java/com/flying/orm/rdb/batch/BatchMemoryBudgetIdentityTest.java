package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchMemoryBudgetIdentityTest {

    @Test
    void countsSharedByteArrayOnce() {
        byte[] payload = new byte[128];

        assertEquals(192L, BatchMemoryBudget.estimateValueBytes(new Object[]{payload, payload}));
    }

    @Test
    void countsSharedByteBufferOnce() {
        ByteBuffer payload = ByteBuffer.allocate(128);

        assertEquals(200L, BatchMemoryBudget.estimateValueBytes(new Object[]{payload, payload}));
    }

    @Test
    void countsSharedStringOnce() {
        String payload = "shared-payload";

        assertEquals(86L, BatchMemoryBudget.estimateValueBytes(new Object[]{payload, payload}));
    }

    @Test
    void countsSharedPrimitiveArrayOnce() {
        int[] payload = new int[32];

        assertEquals(192L, BatchMemoryBudget.estimateValueBytes(new Object[]{payload, payload}));
    }
}
