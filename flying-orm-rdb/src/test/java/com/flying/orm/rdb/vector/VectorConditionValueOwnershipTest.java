package com.flying.orm.rdb.vector;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class VectorConditionValueOwnershipTest {

    private static final int DIMENSIONS = 4_096;

    private volatile float[] vectorSink;

    @Test
    void conditionTakesTheArrayAlreadyOwnedByTheCodecAndKeepsPublicReadsIsolated() {
        float[] encoded = VectorValueCodec.write(List.of(1D, 2D), 2);

        VectorConditionValue value = new VectorConditionValue(encoded, 3D, VectorMetric.L2);

        assertSame(encoded, value.ownedVector());
        float[] published = value.vector();
        assertNotSame(encoded, published);
        published[0] = 9F;
        assertEquals(1F, value.ownedVector()[0]);
    }

    @Test
    void collectionConversionFillsTheFinalVectorWithoutAnIntermediateList() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        List<Double> values = Collections.nCopies(DIMENSIONS, 1D);
        for (int iteration = 0; iteration < 100; iteration++) {
            vectorSink = VectorValueCodec.write(values, DIMENSIONS);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        vectorSink = VectorValueCodec.write(values, DIMENSIONS);
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

        assertEquals(DIMENSIONS, vectorSink.length);
        assertTrue(allocated < DIMENSIONS * 8L,
                   "vector conversion retained an intermediate list: allocated=" + allocated);
    }

    @Test
    void directFinalVectorKeepsDimensionElementAndFiniteGuards() {
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(List.of(1D), 2));
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(List.of("1"), 1));
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(List.of(Double.NaN), 1));
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(List.of(Double.POSITIVE_INFINITY), 1));
    }
}
