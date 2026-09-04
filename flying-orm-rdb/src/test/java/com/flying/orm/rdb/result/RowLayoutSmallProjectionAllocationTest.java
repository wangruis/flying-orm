package com.flying.orm.rdb.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.util.List;

import org.junit.jupiter.api.Test;

class RowLayoutSmallProjectionAllocationTest {

    private static final int WARMUP_ITERATIONS = 2_000;
    private static final int MEASURED_ITERATIONS = 10_000;
    // The retained layout measures about 408 bytes on the supported JDK; the former two-map path used about 856.
    private static final long MAX_BYTES_PER_LAYOUT = 512L;

    private static volatile RowLayout sink;

    @Test
    void smallProjectionDoesNotBuildDiscardedHashIndexes() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean,
                   "per-thread allocation counters are unavailable");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported(),
                   "per-thread allocation counters are unsupported");
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        assumeTrue(allocationBean.isThreadAllocatedMemoryEnabled(),
                   "per-thread allocation counters could not be enabled");

        List<String> columns = List.of("ID", "NAME", "VALUE");
        for (int iteration = 0; iteration < WARMUP_ITERATIONS; iteration++) {
            sink = RowLayout.of(columns);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < MEASURED_ITERATIONS; iteration++) {
            sink = RowLayout.of(columns);
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        long bytesPerLayout = allocated / MEASURED_ITERATIONS;

        assertEquals(3, sink.size());
        assertTrue(bytesPerLayout <= MAX_BYTES_PER_LAYOUT,
                   () -> "small row layout allocated discarded indexes: bytesPerLayout=" + bytesPerLayout);
    }
}
