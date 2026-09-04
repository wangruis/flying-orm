package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorPageQueryTrustedConstructionTest {

    private static final int ITERATIONS = 200;

    private static final long MAX_FACTORY_OVERHEAD = 1_024;

    @Test
    void publicConstructionAndFactoriesStayDefensive() {
        List<Object> values = new ArrayList<>(List.of(7));
        CursorPageQuery query = CursorPageQuery.after(20, values, CursorSort.asc("id"));
        values.set(0, 9);

        assertEquals(List.of(7), query.cursor());
        assertThrows(UnsupportedOperationException.class, () -> query.cursor().add(8));

        assertThrows(UnsupportedOperationException.class, () -> query.sorts().clear());

        List<Object> nullableCursor = new ArrayList<>();
        nullableCursor.add(null);
        assertThrows(NullPointerException.class,
                     () -> CursorPageQuery.after(20, nullableCursor, CursorSort.asc("id")));
    }

    @Test
    void factoryDoesNotTakeAShallowSnapshotBeforeTheCanonicalDeepSnapshot() {
        java.lang.management.ThreadMXBean standardBean = ManagementFactory.getThreadMXBean();
        assumeTrue(standardBean instanceof com.sun.management.ThreadMXBean,
                   "per-thread allocation counters are unavailable");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported(),
                   "per-thread allocation counters are unsupported");
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        assumeTrue(allocationBean.isThreadAllocatedMemoryEnabled(),
                   "per-thread allocation counters could not be enabled");
        List<Object> cursor = new ArrayList<>(IntStream.range(0, 1_000).boxed().toList());
        CursorSort sort = CursorSort.asc("id");
        Supplier<CursorPageQuery> constructor = () -> new CursorPageQuery(20, List.of(sort), cursor);
        Supplier<CursorPageQuery> factory = () -> CursorPageQuery.after(20, cursor, sort);
        for (int iteration = 0; iteration < 100; iteration++) {
            constructor.get();
            factory.get();
        }

        long constructorAllocation = allocatedBytes(allocationBean, constructor);
        long factoryAllocation = allocatedBytes(allocationBean, factory);
        long overhead = (factoryAllocation - constructorAllocation) / ITERATIONS;

        assertTrue(overhead < MAX_FACTORY_OVERHEAD,
                   () -> "cursor factory made an extra list snapshot: constructor="
                           + constructorAllocation + ", factory=" + factoryAllocation
                           + ", overhead=" + overhead);
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean allocationBean,
                                       Supplier<CursorPageQuery> factory) {
        CursorPageQuery[] results = new CursorPageQuery[ITERATIONS];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            results[iteration] = factory.get();
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        assertEquals(1_000, results[ITERATIONS - 1].cursor().size());
        return allocated;
    }
}
