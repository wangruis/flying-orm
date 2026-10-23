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

class KeysetPageQueryTrustedConstructionTest {

    private static final int ITERATIONS = 200;

    private static final long MAX_FACTORY_OVERHEAD = 1_024;

    @Test
    void publicConstructionAndFactoriesStayDefensive() {
        List<Object> values = new ArrayList<>(List.of(7));
        KeysetPageQuery query = KeysetPageQuery.after(20, com.flying.orm.core.page.CursorPosition.of(values), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        values.set(0, 9);

        assertEquals(List.of(7), query.position().values());
        assertThrows(UnsupportedOperationException.class, () -> query.position().values().add(8));

        assertThrows(UnsupportedOperationException.class, () -> query.sorts().clear());

        List<Object> nullableCursor = new ArrayList<>();
        nullableCursor.add(null);
        KeysetPageQuery nullable = KeysetPageQuery.after(20, CursorPosition.of(nullableCursor),
                KeysetSort.asc("label", NullOrder.LAST));
        nullableCursor.set(0, "changed");
        assertEquals(java.util.Collections.singletonList(null), nullable.position().values());
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
        KeysetSort sort = KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST);
        Supplier<KeysetPageQuery> constructor = () -> new KeysetPageQuery(20, List.of(sort), com.flying.orm.core.page.CursorPosition.of(cursor));
        Supplier<KeysetPageQuery> factory = () -> KeysetPageQuery.after(20, com.flying.orm.core.page.CursorPosition.of(cursor), sort);
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
                                       Supplier<KeysetPageQuery> factory) {
        KeysetPageQuery[] results = new KeysetPageQuery[ITERATIONS];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            results[iteration] = factory.get();
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        assertEquals(1_000, results[ITERATIONS - 1].position().values().size());
        return allocated;
    }
}
