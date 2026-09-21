package com.flying.orm.rdb.codec;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ArrayValueCodecConvergenceTest {

    private static final int ELEMENT_COUNT = 4_096;

    private volatile Object arraySink;

    @Test
    void collectionWriteFillsTheTypedTargetArrayWithoutAnIntermediateElementList() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        List<Object> values = Collections.nCopies(ELEMENT_COUNT, "value");
        DatabaseType type = DatabaseType.of("CUSTOM[]");
        for (int iteration = 0; iteration < 100; iteration++) {
            arraySink = ArrayValueCodec.write(values, type);
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        arraySink = ArrayValueCodec.write(values, type);
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

        assertEquals(ELEMENT_COUNT, ((Object[]) arraySink).length);
        assertTrue(allocated < ELEMENT_COUNT * 6L,
                   "array write retained an intermediate element list: allocated=" + allocated);
    }

    @Test
    void dynamicReadFillsTheFinalListWithoutAnIntermediateTypedArray() throws Exception {
        Object[] source = Collections.nCopies(ELEMENT_COUNT, "value").toArray();
        Method readList = java.util.Arrays.stream(ArrayValueCodec.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals("readList")
                        && candidate.getParameterCount() == 3)
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "array codec must expose an internal final-list decoding seam"));
        readList.setAccessible(true);
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        for (int iteration = 0; iteration < 100; iteration++) {
            arraySink = readList.invoke(null, source, String.class, ValueCodecRegistry.standard());
        }

        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        arraySink = readList.invoke(null, source, String.class, ValueCodecRegistry.standard());
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;

        assertEquals(ELEMENT_COUNT, ((List<?>) arraySink).size());
        assertTrue(allocated < ELEMENT_COUNT * 10L,
                   "array read retained an intermediate typed array/list: allocated=" + allocated);
    }

    @Test
    void directFinalListReadKeepsDeclaredElementConversionAndNestedArrayGuard() {
        assertEquals(List.of(1, 2), ArrayValueCodec.readList(
                List.of("1", "2"), Integer.class, ValueCodecRegistry.standard()));
        assertThrows(IllegalArgumentException.class, () -> ArrayValueCodec.readList(
                List.of(List.of("nested")), String.class, ValueCodecRegistry.standard()));
    }
}
