package com.flying.orm.core.param;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.TermRegistry;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterConditionOwnershipTest {

    private static final int ITERATIONS = 200;

    private static final long MAX_DEFAULT_OVERHEAD_PER_COMPILATION = 100_000;

    @Test
    void compilerReadsItsOwnedDefaultWithoutAnAccessorCopy() {
        java.lang.management.ThreadMXBean standardBean = ManagementFactory.getThreadMXBean();
        assertTrue(standardBean instanceof com.sun.management.ThreadMXBean,
                   "the Java 21 runtime must expose per-thread allocation counters");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        List<byte[]> values = IntStream.range(0, 1_000)
                                             .mapToObj(ignored -> new byte[256])
                                             .toList();
        ParameterConditionCompiler compiler = ParameterConditionCompiler.builder()
                                                                        .add(ParameterConditionSpec.builder(
                                                                                "sequence",
                                                                                "sequence",
                                                                                "in")
                                                                                                   .defaultValue(values)
                                                                                                   .build())
                                                                        .build();
        Supplier<ConditionGroup> directPath = () -> ConditionGroup.and()
                                                                   .where("sequence", "in", values)
                                                                   .build();
        Supplier<ConditionGroup> defaultPath = () -> compiler.compile(Map.of());
        for (int iteration = 0; iteration < 100; iteration++) {
            directPath.get();
            defaultPath.get();
        }

        long directAllocation = allocatedBytes(allocationBean, directPath);
        long defaultAllocation = allocatedBytes(allocationBean, defaultPath);
        long overheadPerCompilation = (defaultAllocation - directAllocation) / ITERATIONS;

        assertTrue(overheadPerCompilation < MAX_DEFAULT_OVERHEAD_PER_COMPILATION,
                   () -> "compiler copied its owned default through the public accessor: direct="
                           + directAllocation + ", default=" + defaultAllocation
                           + ", overhead-per-compilation=" + overheadPerCompilation);
    }

    @Test
    void packageRecordPublishesItsExclusiveFactoryListWithoutTraversingItAgain() {
        ParameterConditionSpec spec = ParameterConditionSpec.of("state", "state", "=");
        CountingList<ParameterConditionSpec> specs = new CountingList<>(List.of(spec));

        ParameterConditionPackage conditionPackage = new SimpleParameterConditionPackage(
                "filters", specs, TermRegistry.empty());

        assertEquals(0, specs.reads());
        assertThrows(UnsupportedOperationException.class, () -> conditionPackage.specs().clear());
        assertEquals(List.of(spec), conditionPackage.specs());
    }

    @Test
    void publicPackageFactoryStillSnapshotsMutableInputOnce() {
        ParameterConditionSpec spec = ParameterConditionSpec.of("state", "state", "=");
        List<ParameterConditionSpec> source = new ArrayList<>(List.of(spec));

        ParameterConditionPackage conditionPackage = ParameterConditionPackage.of("filters", source);
        source.clear();

        assertEquals(List.of(spec), conditionPackage.specs());
        assertThrows(UnsupportedOperationException.class, () -> conditionPackage.specs().clear());
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean allocationBean,
                                       Supplier<ConditionGroup> factory) {
        ConditionGroup[] results = new ConditionGroup[ITERATIONS];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            results[iteration] = factory.get();
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        assertEquals(1, results[ITERATIONS - 1].children().size());
        return allocated;
    }

    private static final class CountingList<E> extends AbstractList<E> {

        private final List<E> values;

        private int reads;

        private CountingList(List<E> values) {
            this.values = values;
        }

        @Override
        public E get(int index) {
            reads++;
            return values.get(index);
        }

        @Override
        public int size() {
            return values.size();
        }

        private int reads() {
            return reads;
        }
    }
}
