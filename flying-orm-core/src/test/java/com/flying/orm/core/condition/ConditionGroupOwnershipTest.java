package com.flying.orm.core.condition;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionGroupOwnershipTest {

    private static final int ITERATIONS = 200;

    private static final long MAX_TRUSTED_COMBINE_DEPTH_OVERHEAD = 768;

    @Test
    void repeatedMutableTypesStayIsolatedBetweenImmutableRuns() {
        Date first = new Date(11);
        Date second = new Date(22);
        byte[] firstBytes = {3};
        byte[] secondBytes = {4};
        ConditionGroup group = ConditionGroup.and().where("value", "in",
                List.of(1, 2, first, second, 5L, 6L, firstBytes, secondBytes, 7, 8)).build();
        first.setTime(111);
        second.setTime(222);
        firstBytes[0] = 33;
        secondBytes[0] = 44;
        List<?> values = (List<?>) ((TermCondition) group.children().getFirst()).value();
        assertEquals(List.of(1, 2, new Date(11), new Date(22), 5L, 6L), values.subList(0, 6));
        assertArrayEquals(new byte[]{3}, (byte[]) values.get(6));
        assertArrayEquals(new byte[]{4}, (byte[]) values.get(7));
        assertEquals(List.of(7, 8), values.subList(8, 10));
    }

    @Test
    void builderSnapshotsMutableScalarsDuringItsSingleNormalizationPass() {
        byte[] source = {1, 2, 3};
        ConditionGroup group = ConditionGroup.and()
                                             .where("payload", "in", List.of(source))
                                             .build();
        source[0] = 9;

        TermCondition term = (TermCondition) group.children().getFirst();
        byte[] published = (byte[]) ((List<?>) term.value()).getFirst();

        assertArrayEquals(new byte[]{1, 2, 3}, published);
    }

    @Test
    void standardAndCustomCollectionsKeepTheirOwnSnapshotPolicy() {
        TermRegistry terms = TermRegistry.builder()
                .add(TermHandler.simple("domain-values", ConditionValueShape.COLLECTION))
                .build();
        Date time = new Date(123);
        byte[] bytes = {1, 2};
        ConditionGroup group = ConditionGroup.and(terms)
                .where("values", " IN ", List.of(time, bytes))
                .where("values", "domain-values", List.of(time, bytes))
                .build();
        bytes[0] = 9;
        time.setTime(456);
        List<?> standard = (List<?>) ((TermCondition) group.children().getFirst()).value();
        List<?> custom = (List<?>) ((TermCondition) group.children().get(1)).value();

        assertNotSame(time, standard.getFirst());
        assertSame(time, custom.getFirst());
        assertNotSame(bytes, standard.get(1));
        assertNotSame(bytes, custom.get(1));
        assertEquals(new Date(123), standard.getFirst());
        assertArrayEquals(new byte[]{1, 2}, (byte[]) standard.get(1));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) custom.get(1));
    }

    @Test
    void ownedTermKeepsTheValueNormalizedAtTheBuilderBoundary() throws ReflectiveOperationException {
        FieldIdentity identity = FieldIdentity.of("sequence");
        ConditionValueNormalizer.Result normalized = ConditionValueNormalizer.normalize(
                ConditionValueShape.COLLECTION,
                List.of(1, 2, 3),
                ConditionValuePolicy.REJECT_EMPTY,
                (scalar, index) -> TermCondition.snapshotScalar(true, scalar),
                Integer.MAX_VALUE, Integer.MAX_VALUE);
        TermCondition term = TermCondition.owned(identity, "in", normalized.value());
        ConditionGroup group = ConditionGroup.and().add(term).build();
        Field rawValue = TermCondition.class.getDeclaredField("value");
        rawValue.setAccessible(true);

        assertSame(normalized.value(), rawValue.get(term));
        assertSame(term, group.children().getFirst());
    }

    @Test
    void trustedCombinationDoesNotWalkSubtrees() {
        java.lang.management.ThreadMXBean standardBean = ManagementFactory.getThreadMXBean();
        assertTrue(standardBean instanceof com.sun.management.ThreadMXBean,
                   "the Java 21 runtime must expose per-thread allocation counters");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        ConditionGroup shallow = ConditionGroup.and().where("sequence", "=", 1).build();
        ConditionGroup deep = shallow;
        for (int depth = 2; depth < 64; depth++) {
            deep = ConditionGroup.and().add(deep).build();
        }
        ConditionGroup empty = ConditionGroup.and().build();
        ConditionGroup safeDeep = deep;
        Supplier<ConditionGroup> shallowCombination = () -> ConditionGroups.and(shallow, empty);
        Supplier<ConditionGroup> deepCombination = () -> ConditionGroups.and(safeDeep, empty);
        for (int iteration = 0; iteration < 100; iteration++) {
            shallowCombination.get();
            deepCombination.get();
        }

        long shallowAllocation = allocatedBytes(allocationBean, shallowCombination);
        long deepAllocation = allocatedBytes(allocationBean, deepCombination);
        long depthOverhead = (deepAllocation - shallowAllocation) / ITERATIONS;

        assertTrue(depthOverhead < MAX_TRUSTED_COMBINE_DEPTH_OVERHEAD,
                   () -> "trusted condition combination walked the child subtree: shallow="
                           + shallowAllocation + ", deep=" + deepAllocation
                           + ", depth-overhead=" + depthOverhead);
    }

    @Test
    void trustedCombinationAllowsTreesBeyondTheFormerDepthBudget() {
        ConditionGroup group = ConditionGroup.and().where("sequence", "=", 1).build();
        for (int depth = 2; depth < 63; depth++) {
            group = ConditionGroup.and().add(group).build();
        }
        ConditionGroup depthSixtyFourOr = ConditionGroup.or().add(group).build();

        assertSame(depthSixtyFourOr,
                   ConditionGroups.and(depthSixtyFourOr, ConditionGroup.and().build()).children().getFirst());
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
}
