package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionOwnedConstructionTest {

    private static final int ALLOCATION_MEASURE_ITERATIONS = 400;

    private static final long MAX_SUPERLINEAR_BYTES_PER_COMPILATION = 80_000;

    @Test
    void multiValueConditionsSnapshotMutableElementsOnlyAtConstructionWhilePublicReadsStayDefensive() {
        AtomicInteger copies = new AtomicInteger();
        ConditionGroup where = ConditionGroup.and()
                                             .where("included_at", "in", List.of(
                                                     new CountingDate(1L, copies),
                                                     new CountingDate(2L, copies)))
                                             .where("created_at", "between", List.of(
                                                     new CountingDate(3L, copies),
                                                     new CountingDate(4L, copies)))
                                             .build();

        assertEquals(4, copies.get(),
                     "each mutable element should be owned once when the condition is built");

        copies.set(0);
        TermCondition included = (TermCondition) where.children().getFirst();
        @SuppressWarnings("unchecked")
        List<Date> firstPublicRead = (List<Date>) included.value();
        firstPublicRead.getFirst().setTime(99L);
        @SuppressWarnings("unchecked")
        List<Date> secondPublicRead = (List<Date>) included.value();

        assertEquals(List.of(new Date(1L), new Date(2L)), secondPublicRead);
        assertNotSame(firstPublicRead, secondPublicRead);
        assertNotSame(firstPublicRead.getFirst(), secondPublicRead.getFirst());
        assertEquals(4, copies.get(), "each public read must retain its defensive element snapshots");
    }

    @Test
    void keepsPublicTermValuesDefensive() {
        byte[] source = {1, 2, 3};
        TermCondition condition = TermCondition.of("payload", "custom-value", source);

        source[0] = 9;
        byte[] publicValue = (byte[]) condition.value();
        publicValue[1] = 8;

        byte[] secondPublicValue = (byte[]) condition.value();
        assertArrayEquals(new byte[]{1, 2, 3}, secondPublicValue);
        assertNotSame(publicValue, secondPublicValue);

    }

    @Test
    void rendererIsolatesTheOwnedTermValueAtTheCodecBoundary() {
        byte[] owned = {1, 2, 3};
        TermCondition term = TermCondition.owned(
                com.flying.orm.core.field.FieldIdentity.of("payload"), "=", owned);
        ConditionGroup where = ConditionGroup.and().add(term).build();

        SqlFragment rendered = SqlRenderer.builder().addDefaultTerms().build().renderWhere(where);

        assertNotSame(owned, rendered.parameters().getFirst());
        assertNotSame(owned, term.value());
    }

    @Test
    void termConditionDoesNotExposeItsOwnedMutableValueAsPublicApi() {
        assertFalse(Arrays.stream(TermCondition.class.getDeclaredMethods())
                          .anyMatch(method -> method.getName().startsWith("ownedValue")
                                  && Modifier.isPublic(method.getModifiers())));
    }

    @Test
    void compilerPublishesAStableCollectionThroughTheDefensiveAccessor() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("sequence", "INTEGER"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "sequence", "in", List.of(1, 2, 3));

        ConditionGroup compiled = StructuredConditionCompiler.create().compile(form, input);
        TermCondition term = (TermCondition) compiled.children().getFirst();
        Object firstPublicValue = term.value();
        Object secondPublicValue = term.value();

        assertNotSame(firstPublicValue, secondPublicValue);
        assertEquals(List.of(1, 2, 3), term.value());
    }

    @Test
    void compilerRejectsAnAstBeyondTheConfiguredDepthLimit() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("sequence", "BIGINT"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.and(
                        StructuredConditionInput.term("sequence", "eq", 7L)));

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        form,
                        input,
                        StructuredConditionPolicy.defaults().withMaxDepth(2)));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, error.code());
    }

    @Test
    void compilerRejectsAnAstBeyondTheConfiguredNodeLimit() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("sequence", "BIGINT"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("sequence", "eq", 7L),
                StructuredConditionInput.term("sequence", "gt", 3L));

        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> StructuredConditionCompiler.create().compile(
                        form,
                        input,
                        StructuredConditionPolicy.defaults().withMaxNodes(2)));

        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, error.code());
    }

    @Test
    void compilerAllocationGrowthShowsNestedSubtreesAreNotRevalidatedAtEveryLevel() {
        java.lang.management.ThreadMXBean standardBean = ManagementFactory.getThreadMXBean();
        assertTrue(standardBean instanceof com.sun.management.ThreadMXBean,
                   "the Java 21 runtime must expose per-thread allocation counters");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }

        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("sequence", "BIGINT"))
                                      .build();
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                    .withMaxDepth(64)
                                                                    .withMaxNodes(64);
        StructuredConditionInput depthSixteen = nestedInput(16);
        StructuredConditionInput depthSixtyFour = nestedInput(64);
        for (int iteration = 0; iteration < 100; iteration++) {
            compiler.compile(form, depthSixteen, policy);
            compiler.compile(form, depthSixtyFour, policy);
        }

        long shallowAllocation = allocatedBytes(
                allocationBean, compiler, form, depthSixteen, policy, ALLOCATION_MEASURE_ITERATIONS);
        long deepAllocation = allocatedBytes(
                allocationBean, compiler, form, depthSixtyFour, policy, ALLOCATION_MEASURE_ITERATIONS);
        // Depth 64 has four times as many nodes as depth 16. Remove that linear share so repeated
        // whole-subtree validation remains visible instead of being hidden by ordinary AST construction.
        long superlinearBytesPerCompilation = (deepAllocation - shallowAllocation * 4)
                / ALLOCATION_MEASURE_ITERATIONS;

        assertTrue(superlinearBytesPerCompilation < MAX_SUPERLINEAR_BYTES_PER_COMPILATION,
                   () -> "compiler allocation grew as if nested subtrees were revalidated: shallow="
                           + shallowAllocation + ", deep=" + deepAllocation
                           + ", superlinear-per-compile=" + superlinearBytesPerCompilation);
    }

    private static StructuredConditionInput nestedInput(int depth) {
        StructuredConditionInput input = StructuredConditionInput.term("sequence", "eq", 7L);
        for (int level = 1; level < depth; level++) {
            input = StructuredConditionInput.and(input);
        }
        return input;
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean allocationBean,
                                       StructuredConditionCompiler compiler,
                                       DynamicForm form,
                                       StructuredConditionInput input,
                                       StructuredConditionPolicy policy,
                                       int iterations) {
        ConditionGroup[] results = new ConditionGroup[iterations];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < iterations; iteration++) {
            results[iteration] = compiler.compile(form, input, policy);
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        assertEquals(1, results[iterations - 1].children().size());
        return allocated;
    }

    private static final class CountingDate extends Date {

        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }

}
