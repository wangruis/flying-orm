package com.flying.orm.core.condition;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class StructuredConditionTermPathAllocationTest {

    private static final int DEPTH = 1_024;
    private static final int NODE_COUNT = DEPTH * 2 + 1;
    // Include all ordinary traversal, field lookup, conversion and AST allocations. Materializing
    // complete paths for the term at every level alone exceeds this generous linear envelope.
    private static final long MAX_ALLOCATED_BYTES = NODE_COUNT * 4_096L;

    @Test
    void compilationDoesNotAllocateACompletePathForEverySuccessfulTerm() {
        com.sun.management.ThreadMXBean allocations = allocationCounter();
        DynamicForm form = DynamicForm.builder("samples", "samples")
                .addField(DynamicField.of("id", "INTEGER")).build();
        StructuredConditionCompiler compiler = StructuredConditionCompiler.create();
        StructuredConditionPolicy policy = policy();
        StructuredConditionInput warmup = termAtEveryLevel(32);
        for (int iteration = 0; iteration < 100; iteration++) {
            compiler.compile(form, warmup, policy);
        }
        StructuredConditionInput input = termAtEveryLevel(DEPTH);
        ConditionGroup[] result = new ConditionGroup[1];

        long allocated = allocatedBytes(allocations, () -> result[0] = compiler.compile(form, input, policy));

        ConditionNode node = result[0];
        for (int level = 0; level < DEPTH; level++) {
            ConditionGroup group = assertInstanceOf(ConditionGroup.class, node);
            assertEquals(2, group.children().size());
            assertEquals(7, assertInstanceOf(TermCondition.class, group.children().getFirst()).value());
            node = group.children().get(1);
        }
        assertEquals(7, assertInstanceOf(TermCondition.class, node).value());
        assertLinearAllocation(allocated, "compile");
    }

    @Test
    void structureValidationDoesNotAllocateACompletePathForEverySuccessfulTerm() {
        com.sun.management.ThreadMXBean allocations = allocationCounter();
        StructuredConditionPolicy policy = policy();
        StructuredConditionInput warmup = termAtEveryLevel(32);
        for (int iteration = 0; iteration < 100; iteration++) {
            StructuredConditionCompiler.validateStructure(warmup, policy);
        }
        StructuredConditionInput input = termAtEveryLevel(DEPTH);

        long allocated = allocatedBytes(allocations,
                () -> StructuredConditionCompiler.validateStructure(input, policy));

        assertLinearAllocation(allocated, "validateStructure");
    }

    private static StructuredConditionInput termAtEveryLevel(int depth) {
        StructuredConditionInput term = StructuredConditionInput.term("id", "eq", 7);
        StructuredConditionInput input = term;
        for (int level = 0; level < depth; level++) {
            input = StructuredConditionInput.and(term, input);
        }
        return input;
    }

    private static StructuredConditionPolicy policy() {
        return StructuredConditionPolicy.defaults().withMaxDepth(DEPTH + 1).withMaxNodes(NODE_COUNT);
    }

    private static com.sun.management.ThreadMXBean allocationCounter() {
        java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        assumeTrue(bean instanceof com.sun.management.ThreadMXBean,
                "per-thread allocation counters are unavailable");
        com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) bean;
        assumeTrue(allocations.isThreadAllocatedMemorySupported(),
                "per-thread allocation counters are unsupported");
        if (!allocations.isThreadAllocatedMemoryEnabled()) {
            allocations.setThreadAllocatedMemoryEnabled(true);
        }
        assumeTrue(allocations.isThreadAllocatedMemoryEnabled(),
                "per-thread allocation counters could not be enabled");
        return allocations;
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean allocations, Runnable action) {
        long threadId = Thread.currentThread().threadId();
        long before = allocations.getThreadAllocatedBytes(threadId);
        assumeTrue(before >= 0, "per-thread allocation counter is unavailable for the current thread");
        action.run();
        return allocations.getThreadAllocatedBytes(threadId) - before;
    }

    private static void assertLinearAllocation(long allocated, String operation) {
        assertTrue(allocated <= MAX_ALLOCATED_BYTES,
                () -> operation + " allocated " + allocated + " bytes for " + NODE_COUNT
                        + " nodes; linear allocation budget is " + MAX_ALLOCATED_BYTES);
    }
}
