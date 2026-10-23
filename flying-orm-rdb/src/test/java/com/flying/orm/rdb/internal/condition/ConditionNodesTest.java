package com.flying.orm.rdb.internal.condition;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConditionNodesTest {
    private static volatile Object rewriteSink;

    @Test
    void flatTermVisitsDoNotAllocatePreorderTraversalContainers() {
        ConditionGroup root = ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 100L).build();
        assertFlatRewriteAllocation(() -> ConditionNodes.forEachTerm(root, term -> rewriteSink = term));
        assertSame(root.children().getLast(), rewriteSink);
    }

    @Test
    void flatTermSearchDoesNotAllocatePreorderTraversalContainers() {
        ConditionGroup root = ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 100L).build();
        assertFlatRewriteAllocation(() -> ConditionNodes.anyTerm(root, term -> {
            rewriteSink = term;
            return false;
        }));
        assertSame(root.children().getLast(), rewriteSink);
    }

    @Test
    void leafTraversalPreservesDeepBranchesSharedOccurrencesAndEmptyGroups() {
        TermCondition first = TermCondition.of("a", "=", 1);
        TermCondition second = TermCondition.of("b", "=", 2);
        ConditionGroup branch = ConditionGroup.or().add(first).add(ConditionGroup.and().build()).add(second).build();
        ConditionGroup deep = branch;
        for (int index = 0; index < 4_000; index++) deep = ConditionGroup.and().add(deep).build();
        ConditionGroup root = ConditionGroup.and().add(deep).add(branch).add(first).build();
        List<TermCondition> visited = new ArrayList<>();
        ConditionNodes.forEachTerm(root, visited::add);
        assertEquals(List.of(first, second, first, second, first), visited);
        visited.clear();
        assertTrue(ConditionNodes.anyTerm(root, term -> {
            visited.add(term);
            return visited.size() == 3;
        }));
        assertEquals(List.of(first, second, first), visited);
        visited.clear();
        ConditionNodes.forEachTerm(second, visited::add);
        assertEquals(List.of(second), visited);
        ConditionNodes.forEachTerm(ConditionGroup.or().build(), term -> {
            throw new AssertionError("empty group has no leaves");
        });
    }

    @Test
    void flatConditionRewriteDoesNotAllocateASeparateTraversalContainer() {
        ConditionGroup root = ConditionGroup.and().where("id", ">=", 1L).where("id", "<=", 100L).build();
        assertFlatRewriteAllocation(() -> rewriteSink = ConditionNodes.rewrite(root, term -> term));
        assertSame(root, rewriteSink);
    }

    @Test
    void flatStructuredRewriteDoesNotAllocateASeparateTraversalContainer() {
        StructuredConditionInput root = StructuredConditionInput.and(
                StructuredConditionInput.term("id", "gte", 1L),
                StructuredConditionInput.term("id", "lte", 100L));
        assertFlatRewriteAllocation(() -> rewriteSink = ConditionNodes.rewrite(root, term -> term));
        assertSame(root, rewriteSink);
    }

    private static void assertFlatRewriteAllocation(Runnable rewrite) {
        com.sun.management.ThreadMXBean allocation =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) rewrite.run();
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) rewrite.run();
        long bytes = allocation.getThreadAllocatedBytes(thread) - before;
        assertTrue(bytes < iterations * 64L, "bytes per rewrite: " + bytes / (double) iterations);
    }

    @Test
    void deepChangedConditionsRemainIterativeInBothRepresentations() {
        int depth = 4_000;
        ConditionGroup condition = ConditionGroup.and().where("id", "=", 1L).build();
        StructuredConditionInput input = StructuredConditionInput.term("id", "eq", 1L);
        for (int index = 0; index < depth; index++) {
            condition = ConditionGroup.or().add(condition).build();
            input = StructuredConditionInput.or(input);
        }
        AtomicInteger visits = new AtomicInteger();
        ConditionGroup changed = ConditionNodes.rewrite(condition, term -> {
            visits.incrementAndGet();
            return TermCondition.of(term.field(), term.operator(), 2L);
        });
        StructuredConditionInput changedInput = ConditionNodes.rewrite(input, term -> {
            visits.incrementAndGet();
            return StructuredConditionInput.term(term.field(), term.operator(), 2L);
        });
        assertEquals(2, visits.get());
        for (int index = 0; index < depth; index++) {
            assertEquals(com.flying.orm.core.condition.LogicalOperator.OR, changed.operator());
            assertEquals("or", changedInput.logic());
            changed = (ConditionGroup) changed.children().getFirst();
            changedInput = changedInput.terms().getFirst();
        }
        assertEquals(2L, ((TermCondition) changed.children().getFirst()).value());
        assertEquals(2L, changedInput.value());
    }

    @Test
    void visitsSharedSubtreesAtEveryOccurrenceInPreorder() {
        TermCondition first = TermCondition.of("a", "=", 1);
        TermCondition second = TermCondition.of("b", "=", 2);
        ConditionGroup shared = ConditionGroup.or().add(first).add(second).build();
        ConditionGroup root = ConditionGroup.and().add(shared).add(shared).build();
        List<ConditionNode> visited = new ArrayList<>();
        ConditionNodes.preorder(root).forEach(visited::add);

        assertEquals(List.of(root, shared, first, second, shared, first, second), visited);
        AtomicInteger rewrites = new AtomicInteger();
        assertSame(root, ConditionNodes.rewrite(root, term -> {
            rewrites.incrementAndGet();
            return term;
        }));
        assertEquals(4, rewrites.get());
    }

    @Test
    void predicatesAndFailuresStopBeforeLaterLeaves() {
        ConditionGroup root = ConditionGroup.and().where("a", "=", 1).where("b", "=", 2).build();
        AtomicInteger visits = new AtomicInteger();
        assertTrue(ConditionNodes.anyTerm(root, term -> visits.incrementAndGet() == 1));
        assertEquals(1, visits.get());

        IllegalArgumentException failure = new IllegalArgumentException("denied");
        assertSame(failure, assertThrows(IllegalArgumentException.class, () -> ConditionNodes.forEachTerm(root, term -> {
            visits.incrementAndGet();
            throw failure;
        })));
        assertEquals(2, visits.get());
    }

    @Test
    void rebuildsOnlyChangedAncestorsAndPreservesEmptyGroupsAndOperators() {
        ConditionGroup unchanged = ConditionGroup.and().where("a", "=", 1).build();
        ConditionGroup empty = ConditionGroup.or().build();
        ConditionGroup changed = ConditionGroup.or().where("b", "=", 2).add(empty).build();
        ConditionGroup root = ConditionGroup.and().add(unchanged).add(changed).build();
        ConditionGroup result = ConditionNodes.rewrite(root,
                term -> term.field().equals("b") ? TermCondition.of("b", "=", 3) : term);

        assertNotSame(root, result);
        assertEquals(root.operator(), result.operator());
        assertSame(unchanged, result.children().getFirst());
        ConditionGroup rewritten = (ConditionGroup) result.children().get(1);
        assertEquals(changed.operator(), rewritten.operator());
        assertEquals(3, ((TermCondition) rewritten.children().getFirst()).value());
        assertSame(empty, rewritten.children().get(1));
    }

    @Test
    void structuredTraversalAndRewriteVisitEveryOccurrenceInOrder() {
        StructuredConditionInput first = StructuredConditionInput.term("a", "eq", 1);
        StructuredConditionInput second = StructuredConditionInput.term("b", "eq", 2);
        StructuredConditionInput shared = StructuredConditionInput.or(first, second);
        StructuredConditionInput root = StructuredConditionInput.and(shared, shared);
        List<StructuredConditionInput> visited = new ArrayList<>();
        ConditionNodes.preorder(root).forEach(visited::add);
        assertEquals(List.of(root, shared, first, second, shared, first, second), visited);

        List<String> fields = new ArrayList<>();
        assertSame(root, ConditionNodes.rewrite(root, term -> {
            fields.add(term.field());
            return term;
        }));
        assertEquals(List.of("a", "b", "a", "b"), fields);
    }

    @Test
    void structuredNullChildrenDoNotPreemptEarlierTermErrors() {
        StructuredConditionInput first = StructuredConditionInput.term("a", "eq", 1);
        StructuredConditionInput root = StructuredConditionInput.and(first, null);
        Iterator<StructuredConditionInput> iterator = ConditionNodes.preorder(root).iterator();
        assertSame(root, iterator.next());
        assertSame(first, iterator.next());
        assertEquals("structured condition child must not be null",
                assertThrows(NullPointerException.class, iterator::next).getMessage());

        IllegalArgumentException failure = new IllegalArgumentException("first term denied");
        assertSame(failure, assertThrows(IllegalArgumentException.class,
                () -> ConditionNodes.rewrite(root, term -> { throw failure; })));
    }

    @Test
    void structuredRewriteKeepsUnchangedSiblingsAndMixedTermChildren() {
        StructuredConditionInput first = StructuredConditionInput.term("a", "eq", 1);
        StructuredConditionInput second = StructuredConditionInput.term("b", "eq", 2);
        StructuredConditionInput branch = StructuredConditionInput.or(second);
        StructuredConditionInput result = ConditionNodes.rewrite(StructuredConditionInput.and(first, branch),
                term -> term == second ? StructuredConditionInput.term("b", "eq", 3) : term);
        assertSame(first, result.terms().getFirst());
        assertNotSame(branch, result.terms().get(1));
        assertEquals("or", result.terms().get(1).logic());
        assertEquals(3, result.terms().get(1).terms().getFirst().value());

        StructuredConditionInput mixed = new StructuredConditionInput("a", "eq", 1, "and",
                java.util.Collections.singletonList(null));
        assertSame(mixed, ConditionNodes.rewrite(mixed, term -> term));
    }
}
