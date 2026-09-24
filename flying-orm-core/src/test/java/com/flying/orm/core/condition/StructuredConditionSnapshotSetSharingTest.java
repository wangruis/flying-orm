package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuredConditionSnapshotSetSharingTest {

    @Test
    void equalityOfSharedValueGraphsDoesNotExpandEveryPathToTheSameLeaf() {
        int depth = 16;
        AtomicInteger comparisons = new AtomicInteger();
        Object value = new CountingDate(7, comparisons);
        for (int level = 0; level < depth; level++) {
            value = List.of(value, value);
        }
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", "raw", Collections.singleton(value));
        Set<?> first = assertInstanceOf(Set.class, input.value());
        Set<?> second = assertInstanceOf(Set.class, input.value());
        comparisons.set(0);

        assertEquals(first, second);

        assertTrue(comparisons.get() <= depth + 1,
                () -> "comparing a shared graph repeated leaf equality " + comparisons.get() + " times");
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger comparisons;

        private CountingDate(long time, AtomicInteger comparisons) {
            super(time);
            this.comparisons = comparisons;
        }

        @Override
        public boolean equals(Object other) {
            comparisons.incrementAndGet();
            return super.equals(other);
        }
    }
}
