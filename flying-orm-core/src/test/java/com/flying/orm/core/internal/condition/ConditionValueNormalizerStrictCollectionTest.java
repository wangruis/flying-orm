package com.flying.orm.core.internal.condition;

import com.flying.orm.core.condition.ConditionValueException;
import com.flying.orm.core.condition.ConditionValueShape;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConditionValueNormalizerStrictCollectionTest {

    @Test
    void doesNotTrustAListSizeBeforeConsumingItsBoundedIterator() {
        List<Integer> misleadingSize = new AbstractList<>() {
            @Override
            public Integer get(int index) {
                return 42;
            }

            @Override
            public int size() {
                return Integer.MAX_VALUE;
            }

            @Override
            public Iterator<Integer> iterator() {
                return List.of(42).iterator();
            }
        };

        ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                ConditionValueShape.COLLECTION,
                misleadingSize,
                ConditionValuePolicy.REJECT_EMPTY);

        assertEquals(List.of(42), result.value());
    }

    @Test
    void rejectsAnEmptyElementInsteadOfSilentlyBroadeningAStrictCollection() {
        ConditionValueException error = assertThrows(
                ConditionValueException.class,
                () -> ConditionValueNormalizer.normalize(
                        ConditionValueShape.COLLECTION,
                        Arrays.asList(42, null),
                        ConditionValuePolicy.REJECT_EMPTY));

        assertEquals(ConditionValueException.Error.NULL_VALUE, error.error());
    }

    @Test
    void rejectsARangeAtTheThirdElementWithoutConsumingTheRemainingIterable() {
        AtomicInteger consumed = new AtomicInteger();
        Iterable<Integer> unbounded = () -> new Iterator<>() {
            private int next;

            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                consumed.incrementAndGet();
                return next++;
            }
        };

        ConditionValueException error = assertThrows(
                ConditionValueException.class,
                () -> ConditionValueNormalizer.normalize(
                        ConditionValueShape.RANGE,
                        unbounded,
                        ConditionValuePolicy.REJECT_EMPTY));

        assertEquals(ConditionValueException.Error.RANGE_SIZE_INVALID, error.error());
        assertEquals(3, consumed.get());
    }
}
