package com.flying.orm.core.condition;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TermConditionListSnapshotTest {

    @Test
    void listSnapshotRetainsNullOrderAndSharedArraysButIsolatesMutableValues() {
        for (String operator : List.of("in", "not-in", "between", "not-between")) {
            byte[] bytes = {1};
            Date date = new Date(7);
            List<Object> source = new LinkedList<>(Arrays.asList(bytes, null, date, bytes));
            TermCondition term = TermCondition.of("value", operator, source);
            source.clear();
            bytes[0] = 9;
            date.setTime(99);

            List<?> first = (List<?>) term.value();
            assertEquals(4, first.size());
            assertArrayEquals(new byte[]{1}, (byte[]) first.getFirst());
            assertNull(first.get(1));
            assertEquals(new Date(7), first.get(2));
            assertSame(first.getFirst(), first.get(3));
            assertThrows(UnsupportedOperationException.class, first::clear);
            ((byte[]) first.getFirst())[0] = 8;
            ((Date) first.get(2)).setTime(88);
            List<?> second = (List<?>) term.value();
            assertArrayEquals(new byte[]{1}, (byte[]) second.getFirst());
            assertEquals(new Date(7), second.get(2));
        }
    }

    @Test
    void oneShotIterableIsStillConsumedOnlyOnce() {
        AtomicInteger iterations = new AtomicInteger();
        List<Integer> source = new ArrayList<>(List.of(1, 2, 3));
        Iterable<Integer> once = () -> {
            assertEquals(1, iterations.incrementAndGet());
            return source.iterator();
        };
        TermCondition term = TermCondition.of("id", "in", once);
        source.clear();
        assertEquals(List.of(1, 2, 3), term.value());
        assertEquals(List.of(1, 2, 3), term.value());
        assertEquals(1, iterations.get());
    }
}
