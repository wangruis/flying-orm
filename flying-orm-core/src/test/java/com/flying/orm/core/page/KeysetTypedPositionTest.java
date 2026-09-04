package com.flying.orm.core.page;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeysetTypedPositionTest {

    @Test
    void keepsTypedNullableValuesAndSnapshotsMutableArrays() {
        byte[] token = {1, 2};
        Instant timestamp = Instant.parse("2026-09-03T00:00:00Z");
        CursorPosition position = CursorPosition.of(List.of(7L, timestamp, token));

        token[0] = 9;

        assertEquals(7L, position.values().get(0));
        assertEquals(timestamp, position.values().get(1));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) position.values().get(2));
        assertThrows(UnsupportedOperationException.class, () -> position.values().clear());
    }

    @Test
    void supportsNullPositionsWithoutUsingAnOffsetFallback() {
        List<Object> values = new ArrayList<>();
        values.add(null);
        values.add(42L);

        CursorPosition position = CursorPosition.of(values);
        KeysetPageQuery query = KeysetPageQuery.after(
                50,
                position,
                KeysetSort.asc("updated_at", NullOrder.LAST),
                KeysetSort.desc("id", NullOrder.FIRST));

        assertFalse(query.firstPage());
        assertEquals(java.util.Arrays.asList(null, 42L), query.position().values());
        assertEquals(CursorDirection.ASC, query.sorts().get(0).direction());
        assertEquals(NullOrder.LAST, query.sorts().get(0).nullOrder());
    }

    @Test
    void pageResultPublishesOnlyAnImmutableTypedNextPosition() {
        KeysetPageResult<String> result = new KeysetPageResult<>(
                List.of("row"), CursorPosition.of(List.of(9L)), true);

        assertTrue(result.hasMore());
        assertEquals(List.of(9L), result.nextPosition().values());
        assertThrows(UnsupportedOperationException.class, () -> result.rows().clear());
        assertThrows(IllegalArgumentException.class,
                     () -> new KeysetPageResult<>(List.of("row"), CursorPosition.first(), true));
    }
}
