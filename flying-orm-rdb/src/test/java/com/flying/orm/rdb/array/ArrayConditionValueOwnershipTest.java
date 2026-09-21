package com.flying.orm.rdb.array;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArrayConditionValueOwnershipTest {

    @Test
    void callerValuesAreCanonicalizedOnceAndPublishedAsAnImmutableList() {
        StringBuilder mutable = new StringBuilder("first");
        List<Object> source = new ArrayList<>(List.of(mutable));

        ArrayConditionValue value = ArrayConditionValue.of(source, "TEXT[]");
        mutable.replace(0, mutable.length(), "changed");
        source.clear();

        assertEquals(List.of("first"), value.values());
        assertThrows(UnsupportedOperationException.class, () -> value.values().add("second"));
        assertArrayEquals(new String[]{"first"}, (String[]) value.parameter());
    }

    @Test
    void canonicalValuesPreserveUuidTemporalNumberAndNullCoercion() {
        UUID uuid = UUID.randomUUID();
        LocalDate date = LocalDate.of(2026, 8, 28);

        assertArrayEquals(new UUID[]{uuid}, (UUID[]) ArrayConditionValue
                .of(new String[]{uuid.toString()}, "UUID[]").parameter());
        assertArrayEquals(new LocalDate[]{date}, (LocalDate[]) ArrayConditionValue
                .of(List.of(date.toString()), "DATE[]").parameter());
        assertArrayEquals(new Integer[]{7}, (Integer[]) ArrayConditionValue
                .of(List.of("7"), "INTEGER[]").parameter());
        assertArrayEquals(new String[]{"first", null}, (String[]) ArrayConditionValue
                .of(Arrays.asList("first", null), "TEXT[]").parameter());
    }
}
