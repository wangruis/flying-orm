package com.flying.orm.rdb.result;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicRowTest {

    @Test
    void exposesCompactValuesThroughMapAndIndexApis() {
        RowLayout layout = RowLayout.of(List.of("id", "name", "enabled"));
        DynamicRow row = DynamicRow.owned(layout, new Object[]{1L, null, true});

        assertEquals(3, row.size());
        assertEquals(3, row.columnCount());
        assertEquals(List.of("id", "name", "enabled"), new ArrayList<>(row.keySet()));
        assertEquals(1L, row.get("id"));
        assertNull(row.get("name"));
        assertTrue(row.containsKey("name"));
        assertFalse(row.containsKey("missing"));
        assertEquals("enabled", row.columnName(2));
        assertEquals(true, row.value(2));
        assertEquals(1L, row.get("id", Long.class));
        assertNull(row.get("name", String.class));
        assertEquals(Map.of("id", 1L, "enabled", true), row.toMap().entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Test
    void isReadOnlyAndChecksNamesTypesAndIndexes() {
        DynamicRow row = DynamicRow.owned(RowLayout.of(List.of("id", "name")),
                                          new Object[]{1L, "Alice"});

        assertThrows(UnsupportedOperationException.class, () -> row.put("id", 2L));
        assertThrows(UnsupportedOperationException.class, () -> row.remove("id"));
        assertThrows(UnsupportedOperationException.class, row::clear);
        assertThrows(IndexOutOfBoundsException.class, () -> row.value(2));
        assertThrows(ClassCastException.class, () -> row.get("id", String.class));
        assertNull(row.get("missing"));
    }

    @Test
    void rowsCanShareOneLayoutWithoutSharingValues() {
        RowLayout layout = RowLayout.of(List.of("id", "name"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{1L, "A"});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{2L, "B"});

        assertSame(first.layout(), second.layout());
        assertEquals(1L, first.get("id"));
        assertEquals(2L, second.get("id"));
    }

    @Test
    void rejectsDuplicateColumnLabelsBeforeRowsArePublished() {
        DuplicateColumnLabelException error = assertThrows(
                DuplicateColumnLabelException.class,
                () -> RowLayout.of(List.of("id", "name", "id")));

        assertEquals("id", error.columnLabel());
        assertEquals(0, error.firstIndex());
        assertEquals(2, error.duplicateIndex());
    }

    @Test
    void copiesAnExternalMapIntoAnIndependentCompactRow() {
        Map<String, Object> source = new java.util.LinkedHashMap<>();
        source.put("id", 7L);
        source.put("name", "before");

        DynamicRow row = DynamicRow.copyOf(source);
        source.put("name", "after");

        assertEquals(List.of("id", "name"), new ArrayList<>(row.keySet()));
        assertEquals(7L, row.value(0));
        assertEquals("before", row.get("name"));
    }

    @Test
    void replacesSeveralDecodedValuesWithOneNewValueArray() {
        Map<String, Object> source = new java.util.LinkedHashMap<>();
        source.put("json", "raw");
        source.put("count", 1);
        DynamicRow original = DynamicRow.copyOf(source);

        DynamicRow decoded = original.withValues(Map.of(0, Map.of("ok", true), 1, 2));

        assertEquals("raw", original.get("json"));
        assertEquals(Map.of("ok", true), decoded.get("json"));
        assertEquals(2, decoded.get("count"));
        assertThrows(IndexOutOfBoundsException.class, () -> original.withValues(Map.of(2, "bad")));
    }
}
