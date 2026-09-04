package com.flying.orm.rdb.result;

import com.flying.orm.rdb.mapping.RowMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowLayoutMappingConvergenceTest {

    @Test
    void normalizedMappingLayoutIsSharedByEveryRowInTheResult() {
        RowLayout layout = RowLayout.of(List.of("account.\"user_name\"", "age"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{"first", 20});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{"second", 21});

        assertSame(first.mappingKey(0), second.mappingKey(0));
        assertEquals("username", first.mappingKey(0));
        assertEquals(0, first.mappingIndexOf("username"));
        assertFalse(first.hasAmbiguousMappingColumns());
    }

    @Test
    void normalizedAmbiguityIsAnalyzedOnceWithoutChangingExactDynamicRowLookup() {
        RowLayout layout = RowLayout.of(List.of("user_name", "user-name"));
        DynamicRow row = DynamicRow.owned(layout, new Object[]{"first", "second"});

        assertTrue(row.hasAmbiguousMappingColumns());
        assertEquals("first", row.get("user_name"));
        assertEquals("second", row.get("user-name"));
    }

    @Test
    void indexedLayoutPreservesExactAndNormalizedLookupSemantics() {
        RowLayout layout = RowLayout.of(List.of(
                "id", "name", "age", "email", "phone", "city", "state", "zip_code", "zip-code"));

        assertEquals(7, layout.indexOf("zip_code"));
        assertEquals(8, layout.indexOf("zip-code"));
        assertEquals(7, layout.mappingIndexOf("zipcode"));
        assertTrue(layout.hasAmbiguousMappingColumns());
    }

    @Test
    void indexedLayoutStillReportsTheFirstDuplicateColumnIndex() {
        DuplicateColumnLabelException failure = assertThrows(
                DuplicateColumnLabelException.class,
                () -> RowLayout.of(List.of(
                        "id", "name", "age", "email", "phone", "city", "state", "zip", "name")));

        assertEquals("name", failure.columnLabel());
        assertEquals(1, failure.firstIndex());
        assertEquals(8, failure.duplicateIndex());
    }

    @Test
    void planBindingIsCreatedOnceAndLivesOnlyWithTheSharedResultLayout() {
        RowLayout layout = RowLayout.of(List.of("user_name"));
        DynamicRow first = DynamicRow.owned(layout, new Object[]{"first"});
        DynamicRow second = DynamicRow.owned(layout, new Object[]{"second"});
        Object plan = new Object();
        AtomicInteger bindings = new AtomicInteger();

        int firstBinding = first.mappingBinding(plan, bindings::incrementAndGet);
        int secondBinding = second.mappingBinding(plan, bindings::incrementAndGet);

        assertEquals(1, firstBinding);
        assertEquals(firstBinding, secondBinding);
        assertEquals(1, bindings.get());
    }

    @Test
    void aliasMapperReusesTheBoundTargetLayoutForEveryRowInTheResult() {
        RowLayout sourceLayout = RowLayout.of(List.of("account_name", "age"));
        DynamicRow first = DynamicRow.owned(sourceLayout, new Object[]{"first", 20});
        DynamicRow second = DynamicRow.owned(sourceLayout, new Object[]{"second", 21});
        RowMapper<DynamicRow> mapper = ((RowMapper<DynamicRow>) row -> row)
                .withAliases(Map.of("account_name", "name"));

        DynamicRow mappedFirst = mapper.map(first);
        DynamicRow mappedSecond = mapper.map(second);

        assertSame(mappedFirst.layout(), mappedSecond.layout());
        assertEquals("first", mappedFirst.get("name"));
        assertEquals("second", mappedSecond.get("name"));
    }

    @Test
    void publicRenameColumnsStillInvokesAStatefulRenamerForEveryCall() {
        RowLayout sourceLayout = RowLayout.of(List.of("name"));
        DynamicRow row = DynamicRow.owned(sourceLayout, new Object[]{"value"});
        AtomicInteger calls = new AtomicInteger();

        DynamicRow first = row.renameColumns(name -> name + calls.incrementAndGet());
        DynamicRow second = row.renameColumns(name -> name + calls.incrementAndGet());

        assertEquals("value", first.get("name1"));
        assertEquals("value", second.get("name2"));
        assertEquals(2, calls.get());
    }
}
