package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.rdb.result.DynamicRow;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class KeysetFoldedLabelRegressionTest {
    @Test void fullProjectionUsesLastPublishedRowWithFoldedLabel() {
        var layout = layout(List.of("id"), "id");
        var first = DynamicRow.copyOf(Map.of("ID", 1));
        var second = DynamicRow.copyOf(Map.of("ID", 2));
        var page = layout.finish(List.of(first, second), 1, UnaryOperator.identity());
        assertTrue(page.hasMore());
        assertEquals(List.of(1), page.nextPosition().values());
        assertSame(first, layout.visibleRow(first));
        assertFalse(first.containsKey("id"));
    }

    @Test void exactAndPresentNullLabelsKeepTheirValues() {
        var layout = layout(List.of("id"), "id");
        assertEquals(List.of(3), layout.nextPosition(DynamicRow.copyOf(Map.of("id", 3))).values());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", null);
        var position = layout.nextPosition(DynamicRow.copyOf(values));
        assertEquals(1, position.values().size());
        assertNull(position.values().getFirst());
    }

    @Test void widerRowsAndUnrelatedEntityKeyCollisionsRemainSupported() {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < 12; index++) values.put("EXTRA_" + index, index);
        values.put("A_B", 5);
        values.put("AB", 6);
        values.put("ID", 7);
        assertEquals(List.of(7), layout(List.of("id"), "id")
                .nextPosition(DynamicRow.copyOf(values)).values());
    }

    @Test void dynamicUnderscoreIdentityDoesNotUseEntityNameEquivalence() {
        var layout = layout(List.of("a_b", "ab"), "a_b");
        assertEquals(List.of(5), layout.nextPosition(DynamicRow.copyOf(Map.of("A_B", 5, "AB", 6))).values());
        assertThrows(IllegalArgumentException.class,
                () -> layout.nextPosition(DynamicRow.copyOf(Map.of("AB", 6))));
    }

    @Test void missingAndAmbiguousFoldedCursorLabelsAreNotGuessed() {
        var layout = layout(List.of("id"), "id");
        assertThrows(IllegalArgumentException.class,
                () -> layout.nextPosition(DynamicRow.copyOf(Map.of("other", 1))));
        assertThrows(IllegalArgumentException.class,
                () -> layout.nextPosition(DynamicRow.copyOf(Map.of("ID", 1, "Id", 2))));
    }

    @Test void hiddenProjectionKeepsExistingRenameAndStripContract() {
        var layout = layout(List.of(), "id");
        var label = layout.selections().getFirst().label();
        var driverRow = DynamicRow.copyOf(Map.of(label.toUpperCase(java.util.Locale.ROOT), 9));
        var logical = layout.logicalRowForDecoding(driverRow);
        var physical = layout.physicalRowAfterDecoding(logical);
        assertEquals(List.of(9), layout.nextPosition(physical).values());
        assertTrue(layout.visibleRow(physical).isEmpty());
    }

    @Test void emptyAndLastPagesDoNotInventAnotherPosition() {
        var layout = layout(List.of("id"), "id");
        assertFalse(layout.finish(List.of(), 1, UnaryOperator.identity()).hasMore());
        assertFalse(layout.finish(List.of(DynamicRow.copyOf(Map.of("ID", 1))),
                1, UnaryOperator.identity()).hasMore());
    }

    private static HiddenProjectionLayout layout(List<String> visible, String sort) {
        var form = DynamicForm.builder("t", "t").addField(DynamicField.primaryKey(sort, "INTEGER")).build();
        var page = KeysetPageNormalizer.normalize(form,
                KeysetPageQuery.first(1, KeysetSort.asc(sort, NullOrder.LAST)));
        return HiddenProjectionLayout.of(visible, page);
    }
}
