package com.flying.orm.core.form;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamicFormChangeSetTest {

    @Test
    void differencesAreComputedFromTheFormsAndPublishedReadOnly() {
        DynamicForm source = form("source", DynamicField.of("id", "BIGINT"));
        DynamicField added = DynamicField.of("name", "VARCHAR(64)");
        DynamicForm target = form("target", DynamicField.of("id", "BIGINT"), added);

        DynamicFormChangeSet actual = source.diffTo(target);
        assertEquals(List.of(added), actual.addedFields());
        assertThrows(UnsupportedOperationException.class, () -> actual.addedFields().clear());
        assertEquals(List.of(added), actual.addedFields());
    }

    @Test
    void additionsRemovalsAndChangesKeepTheirDefinitionsAndOrder() {
        DynamicField before = DynamicField.of("name", "VARCHAR").withLength(20);
        DynamicField after = before.withLength(100);
        DynamicField removed = DynamicField.of("obsolete", "INT");
        DynamicField first = DynamicField.of("first", "INT");
        DynamicField second = DynamicField.of("second", "INT");
        DynamicForm source = form("source", before, removed);
        DynamicForm target = form("target", first, after, second);
        DynamicFormChangeSet changes = source.diffTo(target);
        assertEquals(List.of(first, second), changes.addedFields());
        assertEquals(List.of(removed), changes.removedFields());
        assertEquals(List.of(new FieldChange(before, after)), changes.changedFields());
        assertEquals(source, changes.source());
        assertEquals(target, changes.target());
        assertThrows(UnsupportedOperationException.class, () -> changes.removedFields().clear());
        assertThrows(UnsupportedOperationException.class, () -> changes.changedFields().clear());
    }

    private static DynamicForm form(String id, DynamicField... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(id, "people");
        for (DynamicField field : fields) {
            builder.addField(field);
        }
        return builder.build();
    }
}
