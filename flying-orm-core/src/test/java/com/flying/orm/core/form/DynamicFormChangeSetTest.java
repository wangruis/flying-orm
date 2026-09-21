package com.flying.orm.core.form;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamicFormChangeSetTest {

    @Test
    void publicConstructorStillRejectsAChangeSetThatDoesNotMatchItsForms() {
        DynamicForm source = form("source", DynamicField.of("id", "BIGINT"));
        DynamicField added = DynamicField.of("name", "VARCHAR(64)");
        DynamicForm target = form("target", DynamicField.of("id", "BIGINT"), added);

        assertThrows(IllegalArgumentException.class,
                     () -> new DynamicFormChangeSet(source, target, List.of(), List.of(), List.of()));

        DynamicFormChangeSet actual = source.diffTo(target);
        assertEquals(List.of(added), actual.addedFields());
    }

    private static DynamicForm form(String id, DynamicField... fields) {
        DynamicForm.Builder builder = DynamicForm.builder(id, "people");
        for (DynamicField field : fields) {
            builder.addField(field);
        }
        return builder.build();
    }
}
