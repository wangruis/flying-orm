package com.flying.orm.core.form;

import com.flying.orm.core.metadata.RelationalMetadataAdapter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LegacyDynamicFormSchemaColdPathTest {

    @Test
    void legacyFormKeepsCanonicalSchemaObjectsOutOfItsDefaultState() {
        DynamicForm form = DynamicForm.builder("simple", "simple")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        String fingerprint = form.structureFingerprint();

        RelationalMetadataAdapter.from(form);

        assertEquals(fingerprint, form.structureFingerprint());
        assertFalse(Arrays.stream(DynamicForm.class.getDeclaredFields())
                          .map(Field::getType)
                          .anyMatch(type -> type.getName().contains("Relational")));
    }
}
