package com.flying.orm.core.form;

import com.flying.orm.core.protection.MaskedFieldDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class DynamicFormDatabaseTypeTest {

    @Test
    void doesNotTreatACustomTypeContainingTextAsTextualStorage() {
        DynamicForm.Builder form = DynamicForm.builder("documents", "documents")
                                              .addField(DynamicField.of("secret", "CONTEXT_ID"))
                                              .masked("secret", MaskedFieldDefinition.builder("default").build());

        assertThrows(IllegalArgumentException.class, form::build);
    }
}
