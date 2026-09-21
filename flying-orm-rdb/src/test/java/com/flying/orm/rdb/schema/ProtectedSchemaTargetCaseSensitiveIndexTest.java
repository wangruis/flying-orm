package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtectedSchemaTargetCaseSensitiveIndexTest {

    @Test
    void keepsCaseDistinctPhysicalIndexNamesInProtectedSchema() {
        DynamicForm form = DynamicForm.builder("protected-indexes", "CustomerData")
                                      .addField(DynamicField.of("first_value", "VARCHAR"))
                                      .addField(DynamicField.of("second_value", "VARCHAR"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                      .build();
        List<IndexMetadata> indexes = List.of(
                IndexMetadata.builder("CustomerLookup").addColumn("first_value").build(),
                IndexMetadata.builder("customerlookup").addColumn("second_value").build());

        ProtectedSchemaTarget target = ProtectedSchemaTarget.resolve(form, indexes, List.of());

        assertEquals(List.of("CustomerLookup", "customerlookup"),
                     target.indexes().stream().map(IndexMetadata::name).toList());
    }
}
