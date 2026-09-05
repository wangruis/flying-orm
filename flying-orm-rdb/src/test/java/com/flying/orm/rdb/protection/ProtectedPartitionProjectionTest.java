package com.flying.orm.rdb.protection;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedPartitionProjectionTest {

    @Test
    void keepsAnOrdinaryTemporalKeyWhileProjectingAnotherProtectedField() {
        DynamicForm form = form("secret");
        RelationalTableDefinition logical = table();

        RelationalTableDefinition physical = ProtectedRelationalSchemaProjector
                .project(form, logical).tables().getFirst();

        assertEquals(logical.partition(), physical.partition());
    }

    @Test
    void rejectsAProtectedPartitionKeyAtTheProjectorBoundary() {
        assertThrows(IllegalArgumentException.class, () ->
                ProtectedRelationalSchemaProjector.project(form("occurred_at"), table()));
    }

    private static DynamicForm form(String protectedField) {
        return DynamicForm.relationalBuilder(
                        "protected-partition", RelationIdentity.table("protected_partition"))
                .addField(DynamicField.of("occurred_at", "TIMESTAMP").withNullable(false))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted(protectedField, EncryptedFieldDefinition.builder().build())
                .build();
    }

    private static RelationalTableDefinition table() {
        return RelationalTableDefinition.builder(RelationIdentity.table("protected_partition"))
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP")
                        .nullable(false).build())
                .addColumn(ColumnDefinition.builder("secret", "VARCHAR").build())
                .partition(TablePartitionDefinition.range("occurred_at"))
                .build();
    }
}
