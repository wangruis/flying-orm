package com.flying.orm.core.form;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalFormDefinitionSingleSourceTest {

    @Test
    void oneBuilderPublishesAlignedCrudAndSchemaViews() {
        DynamicField id = DynamicField.primaryKey("id", "BIGINT");
        ColumnDefinition idColumn = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.of(null, "sales", "orders"))
                .addField(id, idColumn)
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .build();

        assertSame(id, definition.form().fields().getFirst());
        assertSame(idColumn, definition.table().columns().getFirst());
        assertEquals("orders", definition.form().table());
        assertEquals("sales", definition.table().identity().schema().orElseThrow());
    }

    @Test
    void rejectsMismatchedFieldAndColumnAtTheSingleBoundary() {
        assertThrows(IllegalArgumentException.class, () -> RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR"),
                          ColumnDefinition.builder("other", "VARCHAR").build()));
    }

    @Test
    void primaryKeyOrderIsIndependentFromFieldDeclarationOrder() {
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "lines", RelationIdentity.table("lines"))
                .addField(DynamicField.primaryKey("line_no", "INT"),
                          ColumnDefinition.builder("line_no", "INT").nullable(false).build())
                .addField(DynamicField.primaryKey("order_id", "BIGINT"),
                          ColumnDefinition.builder("order_id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_lines", "order_id", "line_no"))
                .build();

        assertEquals(List.of("order_id", "line_no"),
                     definition.table().primaryKey().orElseThrow().columns());
    }

    @Test
    void rejectsUniqueIndexFactsThatDriftBetweenTheTwoViews() {
        DynamicField uniqueCode = DynamicField.of("code", "VARCHAR").withUnique(true);
        ColumnDefinition codeColumn = ColumnDefinition.builder("code", "VARCHAR").build();

        assertThrows(IllegalArgumentException.class, () -> RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(uniqueCode, codeColumn)
                .build());
        assertThrows(IllegalArgumentException.class, () -> RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR"), codeColumn)
                .index(IndexDefinition.builder("ux_orders_code")
                               .unique()
                               .addKey(IndexKeyPart.asc("code"))
                               .build())
                .build());
    }

    @Test
    void alignsSingleColumnUniqueConstraintsWithTheCrudFieldView() {
        ColumnDefinition codeColumn = ColumnDefinition.builder("code", "VARCHAR").build();
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of("uk_orders_code", "code");

        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR").withUnique(true), codeColumn)
                .unique(unique)
                .build();
        assertEquals(List.of("code"), definition.table().uniqueConstraints().getFirst().columns());

        assertThrows(IllegalArgumentException.class, () -> RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR"), codeColumn)
                .unique(unique)
                .build());
    }
}
