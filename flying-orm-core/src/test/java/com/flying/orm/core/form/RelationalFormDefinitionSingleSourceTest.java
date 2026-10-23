package com.flying.orm.core.form;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RelationalFormDefinitionSingleSourceTest {

    @Test
    void fieldKeyDeclarationsAreEnoughForBothViews() {
        RelationalFormDefinition definition = RelationalFormDefinition.builder("orders", RelationIdentity.table("orders"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withUnique(true))
                .build();
        assertEquals(List.of("id"), definition.table().primaryKey().orElseThrow().columns());
        assertEquals(List.of("code"), definition.table().indexes().getFirst().columns());
        assertTrue(definition.table().indexes().getFirst().unique());
    }

    @Test
    void tableKeyDeclarationsProduceTheCrudFlagsAndNullability() {
        RelationalFormDefinition definition = RelationalFormDefinition.builder("orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR"))
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .unique(UniqueConstraintDefinition.of("uk_orders_code", "code"))
                .build();
        assertTrue(definition.form().field("id").primaryKey());
        assertFalse(definition.form().field("id").nullable());
        assertTrue(definition.form().field("code").unique());
        assertSame(definition.table().column("id"), definition.form().field("id").column());
    }

    @Test
    void explicitPrimaryKeyIsAuthoritativeAndBuilderReuseDoesNotDuplicateGeneratedIndexes() {
        RelationalFormDefinition.Builder builder = RelationalFormDefinition.builder("orders", RelationIdentity.table("orders"))
                .addField(DynamicField.primaryKey("old_id", "BIGINT"))
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withUnique(true))
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"));
        RelationalFormDefinition first = builder.build();
        RelationalFormDefinition second = builder.build();
        assertFalse(first.form().field("old_id").primaryKey());
        assertTrue(first.form().field("id").primaryKey());
        assertEquals(1, first.table().indexes().size());
        assertEquals(1, second.table().indexes().size());
    }

    @Test
    void completeColumnOptionsAreVisibleThroughTheCrudField() {
        ColumnDefinition column = ColumnDefinition.builder("code", "VARCHAR")
                .defaultValue(ColumnDefault.literal("new")).charset("utf8mb4").build();
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(new DynamicField(column, false, false))
                .build();
        assertEquals(ColumnDefault.literal("new"), definition.form().field("code").column().defaultValue());
        assertSame(column, definition.form().field("code").column());
    }

    @Test
    void oneBuilderPublishesAlignedCrudAndSchemaViews() {
        DynamicField id = DynamicField.primaryKey("id", "BIGINT");
        ColumnDefinition idColumn = id.column();
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.of(null, "sales", "orders"))
                .addField(id)
                .primaryKey(PrimaryKeyDefinition.of("pk_orders", "id"))
                .build();

        assertSame(id, definition.form().fields().getFirst());
        assertSame(idColumn, definition.table().columns().getFirst());
        assertEquals("sales.orders", definition.form().table());
        assertEquals("sales", definition.table().identity().schema().orElseThrow());
    }

    @Test
    void primaryKeyOrderIsIndependentFromFieldDeclarationOrder() {
        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "lines", RelationIdentity.table("lines"))
                .addField(DynamicField.primaryKey("line_no", "INT"))
                .addField(DynamicField.primaryKey("order_id", "BIGINT"))
                .primaryKey(PrimaryKeyDefinition.of("pk_lines", "order_id", "line_no"))
                .build();

        assertEquals(List.of("order_id", "line_no"),
                     definition.table().primaryKey().orElseThrow().columns());
    }

    @Test
    void uniqueIndexFactsAreDeclaredOnceAndSharedByBothViews() {
        DynamicField uniqueCode = DynamicField.of("code", "VARCHAR").withUnique(true);

        RelationalFormDefinition fromField = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(uniqueCode)
                .build();
        assertEquals(List.of("code"), fromField.table().indexes().getFirst().columns());
        RelationalFormDefinition fromIndex = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR"))
                .index(IndexDefinition.builder("ux_orders_code")
                               .unique()
                               .addKey(IndexKeyPart.asc("code"))
                               .build())
                .build();
        assertTrue(fromIndex.form().field("code").unique());
    }

    @Test
    void alignsSingleColumnUniqueConstraintsWithTheCrudFieldView() {
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of("uk_orders_code", "code");

        RelationalFormDefinition definition = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR").withUnique(true))
                .unique(unique)
                .build();
        assertEquals(List.of("code"), definition.table().uniqueConstraints().getFirst().columns());
        assertTrue(definition.table().indexes().isEmpty(), "named constraint must not create a duplicate index");

        RelationalFormDefinition fromConstraint = RelationalFormDefinition.builder(
                        "orders", RelationIdentity.table("orders"))
                .addField(DynamicField.of("code", "VARCHAR"))
                .unique(unique)
                .build();
        assertTrue(fromConstraint.form().field("code").unique());
    }
}
