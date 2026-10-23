package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableDefinitionProjectionTest {

    @Test
    void foreignKeyViewsKeepTheSameNamespaceActionsAndColumnPairing() {
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("FK_orders_tenant")
                .addColumn("tenant_id").addColumn("owner_id")
                .reference(RelationIdentity.of("catalog", "identity", "tenant.accounts"))
                .addReferenceColumn("tenant").addReferenceColumn("id")
                .onDelete(ReferentialAction.CASCADE).onUpdate(ReferentialAction.RESTRICT).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(ColumnDefinition.builder("tenant_id", "BIGINT").build())
                .addColumn(ColumnDefinition.builder("owner_id", "BIGINT").build())
                .addForeignKey(foreignKey).build();

        assertSame(foreignKey, table.foreignKey("fk_orders_tenant"));
        assertEquals(RelationIdentity.of("catalog", "identity", "tenant.accounts"),
                table.foreignKey("FK_orders_tenant").reference());
    }

    @Test
    void indexViewsShareTheCompleteDefinitionWithoutRebuilding() {
        IndexDefinition index = IndexDefinition.builder("IX_orders_code").unique()
                .addKey(IndexKeyPart.desc("code")).addKey(IndexKeyPart.asc("id")).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).build())
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).addIndex(index).build();

        assertSame(index, table.index("IX_orders_code"));
        assertSame(index, table.index("ix_orders_code"));
        assertEquals(IndexKeyPart.Direction.DESC, table.index("ix_orders_code").keys().getFirst().direction());
    }

    @Test
    void renamedColumnKeepsAllPhysicalAttributes() {
        ColumnDefinition column = ColumnDefinition.builder("before", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).charset("utf8").collation("utf8_bin").build();
        ColumnDefinition renamed = column.toBuilder(com.flying.orm.core.field.FieldIdentity.of("after")).build();
        assertEquals("after", renamed.name());
        assertEquals(column, renamed.toBuilder(column.identity()).build());
    }

    @Test
    void tableReadViewKeepsTheSameCompleteColumnDefinition() {
        ColumnDefinition column = ColumnDefinition.builder("status", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).charset("utf8").collation("utf8_bin").build();
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(new DynamicField(column, false, false)).build();
        assertSame(column, form.toTableDefinition().column("status"));
    }

    @Test
    void canonicalColumnsSurviveTheReadViewWithoutRebuilding() {
        ColumnDefinition column = ColumnDefinition.builder("status", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).charset("utf8").collation("utf8_bin").build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(column).build();
        assertSame(column, table.column("status"));
    }

    @Test
    void formPublishesItsCompleteColumnsWithoutALossyMetadataRoundTrip() {
        ColumnDefinition column = ColumnDefinition.builder("status", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("NEW")).charset("utf8").build();
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(new DynamicField(column, false, true)).build();

        RelationalTableDefinition definition = form.toTableDefinition();

        assertSame(column, definition.column("status"));
        assertEquals(ColumnDefault.literal("NEW"), definition.column("status").defaultValue());
        assertEquals("utf8", definition.column("status").charset());
        assertTrue(definition.indexes().getFirst().unique());
    }

    @Test
    void tableCommentIsFrozenWithExistingCommentNormalization() {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder("notes")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).comment("  用户 '说明'  ");
        RelationalTableDefinition table = builder.build();
        builder.comment("changed");
        assertEquals("用户 '说明'", table.comment());
        org.junit.jupiter.api.Assertions.assertNull(RelationalTableDefinition.builder("empty").build().comment());
    }

    @Test
    void relationalDynamicFormRetainsItsDeclaredPhysicalIdentityInTheCanonicalModel() {
        RelationIdentity identity = RelationIdentity.of("catalog", "tenant", "events");
        DynamicForm form = DynamicForm.relationalBuilder("events", identity)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();

        RelationalTableDefinition canonical = form.toTableDefinition();

        assertEquals(identity, canonical.identity());
        assertEquals("id", canonical.primaryKey().orElseThrow().columns().getFirst());
    }

    @Test
    void formProjectsPrimaryKeyAndUniqueIndexIntoTheSameTableDefinition() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withUnique(true))
                .build();

        RelationalTableDefinition canonical = form.toTableDefinition();
        assertEquals("orders", canonical.identity().table());
        assertEquals(2, canonical.columns().size());
        assertEquals("id", canonical.primaryKey().orElseThrow().columns().getFirst());
        assertTrue(canonical.indexes().stream().anyMatch(IndexDefinition::unique));
    }

    @Test
    void uniqueIndexIsNotMisreportedAsAUniqueConstraint() {
        RelationalTableDefinition canonical = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).build())
                .addIndex(IndexDefinition.builder("ux_orders_code")
                                  .unique()
                                  .addKey(IndexKeyPart.asc("code"))
                                  .build())
                .build();

        assertTrue(canonical.index("ux_orders_code").unique());
        assertTrue(canonical.uniqueConstraints().isEmpty());
    }
}
