package com.flying.orm.core.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalMetadataCompatibilityViewTest {

    @Test
    void relationalDynamicFormRetainsItsDeclaredPhysicalIdentityInTheCanonicalModel() {
        RelationIdentity identity = RelationIdentity.of("catalog", "tenant", "events");
        DynamicForm form = DynamicForm.relationalBuilder("events", identity)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();

        RelationalTableDefinition canonical = RelationalMetadataAdapter.from(form);

        assertEquals(identity, canonical.identity());
        assertEquals("id", canonical.primaryKey().orElseThrow().columns().getFirst());
    }

    @Test
    void legacyDynamicFormCanEnterAndLeaveTheCanonicalColdPath() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withUnique(true))
                .build();

        RelationalTableDefinition canonical = RelationalMetadataAdapter.from(form);
        TableMetadata compatibility = RelationalMetadataAdapter.toTableMetadata(canonical);

        assertEquals("orders", canonical.identity().table());
        assertEquals(2, compatibility.columns().size());
        assertEquals("id", compatibility.primaryKeyColumns().getFirst().name());
        assertTrue(compatibility.indexes().stream().anyMatch(IndexMetadata::unique));
    }

    @Test
    void canonicalUniqueIndexKeepsItsUniquenessInTheLegacyView() {
        RelationalTableDefinition canonical = RelationalTableDefinition.builder(RelationIdentity.table("orders"))
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).build())
                .addIndex(IndexDefinition.builder("ux_orders_code")
                                  .unique()
                                  .addKey(IndexKeyPart.asc("code"))
                                  .build())
                .build();

        TableMetadata compatibility = RelationalMetadataAdapter.toTableMetadata(canonical);
        RelationalTableDefinition roundTrip = RelationalMetadataAdapter.from(compatibility);

        assertTrue(compatibility.index("ux_orders_code").unique());
        assertTrue(roundTrip.uniqueConstraints().isEmpty());
        assertTrue(roundTrip.indexes().getFirst().unique());
    }
}
