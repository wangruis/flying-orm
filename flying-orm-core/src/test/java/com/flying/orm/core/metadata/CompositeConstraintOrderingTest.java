package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompositeConstraintOrderingTest {

    @Test
    void preservesDeclaredOrderForPrimaryIndexAndForeignKeyColumns() {
        PrimaryKeyDefinition primaryKey = PrimaryKeyDefinition.of("pk_line", "order_id", "line_no");
        IndexDefinition index = IndexDefinition.builder("ix_line")
                .addKey(IndexKeyPart.desc("order_id"))
                .addKey(IndexKeyPart.asc("line_no"))
                .build();
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_line_order")
                .addColumn("tenant_id")
                .addColumn("order_id")
                .reference(RelationIdentity.of(null, "sales", "orders"))
                .addReferenceColumn("tenant_id")
                .addReferenceColumn("id")
                .onDelete(ReferentialAction.CASCADE)
                .build();

        assertEquals(List.of("order_id", "line_no"), primaryKey.columns());
        assertEquals(List.of("order_id", "line_no"),
                     index.keys().stream().map(IndexKeyPart::column).toList());
        assertEquals(List.of("tenant_id", "order_id"), foreignKey.columns());
        assertEquals(List.of("tenant_id", "id"), foreignKey.referenceColumns());
    }

    @Test
    void rejectsConstraintReferencesThatChangeThePhysicalColumnCase() {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(RelationIdentity.table("users"))
                .addColumn(ColumnDefinition.builder("UserId", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_users", "userid"));

        assertThrows(IllegalArgumentException.class, table::build);
    }
}
