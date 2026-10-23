package com.flying.orm.core.metadata;

import com.flying.orm.core.scope.FieldScope;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OwnedMetadataConstructionTest {

    @Test
    void absentTableObjectsRemainAbsentAfterBuilderAddsThem() {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder("people")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build());
        RelationalTableDefinition empty = builder.build();
        IndexDefinition index = IndexDefinition.builder("ix_id").addColumn("id").build();
        ForeignKeyDefinition key = ForeignKeyDefinition.builder("fk_parent").addColumn("id")
                .reference(RelationIdentity.table("parent")).addReferenceColumn("id").build();
        RelationalTableDefinition populated = builder.addIndex(index).addForeignKey(key).build();
        assertTrue(empty.indexes().isEmpty());
        assertTrue(empty.foreignKeys().isEmpty());
        assertTrue(empty.findIndex("ix_id").isEmpty());
        assertTrue(empty.findForeignKey("fk_parent").isEmpty());
        assertSame(index, populated.findIndex("IX_ID").orElseThrow());
        assertSame(key, populated.findForeignKey("FK_PARENT").orElseThrow());
        assertThrows(UnsupportedOperationException.class, () -> populated.indexes().clear());
        assertThrows(UnsupportedOperationException.class, () -> empty.foreignKeys().add(key));
    }

    @Test
    void metadataBuildersPublishImmutableSnapshots() {
        IndexDefinition.Builder indexBuilder = IndexDefinition.builder("ix_customer")
                                                           .addColumn("tenant_id");
        IndexDefinition index = indexBuilder.build();
        indexBuilder.addColumn("customer_id");

        ForeignKeyDefinition.Builder foreignKeyBuilder = ForeignKeyDefinition.builder("fk_customer")
                                                                          .addColumn("tenant_id")
                                                                          .reference(RelationIdentity.table("tenant"))
                                                                          .addReferenceColumn("id");
        ForeignKeyDefinition foreignKey = foreignKeyBuilder.build();
        foreignKeyBuilder.addColumn("customer_id").addReferenceColumn("id");

        assertEquals(java.util.List.of("tenant_id"), index.columns());
        assertEquals(java.util.List.of("tenant_id"), foreignKey.columns());
        assertEquals(java.util.List.of("id"), foreignKey.referenceColumns());
        assertThrows(UnsupportedOperationException.class, () -> index.columns().add("other"));
        assertThrows(UnsupportedOperationException.class, () -> foreignKey.columns().clear());
    }

    @Test
    void fieldScopeFactoriesPublishOwnedNormalizedFieldSets() {
        FieldScope readWrite = FieldScope.readWrite(" CustomerId ", "TENANT_ID");
        FieldScope readable = FieldScope.readable(" CustomerId ", "TENANT_ID");
        FieldScope writable = FieldScope.writable(" CustomerId ", "TENANT_ID");
        FieldScope unrestricted = FieldScope.unrestricted();

        assertSame(readWrite.readableFields(), readWrite.writableFields());
        assertEquals(java.util.Set.of("customerid", "tenant_id"), readWrite.readableFields());
        assertSame(unrestricted.readableFields(), unrestricted.writableFields());
        assertSame(unrestricted.readableFields(), writable.readableFields());
        assertSame(unrestricted.writableFields(), readable.writableFields());
        assertFalse(readable.unrestrictedRead());
        assertTrue(readable.unrestrictedWrite());
        assertTrue(writable.unrestrictedRead());
        assertFalse(writable.unrestrictedWrite());
        assertTrue(unrestricted.unrestrictedRead());
        assertTrue(unrestricted.unrestrictedWrite());
    }
}
