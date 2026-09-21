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
    void metadataBuildersPublishImmutableSnapshots() {
        IndexMetadata.Builder indexBuilder = IndexMetadata.builder("ix_customer")
                                                           .addColumn("tenant_id");
        IndexMetadata index = indexBuilder.build();
        indexBuilder.addColumn("customer_id");

        ForeignKeyMetadata.Builder foreignKeyBuilder = ForeignKeyMetadata.builder("fk_customer")
                                                                          .addColumn("tenant_id")
                                                                          .referenceTable("tenant")
                                                                          .addReferenceColumn("id");
        ForeignKeyMetadata foreignKey = foreignKeyBuilder.build();
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
