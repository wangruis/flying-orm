package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationIdentityTest {

    @Test
    void keepsCatalogSchemaAndTableAsSeparateSegments() {
        RelationIdentity identity = RelationIdentity.of("  tenant_catalog  ", " app ", "Orders");

        assertEquals("tenant_catalog", identity.catalog().orElseThrow());
        assertEquals("app", identity.schema().orElseThrow());
        assertEquals("Orders", identity.table());

        RelationIdentity literalTable = RelationIdentity.table("app.Orders");
        assertTrue(literalTable.catalog().isEmpty());
        assertTrue(literalTable.schema().isEmpty());
        assertEquals("app.Orders", literalTable.table());
    }
}
