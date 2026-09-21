package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalMetadataImmutabilityTest {

    @Test
    void buildPublishesASnapshotThatDoesNotFollowTheBuilder() {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(
                RelationIdentity.table("orders"));
        builder.addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build());

        RelationalTableDefinition snapshot = builder.build();
        builder.addColumn(ColumnDefinition.builder("status", "VARCHAR").length(32).build());

        assertEquals(1, snapshot.columns().size());
        assertThrows(UnsupportedOperationException.class,
                     () -> snapshot.columns().add(ColumnDefinition.builder("extra", "INT").build()));
    }
}
