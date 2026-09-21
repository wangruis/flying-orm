package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSnapshotRoundTripContractTest {

    @Test
    void completeTableRoundTripsWithoutInventingUnknownFacts() {
        RelationalTableDefinition table = RelationalTableDefinition.builder(
                        RelationIdentity.of("catalog_a", "app", "accounts"))
                .comment("账户表")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("name", "VARCHAR").length(80).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();

        SchemaSnapshot snapshot = SchemaSnapshot.present(table);

        assertEquals(SchemaSnapshot.State.PRESENT, snapshot.tableState());
        assertEquals(SchemaSnapshot.State.PRESENT, snapshot.primaryKey().state());
        assertEquals(SchemaSnapshot.State.PRESENT, snapshot.tableComment().state());
        assertEquals("账户表", snapshot.tableComment().value());
        assertTrue(snapshot.completeTable().isPresent());
        assertEquals(RelationalMetadataFingerprint.of(table),
                     RelationalMetadataFingerprint.of(snapshot.completeTable().orElseThrow()));
        assertEquals(SchemaSnapshotFingerprint.of(snapshot),
                     SchemaSnapshotFingerprint.of(SchemaSnapshot.present(table)));
    }

    @Test
    void absentAndUnknownRemainDifferentFacts() {
        RelationIdentity identity = RelationIdentity.table("accounts");

        assertNotEquals(SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(identity)),
                        SchemaSnapshotFingerprint.of(SchemaSnapshot.unknown(identity)));
    }
}
