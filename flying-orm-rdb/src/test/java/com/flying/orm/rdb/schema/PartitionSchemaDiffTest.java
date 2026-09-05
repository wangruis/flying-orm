package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.TablePartitionDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionSchemaDiffTest {

    @Test
    void completeSnapshotRoundTripRetainsThePartitionFact() {
        RelationalTableDefinition table = table("occurred_at", true);

        SchemaSnapshot snapshot = SchemaSnapshot.present(table);

        assertEquals(SchemaSnapshot.State.PRESENT, snapshot.partition().state());
        assertEquals(TablePartitionDefinition.range("occurred_at"), snapshot.partition().value());
        assertTrue(snapshot.completeTable().isPresent());
        assertEquals(RelationalMetadataFingerprint.of(table),
                     RelationalMetadataFingerprint.of(snapshot.completeTable().orElseThrow()));
    }

    @Test
    void exactMatchHasNoDiffButAbsentPresentAndChangedKeysRequireManualVerification() {
        RelationalTableDefinition desired = table("occurred_at", true);

        assertTrue(diff(desired, SchemaSnapshot.present(desired)).operations().isEmpty());
        assertPartitionManual(diff(desired, SchemaSnapshot.present(table("occurred_at", false))));
        assertPartitionManual(diff(table("occurred_at", false), SchemaSnapshot.present(desired)));
        assertPartitionManual(diff(desired, SchemaSnapshot.present(table("created_at", true))));
    }

    @Test
    void unknownAndAbsentPartitionObservationsRemainDifferentAndBothFailClosedWhenNeeded() {
        RelationalTableDefinition desired = table("occurred_at", true);
        SchemaSnapshot unknown = snapshotWithUnknownPartition(desired);
        SchemaSnapshot absent = SchemaSnapshot.present(table("occurred_at", false));

        assertNotEquals(SchemaSnapshotFingerprint.of(unknown), SchemaSnapshotFingerprint.of(absent));
        assertPartitionManual(diff(desired, unknown));
    }

    private static SchemaCompatibilityReport diff(RelationalTableDefinition desired,
                                                  SchemaSnapshot actual) {
        RdbDialect dialect = RdbDialect.postgresql();
        return SchemaDiffer.diff(desired, actual, dialect.capabilities(),
                                 SchemaCompatibilityMode.EXACT, dialect.name(), dialect.schema());
    }

    private static void assertPartitionManual(SchemaCompatibilityReport report) {
        assertEquals(List.of(SchemaOperation.Kind.VERIFY_MANUALLY),
                     report.operations().stream().map(SchemaOperation::kind).toList());
        assertEquals(List.of("table-partition"),
                     report.operations().stream().map(SchemaOperation::objectName).toList());
    }

    private static SchemaSnapshot snapshotWithUnknownPartition(RelationalTableDefinition table) {
        SchemaSnapshot.Builder snapshot = SchemaSnapshot.builder(table.identity())
                .tablePresent()
                .tableCommentAbsent()
                .columns(table.columns())
                .primaryKey(table.primaryKey().orElseThrow())
                .uniqueConstraints(table.uniqueConstraints())
                .indexes(table.indexes())
                .foreignKeys(table.foreignKeys())
                .checks(table.checks());
        return snapshot.build();
    }

    private static RelationalTableDefinition table(String partitionColumn, boolean partitioned) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(
                        RelationIdentity.table("events"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP").nullable(false).build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMP").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of(
                        "pk_events", "id", "occurred_at", "created_at"));
        if (partitioned) {
            table.partition(TablePartitionDefinition.range(partitionColumn));
        }
        return table.build();
    }
}
