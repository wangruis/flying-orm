package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TablePartitionDefinitionTest {

    @Test
    void rejectsAPartitionKeyThatIsNotATableColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> base().partition(TablePartitionDefinition.range("missing_column")).build());
    }

    @Test
    void partitionParticipatesInThePhysicalFingerprint() {
        RelationalTableDefinition unpartitioned = base().build();
        RelationalTableDefinition partitioned = base()
                .partition(TablePartitionDefinition.range("occurred_at"))
                .build();

        assertEquals(RelationalMetadataFingerprint.of(base().build()), RelationalMetadataFingerprint.of(unpartitioned));
        assertNotEquals(RelationalMetadataFingerprint.of(unpartitioned),
                        RelationalMetadataFingerprint.of(partitioned));
    }

    private static RelationalTableDefinition.Builder base() {
        return RelationalTableDefinition.builder(RelationIdentity.table("partition_baseline"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("occurred_at", "TIMESTAMP").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_partition_baseline", "id", "occurred_at"));
    }
}
