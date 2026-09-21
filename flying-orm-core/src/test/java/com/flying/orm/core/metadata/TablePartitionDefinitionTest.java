package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TablePartitionDefinitionTest {

    private static final String UNPARTITIONED_V1 =
            "87a74d8f177c0026d56496759f699ef4e5558861712c68b489bf59ac0b9c0c07";

    @Test
    void rejectsAPartitionKeyThatIsNotATableColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> base().partition(TablePartitionDefinition.range("missing_column")).build());
    }

    @Test
    void partitionChangesTheFingerprintWithoutChangingTheUnpartitionedV1Fingerprint() {
        RelationalTableDefinition unpartitioned = base().build();
        RelationalTableDefinition partitioned = base()
                .partition(TablePartitionDefinition.range("occurred_at"))
                .build();

        assertEquals(UNPARTITIONED_V1, RelationalMetadataFingerprint.of(unpartitioned));
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
