package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCodecIsNotPhysicalFactTest {

    @Test
    void aDatabaseSnapshotDoesNotInventAColumnChangeForAnOrmOnlyCodec() {
        RelationIdentity identity = RelationIdentity.table("documents");
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("payload", "JSON").codecId("json-codec").build())
                .build();
        RelationalTableDefinition physical = RelationalTableDefinition.builder(identity)
                .addColumn(ColumnDefinition.builder("payload", "JSON").build())
                .build();

        SchemaCompatibilityReport report = SchemaDiffer.diff(
                desired,
                SchemaSnapshot.present(physical),
                DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT);

        assertTrue(report.operations().isEmpty());
    }
}
