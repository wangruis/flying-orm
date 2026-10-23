package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaSafeIncrementalClassificationTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final ColumnDefinition ID = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
    private static final RelationalTableDefinition EXISTING = RelationalTableDefinition.builder(ACCOUNTS)
            .addColumn(ID).build();

    @Test
    void ordinaryAdditionAndRequiredAdditionKeepDistinctCompatibility() {
        for (boolean nullable : List.of(false, true)) {
            RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                    .addColumn(ID)
                    .addColumn(ColumnDefinition.builder("note", "VARCHAR").length(80).nullable(nullable).build())
                    .build();
            SchemaCompatibilityReport diff = SchemaDiffer.diff(desired, SchemaSnapshot.present(EXISTING),
                    DialectCapabilities.empty(), SchemaCompatibilityMode.SAFE_INCREMENTAL);
            assertEquals(1, diff.operations().size());
            assertEquals(SchemaOperation.Kind.ADD_COLUMN, diff.operations().getFirst().kind());
            assertEquals(nullable ? SchemaOperation.Compatibility.SAFE_INCREMENTAL
                    : SchemaOperation.Compatibility.REQUIRES_REVIEW, diff.operations().getFirst().compatibility());
        }
    }

    @Test
    void singleAndMultiTablePlanningUseTheSameGeneratedColumnCapabilities() {
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false)
                        .generation(ValueGeneration.identity()).build())
                .build();
        for (boolean supported : List.of(false, true)) {
            DialectCapabilities capabilities = supported
                    ? DialectCapabilities.of(DialectCapabilityId.IDENTITY_COLUMNS) : DialectCapabilities.empty();
            SchemaOperation.Compatibility expected = supported
                    ? SchemaOperation.Compatibility.SAFE_INCREMENTAL : SchemaOperation.Compatibility.REQUIRES_REVIEW;
            SchemaCompatibilityReport diff = SchemaDiffer.diff(desired, SchemaSnapshot.absent(ACCOUNTS),
                    capabilities, SchemaCompatibilityMode.SAFE_INCREMENTAL);
            MultiTableSchemaPlanner.Plan batch = new MultiTableSchemaPlanner(
                    DatabaseDescriptor.of("fixture", "1", "fixture", capabilities),
                    MultiTableSchemaPlanner.ForeignKeyCycleSupport.SUPPORTED)
                    .plan(RelationalSchemaDefinition.of(List.of(desired)));
            assertEquals(1, diff.operations().size());
            assertEquals(SchemaOperation.Kind.CREATE_TABLE, diff.operations().getFirst().kind());
            assertEquals(expected, diff.operations().getFirst().compatibility());
            assertEquals(1, batch.firstPhase().size());
            assertEquals(SchemaOperation.Kind.CREATE_TABLE, batch.firstPhase().getFirst().kind());
            assertEquals(expected, batch.firstPhase().getFirst().compatibility());
        }
    }
}
