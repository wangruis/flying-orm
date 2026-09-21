package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.DialectCapabilityId;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaSafeIncrementalClassificationTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final ColumnDefinition ID =
            ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
    private static final RelationalTableDefinition EXISTING_TABLE = RelationalTableDefinition.builder(ACCOUNTS)
            .addColumn(ID)
            .build();

    private final FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());
    private final SchemaMigrationPlanner planner = new SchemaMigrationPlanner(renderer);
    private final SchemaMigrationReviewer reviewer = SchemaMigrationReviewer.create(renderer);

    @Test
    void acceptsOnlyAConfirmedNullableAdditionForAnExistingTable() {
        ColumnDefinition note = ColumnDefinition.builder("note", "VARCHAR").length(80).build();
        SchemaOperation nullableAddition = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                ACCOUNTS,
                note.name(),
                null,
                note,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        List<SchemaOperation> operations = List.of(nullableAddition);
        SchemaSnapshot snapshot = SchemaSnapshot.present(EXISTING_TABLE);

        assertEquals(operations, planner.safeIncrementalPlan(
                operations, snapshot, DialectCapabilities.empty()));
        assertEquals(SchemaMigrationRiskLevel.LOW, reviewer.riskLevel(
                operations, snapshot, DialectCapabilities.empty()));

        ColumnDefinition required = ColumnDefinition.builder("required", "VARCHAR")
                .nullable(false)
                .build();
        SchemaOperation nonNullableAddition = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                ACCOUNTS,
                required.name(),
                null,
                required,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertThrows(IllegalArgumentException.class, () -> planner.safeIncrementalPlan(
                List.of(nonNullableAddition), snapshot, DialectCapabilities.empty()));
        assertEquals(SchemaMigrationRiskLevel.HIGH, reviewer.riskLevel(
                List.of(nonNullableAddition), snapshot, DialectCapabilities.empty()));
    }

    @Test
    void requiresConfirmedDialectSupportForGeneratedColumnsOnANewTable() {
        ColumnDefinition generatedId = ColumnDefinition.builder("id", "BIGINT")
                .nullable(false)
                .generation(ValueGeneration.identity())
                .build();
        RelationalTableDefinition generatedTable = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(generatedId)
                .build();
        SchemaOperation create = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                ACCOUNTS,
                "accounts",
                null,
                generatedTable,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        List<SchemaOperation> operations = List.of(create);
        SchemaSnapshot absent = SchemaSnapshot.absent(ACCOUNTS);

        assertThrows(IllegalArgumentException.class, () -> planner.safeIncrementalPlan(
                operations, absent, DialectCapabilities.empty()));
        DialectCapabilities identityColumns = DialectCapabilities.of(DialectCapabilityId.IDENTITY_COLUMNS);
        assertEquals(operations, planner.safeIncrementalPlan(operations, absent, identityColumns));
        assertEquals(SchemaMigrationRiskLevel.LOW, reviewer.riskLevel(
                operations, absent, identityColumns));
    }
}
