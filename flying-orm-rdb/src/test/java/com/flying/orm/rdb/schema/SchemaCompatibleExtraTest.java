package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCompatibleExtraTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");

    @Test
    void rollingModeAcceptsOnlyAControlledExtraColumnAndNonUniqueIndex() {
        RelationalTableDefinition desired = baseTable().build();
        RelationalTableDefinition actual = baseTable()
                .addColumn(ColumnDefinition.builder("legacy_note", "VARCHAR").length(80).build())
                .addIndex(IndexDefinition.builder("ix_legacy_note")
                                         .addKey(IndexKeyPart.asc("legacy_note"))
                                         .build())
                .build();

        SchemaCompatibilityReport exact = SchemaDiffer.diff(
                desired, SchemaSnapshot.present(actual), DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT);
        SchemaCompatibilityReport rolling = SchemaDiffer.diff(
                desired, SchemaSnapshot.present(actual), DialectCapabilities.empty(),
                SchemaCompatibilityMode.ROLLING_COMPATIBLE);

        assertFalse(exact.compatible());
        assertTrue(rolling.compatible());
        assertEquals(List.of(SchemaOperation.Kind.DROP_INDEX, SchemaOperation.Kind.DROP_COLUMN),
                     rolling.operations().stream().map(SchemaOperation::kind).toList());
        assertTrue(rolling.operations().stream().allMatch(
                operation -> operation.compatibility() == SchemaOperation.Compatibility.COMPATIBLE_EXTRA));
    }

    @Test
    void requiredOrConstraintBackedExtraCanRejectCurrentWrites() {
        RelationalTableDefinition requiredExtra = baseTable()
                .addColumn(ColumnDefinition.builder("tenant_code", "VARCHAR")
                                           .length(32)
                                           .nullable(false)
                                           .build())
                .build();
        RelationalTableDefinition constrainedExtra = baseTable()
                .addColumn(ColumnDefinition.builder("legacy_code", "VARCHAR").length(32).build())
                .addUnique(UniqueConstraintDefinition.of("uk_legacy_code", "legacy_code"))
                .build();

        assertFalse(rolling(baseTable().build(), requiredExtra).compatible());
        assertFalse(rolling(baseTable().build(), constrainedExtra).compatible());
    }

    @Test
    void safeIncrementalAcceptsAPlainNullableColumnButNotARequiredColumn() {
        RelationalTableDefinition actual = baseTable().build();
        RelationalTableDefinition nullableDesired = baseTable()
                .addColumn(ColumnDefinition.builder("nickname", "VARCHAR").length(80).build())
                .build();
        RelationalTableDefinition requiredDesired = baseTable()
                .addColumn(ColumnDefinition.builder("tenant_id", "BIGINT").nullable(false).build())
                .build();

        SchemaCompatibilityReport nullable = SchemaDiffer.diff(
                nullableDesired, SchemaSnapshot.present(actual), DialectCapabilities.empty(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);
        SchemaCompatibilityReport required = SchemaDiffer.diff(
                requiredDesired, SchemaSnapshot.present(actual), DialectCapabilities.empty(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertTrue(nullable.compatible());
        assertEquals(SchemaOperation.Compatibility.SAFE_INCREMENTAL,
                     nullable.operations().getFirst().compatibility());
        assertFalse(required.compatible());
    }

    private static SchemaCompatibilityReport rolling(RelationalTableDefinition desired,
                                                      RelationalTableDefinition actual) {
        return SchemaDiffer.diff(desired, SchemaSnapshot.present(actual), DialectCapabilities.empty(),
                                 SchemaCompatibilityMode.ROLLING_COMPATIBLE);
    }

    private static RelationalTableDefinition.Builder baseTable() {
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"));
    }
}
