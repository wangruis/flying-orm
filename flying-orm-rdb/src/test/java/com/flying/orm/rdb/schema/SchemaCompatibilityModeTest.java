package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCompatibilityModeTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final ColumnDefinition OPTIONAL_NOTE =
            ColumnDefinition.builder("note", "VARCHAR").length(80).build();

    @Test
    void incompatibleIsAResultAndNotAnExecutionMode() {
        assertArrayEquals(new SchemaCompatibilityMode[]{
                SchemaCompatibilityMode.EXACT,
                SchemaCompatibilityMode.ROLLING_COMPATIBLE,
                SchemaCompatibilityMode.SAFE_INCREMENTAL
        }, SchemaCompatibilityMode.values());
        assertArrayEquals(new SchemaCompatibilityStatus[]{
                SchemaCompatibilityStatus.COMPATIBLE,
                SchemaCompatibilityStatus.INCOMPATIBLE
        }, SchemaCompatibilityStatus.values());
    }

    @Test
    void eachModeAcceptsOnlyItsDeclaredCompatibilityBoundary() {
        SchemaOperation safeAddition = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                ACCOUNTS,
                OPTIONAL_NOTE.name(),
                null,
                OPTIONAL_NOTE,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        SchemaOperation compatibleExtra = SchemaOperation.of(
                SchemaOperation.Kind.DROP_COLUMN,
                ACCOUNTS,
                OPTIONAL_NOTE.name(),
                OPTIONAL_NOTE,
                null,
                SchemaOperation.Compatibility.COMPATIBLE_EXTRA);

        assertTrue(SchemaCompatibilityReport.of(SchemaCompatibilityMode.EXACT, List.of()).compatible());
        assertFalse(SchemaCompatibilityReport.of(
                SchemaCompatibilityMode.EXACT, List.of(compatibleExtra)).compatible());
        assertTrue(SchemaCompatibilityReport.of(
                SchemaCompatibilityMode.ROLLING_COMPATIBLE, List.of(compatibleExtra)).compatible());
        assertFalse(SchemaCompatibilityReport.of(
                SchemaCompatibilityMode.ROLLING_COMPATIBLE, List.of(safeAddition)).compatible());
        assertTrue(SchemaCompatibilityReport.of(
                SchemaCompatibilityMode.SAFE_INCREMENTAL,
                List.of(compatibleExtra, safeAddition)).compatible());
    }

    @Test
    void reportPublishesAnIndependentImmutableOperationList() {
        List<SchemaOperation> input = new java.util.ArrayList<>();
        SchemaCompatibilityReport report = SchemaCompatibilityReport.of(
                SchemaCompatibilityMode.SAFE_INCREMENTAL, input);
        input.add(SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                ACCOUNTS,
                OPTIONAL_NOTE.name(),
                null,
                OPTIONAL_NOTE,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL));

        assertEquals(SchemaCompatibilityStatus.COMPATIBLE, report.status());
        assertTrue(report.operations().isEmpty());
    }
}
