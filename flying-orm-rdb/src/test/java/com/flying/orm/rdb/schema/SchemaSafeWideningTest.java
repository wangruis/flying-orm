package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSafeWideningTest {

    @Test
    void vectorDimensionGrowthRequiresExplicitColumnChangeApproval() {
        TableMetadata current = TableMetadata.builder("embeddings")
                .addColumn(ColumnMetadata.of("embedding", "VECTOR").withLength(3)).build();
        DynamicForm target = DynamicForm.builder("embeddings", "embeddings")
                .addField(DynamicField.of("embedding", "VECTOR").withLength(4)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql().schema());

        SchemaMigrationPlan safe = renderer.migrateSafelyPlan(current, target, List.of());
        assertFalse(safe.hasExecutableSql());
        assertTrue(safe.requiresManualReview());
        assertEquals(List.of(SkippedSchemaChange.Kind.CHANGE_COLUMN),
                safe.skippedChanges().stream().map(SkippedSchemaChange::kind).toList());

        SchemaMigrationPlan approved = renderer.migrateSafelyPlan(current, target, List.of(),
                SchemaMigrationOptions.safe().allowColumnChange());
        assertTrue(approved.hasExecutableSql());
        assertTrue(approved.skippedChanges().isEmpty());
    }

    @Test
    void unknownTypeParametersDoNotImplyStorageCapacity() {
        assertFalse(SchemaTypeComparison.safeWidening("custom_type(3)", "custom_type(4)"));
        assertFalse(SchemaTypeComparison.safeWidening("extensions.vector(3)", "extensions.vector(4)"));
    }

    @Test
    void knownLengthAndPrecisionGrowthRemainSafe() {
        for (String[] types : List.of(
                new String[]{"VARCHAR(32)", "VARCHAR(64)"},
                new String[]{"VARCHAR2(32 CHAR)", "VARCHAR2(64 CHAR)"},
                new String[]{"VARBINARY(32)", "VARBINARY(64)"},
                new String[]{"DECIMAL(10,2)", "DECIMAL(20,4)"},
                new String[]{"TIMESTAMP(3)", "TIMESTAMP(6)"})) {
            assertTrue(SchemaTypeComparison.safeWidening(types[0], types[1]), types[0]);
        }
    }
}
