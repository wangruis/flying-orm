package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaColumnSelectionTest {

    @Test
    void combinedChangeRequiresExplicitSelectionWithoutEmittingPartialSql() {
        SchemaMigrationPlan plan = plan(
                DynamicField.of("name", "VARCHAR").withLength(16).withNullable(false),
                DynamicField.of("name", "VARCHAR").withLength(32).withNullable(true), SchemaMigrationOptions.safe());
        assertTrue(plan.requests().isEmpty());
        assertEquals(1, plan.skippedChanges().size());
    }

    @Test
    void explicitColumnChangeDoesNotSilentlyChangeGenerationWithNullability() {
        SchemaMigrationPlan plan = plan(
                DynamicField.of("id", "BIGINT").withNullable(false)
                        .withGeneration(com.flying.orm.core.metadata.ValueGeneration.identity()),
                DynamicField.of("id", "BIGINT").withNullable(true), SchemaMigrationOptions.safe().allowColumnChange());
        assertTrue(plan.requests().isEmpty());
        assertEquals(1, plan.skippedChanges().size());
    }

    @Test
    void appliesAProvenSafeWideningWithoutMixingItWithSkippedChanges() {
        SchemaMigrationPlan plan = plan(DynamicField.of("name", "VARCHAR").withLength(32),
                DynamicField.of("name", "VARCHAR").withLength(64), SchemaMigrationOptions.safe());
        assertEquals(1, plan.requests().size());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    private static SchemaMigrationPlan plan(DynamicField current, DynamicField target, SchemaMigrationOptions options) {
        return FormSchemaSqlRenderer.create(SchemaDialect.standard()).migrateSafelyPlan(
                DynamicForm.builder("device", "device").addField(current).build().toTableDefinition(),
                DynamicForm.builder("device", "device").addField(target).build(), List.of(), options);
    }
}
