package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaOperationOrderingTest {
    private static final RdbDialect DIALECT = RdbDialect.postgresql();

    @Test
    void directChangesAndMetadataPlansShareColumnAndIndexOrdering() {
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "VARCHAR").withLength(32))
                .addField(DynamicField.of("obsolete", "INTEGER")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "VARCHAR").withLength(64))
                .addField(DynamicField.of("code", "VARCHAR").withLength(40).withUnique(true)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(DIALECT);
        List<String> direct = renderer.migrate(source.diffTo(target)).stream().map(request -> request.sql()).toList();
        SchemaMigrationPlan planned = renderer.migrateSafelyPlan(source.toTableDefinition(), target,
                target.toTableDefinition().indexes(), SchemaMigrationOptions.safe().allowDropColumn());
        SchemaMigrationPlan relational = RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("PostgreSQL", "16", DIALECT), target.toTableDefinition(),
                SchemaSnapshot.present(source.toTableDefinition()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertEquals(planned.sqlTexts(), direct);
        assertEquals(relational.requests().stream().map(request -> request.sql()).toList(), direct);
        assertEquals(4, direct.size());
        assertTrue(direct.getLast().startsWith("create unique index "));
    }

    @Test
    void replacementIndexCanUseAColumnAddedInTheSamePlan() {
        assertOrderedReplacement(false);
    }

    @Test
    void replacementIndexIsMovedBeforeItsOldColumnIsRemoved() {
        assertOrderedReplacement(true);
    }

    private static void assertOrderedReplacement(boolean removeOld) {
        ColumnDefinition oldColumn = ColumnDefinition.builder("old_value", "INTEGER").build();
        ColumnDefinition newColumn = ColumnDefinition.builder("new_value", "INTEGER").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(oldColumn).addIndex(index("old_value")).build();
        RelationalTableDefinition.Builder target = RelationalTableDefinition.builder("items");
        DynamicForm.Builder form = DynamicForm.builder("items", "items");
        if (!removeOld) {
            target.addColumn(oldColumn);
            form.addField(DynamicField.of("old_value", "INTEGER"));
        }
        target.addColumn(newColumn).addIndex(index("new_value"));
        form.addField(DynamicField.of("new_value", "INTEGER"));
        SchemaMigrationPlan relational = RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("PostgreSQL", "16", DIALECT), target.build(),
                SchemaSnapshot.present(current), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        SchemaMigrationPlan ordinary = FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(
                current, form.build(), List.of(index("new_value")), List.of(),
                SchemaMigrationOptions.safe().allowRebuildIndex().allowDropColumn());

        List<String> expected = new java.util.ArrayList<>(List.of(
                "alter table \"items\" add column \"new_value\" INTEGER",
                "drop index \"ix_value\"",
                "create index \"ix_value\" on \"items\" (\"new_value\" asc)"));
        if (removeOld) expected.add("alter table \"items\" drop column \"old_value\"");
        assertEquals(expected, relational.requests().stream().map(request -> request.sql()).toList());
        assertEquals(expected, ordinary.sqlTexts());
    }

    private static IndexDefinition index(String column) {
        return IndexDefinition.builder("ix_value").addColumn(column).build();
    }
}
