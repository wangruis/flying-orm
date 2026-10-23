package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaForeignKeyPlanningTest {
    private static final DynamicForm FORM = DynamicForm.builder("items", "items")
            .addField(DynamicField.of("id", "BIGINT"))
            .addField(DynamicField.of("parent_id", "BIGINT")).build();

    @Test
    void addsDeclaredForeignKeyUsingTheSharedDdlRenderer() {
        SchemaMigrationPlan plan = plan(table(), FORM, List.of(key("parent_id", "parents")), SchemaMigrationOptions.safe());
        assertEquals(List.of("alter table \"items\" add constraint \"fk_parent\" foreign key (\"parent_id\")"
                + " references \"parents\" (\"id\")"), plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void retainsUndeclaredForeignKeysWithoutRequestingManualMigration() {
        SchemaMigrationPlan plan = plan(table(key("parent_id", "parents")), FORM, List.of(), SchemaMigrationOptions.safe());
        assertTrue(plan.sqlTexts().isEmpty());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void replacesForeignKeyBeforeRemovingItsOldColumn() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("owner_id", "BIGINT")).build();
        SchemaMigrationPlan plan = plan(table(key("parent_id", "parents")), target,
                List.of(key("owner_id", "owners")), SchemaMigrationOptions.safe().allowDropColumn());
        assertEquals(List.of(
                "alter table \"items\" drop constraint \"fk_parent\"",
                "alter table \"items\" add column \"owner_id\" BIGINT",
                "alter table \"items\" drop column \"parent_id\"",
                "alter table \"items\" add constraint \"fk_parent\" foreign key (\"owner_id\") references \"owners\" (\"id\")"),
                plan.sqlTexts());
        RelationalTableDefinition.Builder replacement = RelationalTableDefinition.builder("items");
        target.fields().forEach(field -> replacement.addColumn(field.column()));
        replacement.addForeignKey(key("owner_id", "owners"));
        RdbDialect dialect = RdbDialect.postgresql();
        SchemaMigrationPlan relational = RelationalSchemaPlanReviewer.create(dialect).review(
                com.flying.orm.rdb.dialect.DatabaseDescriptor.of("PostgreSQL", "test", dialect), replacement.build(),
                SchemaSnapshot.present(table(key("parent_id", "parents"))), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertEquals(plan.sqlTexts(), relational.sqlTexts());
    }

    @Test
    void unchangedColumnsDoNotSplitAnIndependentForeignKeyReplacement() {
        ForeignKeyDefinition desired = ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                .reference(RelationIdentity.table("parents")).addReferenceColumn("id")
                .onDelete(com.flying.orm.core.metadata.ReferentialAction.CASCADE).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(dialect).migrateSafelyPlan(
                    table(key("parent_id", "parents")), FORM, List.of(), List.of(desired), SchemaMigrationOptions.safe());
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_FOREIGN_KEY),
                    plan.operations().stream().map(SchemaOperation::kind).toList(), dialect.name());
        }
    }

    @Test
    void retainsCandidateIndexUsedByAnUndeclaredSelfReference() {
        RelationalTableDefinition.Builder current = RelationalTableDefinition.builder("items");
        FORM.fields().forEach(field -> current.addColumn(field.column()));
        current.addIndex(IndexDefinition.builder("uq_id").unique().addColumn("id").build())
                .addForeignKey(key("parent_id", "items"));
        SchemaMigrationPlan plan = plan(current.build(), FORM, List.of(), SchemaMigrationOptions.safe().allowDropIndex());
        assertTrue(plan.sqlTexts().isEmpty());
        assertEquals(List.of("uq_id"), plan.skippedChanges().stream().map(SkippedSchemaChange::name).toList());
    }

    @Test
    void replacedForeignKeyIsRemovedBeforeItsColumnIsRenamed() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("owner_id", "BIGINT")).build();
        SchemaMigrationPlan plan = plan(table(key("parent_id", "parents")), target,
                List.of(key("owner_id", "owners")),
                SchemaMigrationOptions.safe().renameColumn("parent_id", "owner_id"));
        assertEquals(List.of("alter table \"items\" drop constraint \"fk_parent\"",
                "alter table \"items\" rename column \"parent_id\" to \"owner_id\"",
                "alter table \"items\" add constraint \"fk_parent\" foreign key (\"owner_id\") references \"owners\" (\"id\")"),
                plan.sqlTexts());
    }

    @Test
    void retainedSelfReferenceStillPreventsCandidateIndexRemovalAfterColumnRename() {
        RelationalTableDefinition.Builder current = RelationalTableDefinition.builder("items");
        FORM.fields().forEach(field -> current.addColumn(field.column()));
        current.addIndex(IndexDefinition.builder("uq_id").unique().addColumn("id").build())
                .addForeignKey(key("parent_id", "items"));
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("new_id", "BIGINT"))
                .addField(DynamicField.of("parent_id", "BIGINT")).build();
        SchemaMigrationPlan plan = plan(current.build(), target, List.of(),
                SchemaMigrationOptions.safe().allowDropIndex().renameColumn("id", "new_id"));
        assertEquals(List.of("alter table \"items\" rename column \"id\" to \"new_id\""), plan.sqlTexts());
        assertEquals(List.of("uq_id"), plan.skippedChanges().stream().map(SkippedSchemaChange::name).toList());
    }

    @Test
    void selfReferenceFollowsExplicitColumnRenameWithoutBeingRebuilt() {
        ForeignKeyDefinition original = ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                .reference(RelationIdentity.table("items")).addReferenceColumn("id").build();
        ForeignKeyDefinition renamed = ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                .reference(RelationIdentity.table("items")).addReferenceColumn("new_id").build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("new_id", "BIGINT"))
                .addField(DynamicField.of("parent_id", "BIGINT")).build();
        SchemaMigrationPlan plan = plan(table(original), target, List.of(renamed),
                SchemaMigrationOptions.safe().renameColumn("id", "new_id"));
        assertEquals(List.of("alter table \"items\" rename column \"id\" to \"new_id\""), plan.sqlTexts());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    @Test
    void doesNotRemoveAColumnReferencedByAnUndeclaredRetainedForeignKey() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        SchemaMigrationPlan plan = plan(table(key("parent_id", "parents")), target, List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());
        assertTrue(plan.sqlTexts().isEmpty());
        assertEquals(List.of("parent_id"), plan.skippedChanges().stream().map(SkippedSchemaChange::name).toList());
    }

    private static ForeignKeyDefinition key(String column, String reference) {
        return ForeignKeyDefinition.builder("fk_parent").addColumn(column)
                .reference(RelationIdentity.table(reference)).addReferenceColumn("id").build();
    }

    private static RelationalTableDefinition table(ForeignKeyDefinition... keys) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder("items");
        FORM.fields().forEach(field -> table.addColumn(field.column()));
        for (ForeignKeyDefinition key : keys) table.addForeignKey(key);
        return table.build();
    }

    private static SchemaMigrationPlan plan(RelationalTableDefinition current, DynamicForm target,
                                             List<ForeignKeyDefinition> keys, SchemaMigrationOptions options) {
        return FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .migrateSafelyPlan(current, target, List.of(), keys, options);
    }
}
