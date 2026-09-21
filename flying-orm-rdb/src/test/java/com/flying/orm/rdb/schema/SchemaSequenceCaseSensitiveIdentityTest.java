package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSequenceCaseSensitiveIdentityTest {

    private final SchemaTableSqlRenderer renderer =
            new SchemaTableSqlRenderer(RdbDialect.postgresql().schema());

    @Test
    void createsCaseDistinctPhysicalSequencesSeparately() {
        List<SqlRequest> requests = renderer.createTable(form());

        assertEquals(3, requests.size());
        assertTrue(requests.stream().anyMatch(request -> request.sql().contains("\"OrderSeq\"")));
        assertTrue(requests.stream().anyMatch(request -> request.sql().contains("\"orderseq\"")));
    }

    @Test
    void dropsCaseDistinctPhysicalSequencesSeparately() {
        List<SqlRequest> requests = renderer.dropSequences(form().fields(), List.of());

        assertEquals(List.of("drop sequence \"OrderSeq\"", "drop sequence \"orderseq\""),
                     requests.stream().map(SqlRequest::sql).toList());
    }

    @Test
    void createsOneSharedSequenceWhenAddingMultipleColumns() {
        DynamicForm source = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .build();
        DynamicField first = DynamicField.of("first_value", "BIGINT")
                                         .generatedBySequence("shared_seq");
        DynamicField second = DynamicField.of("second_value", "BIGINT")
                                          .generatedBySequence("shared_seq");
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(first)
                                        .addField(second)
                                        .build();
        DynamicFormChangeSet changes = new DynamicFormChangeSet(
                source, target, List.of(first, second), List.of(), List.of());
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        TableMetadata current = TableMetadata.builder("events")
                                             .addColumn(ColumnMetadata.of("id", "BIGINT"))
                                             .build();

        assertEquals(1, sequenceCreates(formRenderer.migrate(changes)));
        assertEquals(1, sequenceCreates(formRenderer.migrateSafelyPlan(current, target, List.of()).requests()));
    }

    @Test
    void reviewedRollbackDropsASequenceCreatedForAnAddedColumn() {
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(DynamicField.of("generated_value", "BIGINT")
                                                              .generatedBySequence("added_seq"))
                                        .build();
        TableMetadata current = TableMetadata.builder("events")
                                             .addColumn(ColumnMetadata.of("id", "BIGINT"))
                                             .build();
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationPlan migration = formRenderer.migrateSafelyPlan(current, target, List.of());

        List<String> rollback = SchemaMigrationReviewer.create(formRenderer)
                                                       .review(current,
                                                               migration,
                                                               SchemaMigrationReviewPolicy.allowBlocking())
                                                       .rollback()
                                                       .requests()
                                                       .stream()
                                                       .map(SqlRequest::sql)
                                                       .toList();

        assertEquals(List.of("alter table \"events\" drop column \"generated_value\"",
                             "drop sequence \"added_seq\""),
                     rollback);
    }

    @Test
    void doesNotCreateASequenceThatAnExistingColumnAlreadyUses() {
        DynamicField existing = DynamicField.of("existing_value", "BIGINT")
                                            .generatedBySequence("shared_seq");
        DynamicField added = DynamicField.of("added_value", "BIGINT")
                                         .generatedBySequence("shared_seq");
        DynamicForm source = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(existing)
                                        .build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(existing)
                                        .addField(added)
                                        .build();
        DynamicFormChangeSet changes = new DynamicFormChangeSet(
                source, target, List.of(added), List.of(), List.of());
        TableMetadata current = TableMetadata.builder("events")
                                             .addColumn(ColumnMetadata.of("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("existing_value", "BIGINT"))
                                             .build();
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        assertEquals(0, sequenceCreates(formRenderer.migrate(changes)));
        assertEquals(0, sequenceCreates(formRenderer.migrateSafelyPlan(current, target, List.of()).requests()));
    }

    @Test
    void directMigrationRejectsConflictingDefinitionsForAnExistingSequence() {
        DynamicField existing = DynamicField.of("old_value", "BIGINT")
                                            .withGeneration(ValueGeneration.sequence("shared_seq", 1, 1, 100));
        DynamicField added = DynamicField.of("new_value", "BIGINT")
                                         .withGeneration(ValueGeneration.sequence("shared_seq", 10, 2, 50));
        DynamicForm source = DynamicForm.builder("events", "events").addField(existing).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(existing)
                                        .addField(added)
                                        .build();
        DynamicFormChangeSet changes = new DynamicFormChangeSet(
                source, target, List.of(added), List.of(), List.of());

        assertThrows(IllegalArgumentException.class,
                     () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).migrate(changes));
    }

    @Test
    void reviewedRollbackKeepsASequenceUsedByAnExistingColumn() {
        DynamicField existing = DynamicField.of("existing_value", "BIGINT")
                                            .generatedBySequence("shared_seq");
        DynamicField added = DynamicField.of("added_value", "BIGINT")
                                         .generatedBySequence("shared_seq");
        DynamicForm target = DynamicForm.builder("events", "events")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .addField(existing)
                                        .addField(added)
                                        .build();
        TableMetadata current = TableMetadata.builder("events")
                                             .addColumn(ColumnMetadata.of("id", "BIGINT"))
                                             .addColumn(ColumnMetadata.of("existing_value", "BIGINT"))
                                             .build();
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        SchemaMigrationPlan migration = formRenderer.migrateSafelyPlan(current, target, List.of());

        List<String> rollback = SchemaMigrationReviewer.create(formRenderer)
                                                       .review(current,
                                                               migration,
                                                               SchemaMigrationReviewPolicy.allowBlocking())
                                                       .rollback()
                                                       .requests()
                                                       .stream()
                                                       .map(SqlRequest::sql)
                                                       .toList();

        assertEquals(List.of("alter table \"events\" drop column \"added_value\""), rollback);
    }

    @Test
    void reusesSequenceOwnedByAColumnRemovedInTheSameMigration() {
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());

        SchemaMigrationPlan migration = formRenderer.migrateSafelyPlan(
                sourceWithSequence("old_value"),
                targetWithSequence("new_value"),
                List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());

        assertEquals(0, sequenceCreates(migration.requests()));
    }

    @Test
    void reviewedRollbackKeepsSequenceNeededByTheRestoredSourceColumn() {
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        TableMetadata current = sourceWithSequence("old_value");
        SchemaMigrationPlan migration = formRenderer.migrateSafelyPlan(
                current,
                targetWithSequence("new_value"),
                List.of(),
                SchemaMigrationOptions.safe().allowDropColumn());

        List<String> rollback = SchemaMigrationReviewer.create(formRenderer)
                                                       .review(current,
                                                               migration,
                                                               SchemaMigrationReviewPolicy.allowBlocking())
                                                       .rollback()
                                                       .requests()
                                                       .stream()
                                                       .map(SqlRequest::sql)
                                                       .toList();

        assertTrue(rollback.stream().noneMatch(sql -> sql.equals("drop sequence \"shared_seq\"")));
    }

    private static long sequenceCreates(List<SqlRequest> requests) {
        return requests.stream()
                       .map(SqlRequest::sql)
                       .filter(sql -> sql.startsWith("create sequence "))
                       .count();
    }

    private static TableMetadata sourceWithSequence(String field) {
        return TableMetadata.builder("events")
                            .addColumn(ColumnMetadata.of("id", "BIGINT"))
                            .addColumn(ColumnMetadata.of(field, "BIGINT")
                                                     .withGeneration(ValueGeneration.sequence("shared_seq")))
                            .build();
    }

    private static DynamicForm targetWithSequence(String field) {
        return DynamicForm.builder("events", "events")
                          .addField(DynamicField.of("id", "BIGINT"))
                          .addField(DynamicField.of(field, "BIGINT").generatedBySequence("shared_seq"))
                          .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("case-sensitive-sequences", "SequenceTable")
                          .addField(DynamicField.of("upper_value", "BIGINT")
                                                .generatedBySequence("OrderSeq"))
                          .addField(DynamicField.of("lower_value", "BIGINT")
                                                .generatedBySequence("orderseq"))
                          .build();
    }
}
