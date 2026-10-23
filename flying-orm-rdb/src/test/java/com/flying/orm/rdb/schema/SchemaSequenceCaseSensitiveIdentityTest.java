package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaSequenceCaseSensitiveIdentityTest {

    private final RelationalSchemaSqlRenderer renderer =
            RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema());

    @Test
    void createsCaseDistinctPhysicalSequencesSeparately() {
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).createTable(form());

        assertEquals(3, requests.size());
        assertTrue(requests.stream().anyMatch(request -> request.sql().contains("\"OrderSeq\"")));
        assertTrue(requests.stream().anyMatch(request -> request.sql().contains("\"orderseq\"")));
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
        DynamicFormChangeSet changes = source.diffTo(target);
        FormSchemaSqlRenderer formRenderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        RelationalTableDefinition current = RelationalTableDefinition.builder("events")
                                             .addColumn(DynamicField.of("id", "BIGINT").column())
                                             .build();

        assertEquals(1, sequenceCreates(formRenderer.migrate(changes)));
        assertEquals(1, sequenceCreates(formRenderer.migrateSafelyPlan(current, target, List.of()).requests()));
    }

    @Test
    void unquotedSequenceNamesUseTheSamePhysicalIdentityInOrdinaryMigration() {
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL).build();
        DynamicForm source = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("first_value", "BIGINT").generatedBySequence("SharedSeq"))
                .addField(DynamicField.of("second_value", "BIGINT").generatedBySequence("sharedseq")).build();
        assertEquals(1, sequenceCreates(FormSchemaSqlRenderer.create(dialect).migrate(source.diffTo(target))));
        assertEquals(1, sequenceCreates(FormSchemaSqlRenderer.create(dialect)
                .migrateSafelyPlan(source.toTableDefinition(), target, List.of()).requests()));
    }

    @Test
    void ordinaryQualifiedTableKeepsExplicitBareSequenceNames() {
        DynamicForm source = DynamicForm.builder("events", "tenant.events")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("events", "tenant.events")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("new_value", "BIGINT").generatedBySequence("event_seq")).build();
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(RdbDialect.postgresql()).migrate(source.diffTo(target));
        assertEquals(1, sequenceCreates(requests));
        assertTrue(requests.getFirst().sql().startsWith("create sequence \"event_seq\""));
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
        DynamicFormChangeSet changes = source.diffTo(target);
        RelationalTableDefinition current = RelationalTableDefinition.builder("events")
                                             .addColumn(DynamicField.of("id", "BIGINT").column())
                                             .addColumn(DynamicField.of("existing_value", "BIGINT").column())
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
        DynamicFormChangeSet changes = source.diffTo(target);

        assertThrows(IllegalArgumentException.class,
                     () -> FormSchemaSqlRenderer.create(RdbDialect.postgresql()).migrate(changes));
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

    private static long sequenceCreates(List<SqlRequest> requests) {
        return requests.stream()
                       .map(SqlRequest::sql)
                       .filter(sql -> sql.startsWith("create sequence "))
                       .count();
    }

    private static RelationalTableDefinition sourceWithSequence(String field) {
        return RelationalTableDefinition.builder("events")
                            .addColumn(DynamicField.of("id", "BIGINT").column())
                            .addColumn(DynamicField.of(field, "BIGINT")
                                                     .withGeneration(ValueGeneration.sequence("shared_seq")).column())
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
