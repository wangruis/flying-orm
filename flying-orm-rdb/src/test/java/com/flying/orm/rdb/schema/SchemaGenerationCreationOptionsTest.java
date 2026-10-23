package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicFormChangeSet;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SchemaGenerationCreationOptionsTest {
    private static final RelationIdentity TABLE = RelationIdentity.table("events");

    @Test
    void mysqlHonorsInitialValueInCreateAndAddColumnForBothModels() {
        RdbDialect dialect = RdbDialect.mysql();
        ValueGeneration generation = ValueGeneration.identity(700, 1, 0);
        DynamicField field = DynamicField.primaryKey("serial", "BIGINT").withGeneration(generation);
        DynamicForm target = DynamicForm.builder("events", "events").addField(field).build();
        FormSchemaSqlRenderer legacy = FormSchemaSqlRenderer.create(dialect);
        assertTrue(assertDoesNotThrow(() -> legacy.createTable(target)).getFirst().sql()
                .endsWith(" auto_increment = 700"));

        ColumnDefinition column = ColumnDefinition.builder("serial", "BIGINT")
                .nullable(false).generation(generation).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE).addColumn(column)
                .primaryKey(PrimaryKeyDefinition.of("events_pk", "serial")).build();
        SchemaMigrationPlan creation = review(dialect, desired, SchemaSnapshot.absent(TABLE));
        assertTrue(creation.requests().getFirst().sql().endsWith(" auto_increment = 700"));

        DynamicField ordinary = DynamicField.of("name", "VARCHAR").withLength(32);
        DynamicForm source = DynamicForm.builder("events", "events").addField(ordinary).build();
        DynamicForm expanded = DynamicForm.builder("events", "events").addField(ordinary).addField(field).build();
        DynamicFormChangeSet changes = source.diffTo(expanded);
        assertTrue(assertDoesNotThrow(() -> legacy.migrate(changes)).getFirst().sql()
                .endsWith(", auto_increment = 700"));
        SchemaOperation addition = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, TABLE, "serial",
                null, column, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertTrue(RelationalSchemaSqlRenderer.create(dialect.schema()).render(addition).getFirst().sql()
                .endsWith(", auto_increment = 700"));
    }

    @Test
    void oracleRendersCallerInitialValueAndDescendingIncrement() {
        for (ValueGeneration generation : List.of(ValueGeneration.sequence("event_seq", 700, 2, 20),
                ValueGeneration.sequence("event_seq", -700, -2, 20))) {
            String options = "start with " + generation.startWith() + " increment by " + generation.incrementBy();
            RdbDialect dialect = RdbDialect.oracle();
            DynamicForm form = DynamicForm.builder("events", "events")
                    .addField(DynamicField.of("serial", "BIGINT").withGeneration(generation)).build();
            assertTrue(assertDoesNotThrow(() -> FormSchemaSqlRenderer.create(dialect).createTable(form))
                    .getFirst().sql().contains(options));
            SchemaMigrationPlan plan = review(dialect, table(column("serial", generation)), SchemaSnapshot.absent(TABLE));
            assertTrue(plan.requests().getFirst().sql().contains(options));
        }
    }

    @Test
    void observedComparisonIgnoresOnlyUnavailableInitialValues() {
        assertObservedOptions(RdbDialect.mysql(), ValueGeneration.identity(), ValueGeneration.identity(700, 1, 0));
        assertObservedOptions(RdbDialect.oracle(), ValueGeneration.sequence("event_seq", 1, 2, 20),
                ValueGeneration.sequence("event_seq", 700, 2, 0));
        assertObservedOptions(RdbDialect.oracle(), ValueGeneration.sequence("event_seq", -1, -2, 20),
                ValueGeneration.sequence("event_seq", -700, -2, 20));

        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            assertFalse(SchemaGeneratedValueComparison.same(ValueGeneration.sequence("event_seq"),
                    ValueGeneration.sequence("event_seq", 7, 1, 100),
                    dialect.schema().generatedValueStyle()));
        }
        assertFalse(SchemaGeneratedValueComparison.same(ValueGeneration.identity(),
                ValueGeneration.identity(7, 2, 0),
                SchemaDialect.GeneratedValueStyle.MYSQL));
        assertFalse(SchemaGeneratedValueComparison.same(ValueGeneration.sequence("event_seq", 1, 2, 20),
                ValueGeneration.sequence("event_seq", 7, 3, 20),
                SchemaDialect.GeneratedValueStyle.ORACLE));
    }

    @Test
    void oracleReusesObservedSequenceButRejectsConflictingNewDeclarations() {
        RdbDialect dialect = RdbDialect.oracle();
        ValueGeneration observed = ValueGeneration.sequence("event_seq", 1, 2, 20);
        ValueGeneration requested = ValueGeneration.sequence("event_seq", 700, 2, 0);
        RelationalTableDefinition desired = table(column("first_value", requested), column("second_value", requested));
        SchemaMigrationPlan plan = assertDoesNotThrow(() -> review(dialect, desired,
                SchemaSnapshot.present(table(column("first_value", observed)))));
        assertEquals(0, plan.requests().stream().filter(sql -> sql.sql().startsWith("create sequence ")).count());
        assertTrue(plan.requests().stream().anyMatch(sql -> sql.sql().contains("second_value")));

        DynamicField old = DynamicField.of("first_value", "BIGINT").withGeneration(observed);
        DynamicField added = DynamicField.of("second_value", "BIGINT").withGeneration(requested);
        DynamicForm source = DynamicForm.builder("events", "events").addField(old).build();
        DynamicForm target = DynamicForm.builder("events", "events").addField(old).addField(added).build();
        List<SqlRequest> requests = assertDoesNotThrow(() -> FormSchemaSqlRenderer.create(dialect)
                .migrate(source.diffTo(target)));
        assertTrue(requests.stream().noneMatch(sql -> sql.sql().startsWith("create sequence ")));

        RelationalTableDefinition conflicting = table(column("first_value", requested),
                column("second_value", ValueGeneration.sequence("event_seq", 800, 2, 0)));
        assertThrows(IllegalArgumentException.class, () -> review(dialect, conflicting, SchemaSnapshot.absent(TABLE)));
    }

    private static void assertObservedOptions(RdbDialect dialect, ValueGeneration actual, ValueGeneration desired) {
        assertTrue(SchemaGeneratedValueComparison.same(actual, desired, dialect.schema().generatedValueStyle()));
        SchemaMigrationPlan plan = review(dialect, table(column("serial", desired)),
                SchemaSnapshot.present(table(column("serial", actual))));
        assertTrue(plan.steps().isEmpty(), plan.operations().toString());
    }

    @Test
    void directMigrationDoesNotTreatUnavailableInitialValueAsConstraintChange() {
        DynamicField before = DynamicField.of("serial", "BIGINT")
                .withGeneration(ValueGeneration.sequence("event_seq", 1, 2, 20));
        DynamicField after = DynamicField.of("serial", "BIGINT")
                .withGeneration(ValueGeneration.sequence("event_seq", 700, 2, 20));
        DynamicForm source = DynamicForm.builder("events", "events").addField(before).build();
        DynamicForm target = DynamicForm.builder("events", "events").addField(after).build();
        DynamicFormChangeSet change = source.diffTo(target);
        assertTrue(assertDoesNotThrow(() -> FormSchemaSqlRenderer.create(RdbDialect.oracle())
                .migrate(change)).isEmpty());
    }

    private static ColumnDefinition column(String name, ValueGeneration generation) {
        return ColumnDefinition.builder(name, "BIGINT").generation(generation).build();
    }

    private static RelationalTableDefinition table(ColumnDefinition... columns) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(TABLE);
        for (ColumnDefinition column : columns) builder.addColumn(column);
        return builder.build();
    }

    private static SchemaMigrationPlan review(RdbDialect dialect, RelationalTableDefinition desired, SchemaSnapshot actual) {
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired, actual,
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }
}
