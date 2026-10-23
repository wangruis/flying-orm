package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SchemaOperationRendererTest {
    private static final RelationalTableDefinition TABLE = RelationalTableDefinition.builder(RelationIdentity.table("items"))
            .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build();

    @Test
    void sharedPlanningPropagatesTheOriginalRendererFailureAndStops() {
        UnsupportedOperationException failure = new UnsupportedOperationException("backend cannot change this type");
        AtomicInteger rendered = new AtomicInteger();
        SchemaOperation add = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, TABLE.identity(), "name",
                null, ColumnDefinition.builder("name", "VARCHAR").build(),
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);

        UnsupportedOperationException actual = assertThrows(UnsupportedOperationException.class,
                () -> SchemaOperationPlanner.plan(List.of(create(TABLE), add), null, SchemaDialect.standard(),
                        operation -> false, operation -> {
                            rendered.incrementAndGet();
                            throw failure;
                        }));

        assertSame(failure, actual);
        assertEquals(1, rendered.get(), "planning must not continue after an actual rendering failure");
    }

    @Test
    void relationalPlanningDoesNotReplaceAnUnsupportedDefinitionWithAManualStep() {
        com.flying.orm.rdb.dialect.RdbDialect dialect = com.flying.orm.rdb.dialect.RdbDialect.mysql();
        RelationalTableDefinition desired = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").generation(ValueGeneration.identity()).build())
                .build();

        UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class,
                () -> RelationalSchemaPlanReviewer.create(dialect).review(
                        com.flying.orm.rdb.dialect.DatabaseDescriptor.of("MySQL", "8.4", dialect), desired,
                        SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete(),
                        SchemaCompatibilityMode.EXACT));

        assertTrue(failure.getMessage().contains("first column of an index"));
    }

    @Test
    void ordinaryCreateAddAndChangePreserveTheExtensionFailure() {
        UnsupportedOperationException failure = new UnsupportedOperationException("unsupported by extension");
        SchemaDialect dialect = SchemaDialect.builder()
                .operationRenderer((operation, fallback) -> { throw failure; }).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "VARCHAR").withLength(32)).build();
        DynamicForm changed = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("name", "VARCHAR").withLength(64)).build();
        DynamicForm added = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("extra", "INTEGER")).build();

        assertSame(failure, assertThrows(UnsupportedOperationException.class,
                () -> renderer.createTablePlan(source, List.of())));
        assertSame(failure, assertThrows(UnsupportedOperationException.class,
                () -> renderer.migrateSafelyPlan(source.toTableDefinition(), added, List.of())));
        assertSame(failure, assertThrows(UnsupportedOperationException.class,
                () -> renderer.migrate(source.diffTo(changed))));
    }

    @Test
    void customColumnCommentSqlIsNotRejectedByBuiltinPreflight() {
        List<SchemaOperation.Kind> operations = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            operations.add(operation.kind());
            return List.of(new SqlRequest("custom " + operation.kind(), List.of()));
        }).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT").withComment("identifier"))
                .addField(DynamicField.of("name", "VARCHAR").withComment("name")).build();

        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(TABLE, target, List.of());

        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN, SchemaOperation.Kind.CHANGE_COLUMN), operations);
        assertEquals(List.of("custom ADD_COLUMN", "custom CHANGE_COLUMN"), plan.sqlTexts());
    }

    @Test
    void defaultAddColumnCannotSilentlyDiscardAnExplicitComment() {
        ColumnDefinition column = ColumnDefinition.builder("name", "VARCHAR").comment("name").build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN, TABLE.identity(),
                column.name(), null, column, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertThrows(UnsupportedOperationException.class,
                () -> RelationalSchemaSqlRenderer.create(SchemaDialect.standard()).render(operation));
    }

    @Test
    void ordinaryColumnChangesUseTheSharedOperationExtensionWithoutEmittingNoops() {
        List<SchemaOperation.Kind> operations = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .operationRenderer((operation, fallback) -> {
                    operations.add(operation.kind());
                    assertEquals("name", operation.objectName());
                    assertEquals(32, ((ColumnDefinition) operation.actual()).length());
                    assertEquals(64, ((ColumnDefinition) operation.desired()).length());
                    List<SqlRequest> requests = fallback.get();
                    assertSame(requests, fallback.get());
                    return requests;
                }).build();
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(32)).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(64)).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);
        List<SqlRequest> direct = renderer.migrate(source.diffTo(target));
        assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN), operations);
        operations.clear();
        assertEquals(direct, renderer.migrateSafelyPlan(source.toTableDefinition(), target, List.of()).requests());
        assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN), operations);
        operations.clear();
        assertTrue(renderer.migrateSafelyPlan(source.toTableDefinition(), source, List.of()).requests().isEmpty());
        assertTrue(operations.isEmpty());
    }

    @Test
    void ordinaryColumnFallbackOwnsItsSequenceDependenciesExactlyOnce() {
        List<Long> sequenceCounts = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .operationRenderer((operation, fallback) -> {
                    List<SqlRequest> requests = fallback.get();
                    assertSame(requests, fallback.get());
                    sequenceCounts.add(requests.stream().filter(sql -> sql.sql().startsWith("create sequence ")).count());
                    return requests;
                }).build();
        DynamicForm source = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("first_value", "BIGINT").generatedBySequence("item_values"))
                .addField(DynamicField.of("second_value", "BIGINT").generatedBySequence("item_values")).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);

        List<SqlRequest> direct = renderer.migrate(source.diffTo(target));
        assertEquals(List.of(1L, 0L), sequenceCounts);
        sequenceCounts.clear();
        List<SqlRequest> planned = renderer.migrateSafelyPlan(source.toTableDefinition(), target, List.of()).requests();
        assertEquals(List.of(1L, 0L), sequenceCounts);
        assertEquals(direct, planned);
        assertEquals(3, planned.size());
    }

    @Test
    void replacingOrdinaryAddDoesNotLeakDefaultSequenceSql() {
        SqlRequest custom = new SqlRequest("custom backend column and generation", List.of());
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .operationRenderer((operation, fallback) -> List.of(custom)).build();
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("new_value", "BIGINT").generatedBySequence("item_values")).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);

        assertEquals(List.of(custom), renderer.migrate(source.diffTo(target)));
        assertEquals(List.of(custom), renderer.migrateSafelyPlan(source.toTableDefinition(), target, List.of()).requests());
    }

    @Test
    void ordinaryCreateUsesTheSameOperationExtensionAndDependencies() {
        AtomicInteger calls = new AtomicInteger();
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .operationRenderer((operation, fallback) -> {
                    calls.incrementAndGet();
                    assertEquals(SchemaOperation.Kind.CREATE_TABLE, operation.kind());
                    List<SqlRequest> requests = fallback.get();
                    assertSame(requests, fallback.get());
                    assertTrue(requests.getFirst().sql().startsWith("create sequence "));
                    return requests;
                }).build();
        DynamicForm target = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.primaryKey("id", "BIGINT").generatedBySequence("item_ids")).build();
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(dialect).createTable(target);
        assertEquals(1, calls.get());
        assertEquals(2, requests.size());
        assertTrue(requests.getLast().sql().contains("primary key"));
    }

    @Test
    void directlyAddedUniqueFieldRendersItsIndexThroughTheOperationExtension() {
        List<SchemaOperation.Kind> operations = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            operations.add(operation.kind());
            return fallback.get();
        }).build();
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("code", "VARCHAR").withLength(32).withUnique(true)).build();
        List<SqlRequest> requests = FormSchemaSqlRenderer.create(dialect).migrate(source.diffTo(target));
        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN, SchemaOperation.Kind.ADD_INDEX), operations);
        assertEquals(2, requests.size());
        assertTrue(requests.getLast().sql().startsWith("create unique index "));
    }

    @Test
    void ordinaryAddAndDropUseTheSharedOperationExtension() {
        List<SchemaOperation.Kind> operations = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            operations.add(operation.kind());
            List<SqlRequest> requests = fallback.get();
            assertSame(requests, fallback.get());
            return requests;
        }).build();
        DynamicForm source = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("old_value", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("items", "tenant.items")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("new_value", "BIGINT")).build();
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(dialect);

        List<String> direct = renderer.migrate(source.diffTo(target)).stream().map(SqlRequest::sql).toList();
        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN, SchemaOperation.Kind.DROP_COLUMN), operations);
        operations.clear();
        SchemaMigrationPlan plan = renderer.migrateSafelyPlan(source.toTableDefinition(), target,
                List.of(), List.of(), SchemaMigrationOptions.safe().allowDropColumn());
        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN, SchemaOperation.Kind.DROP_COLUMN), operations);
        assertEquals(direct, plan.sqlTexts());
        assertEquals(List.of("alter table tenant.items add column new_value BIGINT",
                "alter table tenant.items drop column old_value"), direct);
    }

    @Test
    void ordinaryColumnRenameUsesTheSharedOperationExtension() {
        List<String> operations = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            operations.add(operation.kind().name());
            assertEquals("id", ((ColumnDefinition) operation.actual()).name());
            assertEquals("item_id", ((ColumnDefinition) operation.desired()).name());
            return fallback.get();
        }).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("item_id", "BIGINT")).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(dialect).migrateSafelyPlan(
                TABLE, target, List.of(), List.of(), SchemaMigrationOptions.safe().renameColumn("id", "item_id"));

        assertEquals(List.of("RENAME_COLUMN"), operations);
        assertEquals(List.of("alter table items rename column id to item_id"), plan.sqlTexts());
    }

    @Test
    void backendCanReplaceAnOperationWithoutInvokingTheBuiltinSyntax() {
        List<SqlRequest> output = new ArrayList<>();
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            assertSame(TABLE, operation.desired());
            output.add(new SqlRequest("create table items (id bigint) engine=custom", List.of()));
            return output;
        }).build();
        List<SqlRequest> requests = RelationalSchemaSqlRenderer.create(dialect).render(create(TABLE));
        output.clear();
        assertEquals("create table items (id bigint) engine=custom", requests.getFirst().sql());
        assertThrows(UnsupportedOperationException.class, requests::clear);
    }

    @Test
    void renamePreservesSegmentedTableIdentity() {
        SchemaOperation rename = SchemaOperation.of(SchemaOperation.Kind.RENAME_COLUMN,
                RelationIdentity.of(null, "tenant", "items.archive"), "new_id",
                ColumnDefinition.builder("old_id", "BIGINT").build(),
                ColumnDefinition.builder("new_id", "BIGINT").build(), SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertEquals("alter table \"tenant\".\"items.archive\" rename column \"old_id\" to \"new_id\"",
                RelationalSchemaSqlRenderer.create(com.flying.orm.rdb.dialect.RdbDialect.postgresql().schema())
                        .render(rename).getFirst().sql());
        assertEquals("exec sp_rename N'[tenant].[items.archive].[old_id]', N'new_id', N'COLUMN'",
                RelationalSchemaSqlRenderer.create(com.flying.orm.rdb.dialect.RdbDialect.sqlServer().schema())
                        .render(rename).getFirst().sql());
    }

    @Test
    void delegatesUnchangedOperationsAndSnapshotsBuilderRegistration() {
        AtomicInteger calls = new AtomicInteger();
        SchemaDialect.Builder builder = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            calls.incrementAndGet();
            return fallback.get();
        });
        SchemaDialect dialect = builder.build();
        builder.operationRenderer((operation, fallback) -> { throw new AssertionError("later registration"); });
        List<SqlRequest> actual = RelationalSchemaSqlRenderer.create(dialect).render(create(TABLE));
        List<SqlRequest> expected = RelationalSchemaSqlRenderer.create(SchemaDialect.standard()).render(create(TABLE));
        assertEquals(expected.stream().map(SqlRequest::sql).toList(), actual.stream().map(SqlRequest::sql).toList());
        assertEquals(1, calls.get());
    }

    @Test
    void emptyResultsCannotSilentlyRemoveAnExecutableOperation() {
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> List.of()).build();
        assertThrows(IllegalArgumentException.class,
                () -> RelationalSchemaSqlRenderer.create(dialect).render(create(TABLE)));
    }

    @Test
    void unknownFactsCannotBeTurnedIntoExecutableDdl() {
        SchemaDialect dialect = SchemaDialect.builder().operationRenderer((operation, fallback) -> {
            fail("manual facts must not enter custom rendering");
            return List.of();
        }).build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.VERIFY_MANUALLY, TABLE.identity(),
                "unknown", null, null, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertThrows(UnsupportedOperationException.class,
                () -> RelationalSchemaSqlRenderer.create(dialect).render(operation));
        assertThrows(IllegalArgumentException.class,
                () -> new SchemaPlanStep(operation, List.of(new SqlRequest("select 1", List.of()))));
    }

    @Test
    void failedCallbackDoesNotPublishSequencePreparationAndFallbackIsReusable() {
        AtomicInteger calls = new AtomicInteger();
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .operationRenderer((operation, fallback) -> {
                    List<SqlRequest> first = fallback.get();
                    assertSame(first, fallback.get());
                    if (calls.getAndIncrement() == 0) throw new UnsupportedOperationException("custom rejection");
                    return first;
                }).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(TABLE.identity())
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .generation(ValueGeneration.sequence("item_ids")).build()).build();
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(dialect);
        LinkedHashMap<String, SchemaSequenceSqlRenderer.Definition> sequences = new LinkedHashMap<>();
        assertThrows(UnsupportedOperationException.class, () -> renderer.render(create(table), sequences));
        assertTrue(sequences.isEmpty());
        List<SqlRequest> retried = renderer.render(create(table), sequences);
        assertTrue(retried.getFirst().sql().startsWith("create sequence"));
        assertEquals(1, sequences.size());
    }

    @Test
    void recoveredBuiltinFailureDoesNotRetainItsPartialSequenceState() {
        SchemaDialect dialect = SchemaDialect.builder()
                .generatedValues(SchemaDialect.GeneratedValueStyle.POSTGRESQL)
                .mapType("VARCHAR", type -> { throw new UnsupportedOperationException("custom type required"); })
                .operationRenderer((operation, fallback) -> {
                    assertThrows(UnsupportedOperationException.class, fallback::get);
                    return List.of(new SqlRequest("custom create table items", List.of()));
                }).build();
        RelationalTableDefinition table = RelationalTableDefinition.builder(TABLE.identity())
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .generation(ValueGeneration.sequence("item_ids")).build())
                .addColumn(ColumnDefinition.builder("name", "VARCHAR").build()).build();
        LinkedHashMap<String, SchemaSequenceSqlRenderer.Definition> sequences = new LinkedHashMap<>();
        RelationalSchemaSqlRenderer.create(dialect).render(create(table), sequences);
        assertTrue(sequences.isEmpty(), "discarded default SQL must not mark its sequence as prepared");
    }

    private static SchemaOperation create(RelationalTableDefinition table) {
        return SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, table.identity(), "items", null, table,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
    }
}
