package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyMetadata;
import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaPlannerSurfaceConvergenceTest {

    @Test
    void jdbcPlanningHasNoSecondPlannerOwner() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.schema.JdbcSchemaMigrationPlanner", false,
                getClass().getClassLoader()));
    }

    @Test
    void bothClientsReuseTheRendererOwnedPlanner() throws ReflectiveOperationException {
        Harness harness = new Harness(Map.of(), Map.of());
        Object planner = field(harness.renderer, "migrations");
        assertSame(planner, field(harness.jdbc, "planner"));
        assertSame(planner, field(harness.reactive, "planner"));
        assertSame(planner, field(harness.jdbc.withMigrationObserver(SchemaMigrationObserver.noop()), "planner"));
        assertSame(planner, field(harness.reactive.withMigrationObserver(SchemaMigrationObserver.noop()), "planner"));
    }

    @Test
    void ordinaryCreateAndAddKeepIdenticalPlanAndReviewFacts() {
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR").withLength(80)).build();
        for (boolean exists : List.of(false, true)) {
            Harness harness = new Harness(exists ? Map.of(source.table(), source) : Map.of(), Map.of());
            ReviewedSchemaMigrationPlan review = harness.assertParity(target);
            assertEquals(exists, review.migration().tableExists());
            assertEquals(1, review.migration().requests().size());
            assertEquals(exists ? "alter table items add column note VARCHAR(80)"
                    : "create table items (id BIGINT primary key, note VARCHAR(80))",
                    review.migration().requests().getFirst().sql());
            assertEquals(List.of("items", "items", "items", "items"), harness.metadata.tables);
        }
    }

    @Test
    void containsCreateAndExistingOwnerKeepAuxiliaryRollbackAndInvalidationFacts() {
        DynamicForm target = protectedForm();
        String side = ProtectedContainsLayout.resolve(target).orElseThrow().table().table();
        for (boolean ownerExists : List.of(false, true)) {
            Harness harness = new Harness(ownerExists
                    ? Map.of(target.table(), ProtectedFormLayout.physical(target)) : Map.of(), Map.of());
            ReviewedSchemaMigrationPlan review = harness.assertParity(target);
            assertEquals(ownerExists, review.migration().tableExists());
            assertEquals(List.of(side), review.migration().additionalCreatedTables());
            assertTrue(review.migration().sqlTexts().stream()
                    .anyMatch(sql -> sql.startsWith("create table " + side + " (")));
            List<String> rollback = review.rollback().requests().stream().map(SqlRequest::sql).toList();
            assertEquals("drop table " + side, rollback.getFirst());
            assertEquals(ownerExists ? 1 : 2, rollback.size());
            assertEquals(List.of(), review.rollback().gaps());
            assertEquals(ownerExists ? List.of(target.table(), side, target.table(), side,
                    target.table(), side, target.table(), side)
                    : List.of(target.table(), target.table(), target.table(), target.table()),
                    harness.metadata.tables);
        }
    }

    @Test
    void onlyTheExistingMissingTableProtocolTriggersCreate() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        for (String message : List.of("table metadata not found", "table metadata not found: items")) {
            Harness harness = new Harness(Map.of(),
                    Map.of(target.table(), new IllegalArgumentException(message)));
            assertFalse(harness.assertParity(target).migration().tableExists());
        }
        for (RuntimeException failure : List.of(
                new IllegalArgumentException("table metadata not found because access is denied"),
                new IllegalStateException("table metadata not found"), new IllegalArgumentException())) {
            new Harness(Map.of(), Map.of(target.table(), failure)).assertFailure(target, failure);
        }
    }

    @Test
    void auxiliaryMetadataFailuresAreNotReclassifiedAsMissingOwner() {
        DynamicForm target = protectedForm();
        String side = ProtectedContainsLayout.resolve(target).orElseThrow().table().table();
        IllegalArgumentException failure = new IllegalArgumentException("auxiliary metadata cannot be represented");
        Harness harness = new Harness(Map.of(target.table(), ProtectedFormLayout.physical(target)),
                Map.of(side, failure));
        harness.assertFailure(target, failure);
        assertEquals(List.of(target.table(), side, target.table(), side,
                target.table(), side, target.table(), side), harness.metadata.tables);
    }

    @Test
    void reactivePlanAndReviewFreezeStructureListsBeforeSubscription() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("first_parent", "BIGINT"))
                .addField(DynamicField.of("second_parent", "BIGINT"))
                .build();
        IndexMetadata firstIndex = IndexMetadata.builder("ix_items_first")
                .addColumn("first_parent").build();
        IndexMetadata secondIndex = IndexMetadata.builder("ix_items_second")
                .addColumn("second_parent").build();
        ForeignKeyMetadata firstForeignKey = foreignKey(
                "fk_items_first", "first_parent", "first_parents");
        ForeignKeyMetadata secondForeignKey = foreignKey(
                "fk_items_second", "second_parent", "second_parents");

        for (boolean exists : List.of(false, true)) {
            Harness harness = new Harness(exists ? Map.of(target.table(), target) : Map.of(), Map.of());
            List<IndexMetadata> indexes = new ArrayList<>(List.of(firstIndex));
            List<ForeignKeyMetadata> foreignKeys = new ArrayList<>(List.of(firstForeignKey));
            Mono<SchemaMigrationPlan> plan = harness.reactive.planCreateOrAlter(
                    target, indexes, foreignKeys, harness.reactiveReader, SchemaMigrationOptions.safe());
            Mono<ReviewedSchemaMigrationPlan> review = harness.reactive.reviewCreateOrAlter(
                    target, indexes, foreignKeys, harness.reactiveReader,
                    SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking());

            assertTrue(harness.metadata.tables.isEmpty());
            indexes.set(0, secondIndex);
            foreignKeys.set(0, secondForeignKey);

            SchemaMigrationPlan planned = plan.block();
            ReviewedSchemaMigrationPlan reviewed = review.block();
            assertNotNull(planned);
            assertNotNull(reviewed);
            assertEquals(List.of("ix_items_first"),
                    planned.targetIndexes().stream().map(IndexMetadata::name).toList());
            assertEquals(List.of("fk_items_first"),
                    planned.targetForeignKeys().stream().map(ForeignKeyMetadata::name).toList());
            assertEquals(List.of("ix_items_first"), reviewed.migration().targetIndexes().stream()
                    .map(IndexMetadata::name).toList());
            assertEquals(List.of("fk_items_first"), reviewed.migration().targetForeignKeys().stream()
                    .map(ForeignKeyMetadata::name).toList());
        }
    }

    private static ForeignKeyMetadata foreignKey(String name, String column, String referenceTable) {
        return ForeignKeyMetadata.builder(name)
                .addColumn(column)
                .referenceTable(referenceTable)
                .addReferenceColumn("id")
                .build();
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("note", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
    }

    private static Object field(Object target, String name) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void assertPlan(SchemaMigrationPlan jdbc, SchemaMigrationPlan reactive) {
        assertNotNull(reactive);
        assertEquals(jdbc.target().table(), reactive.target().table());
        assertEquals(jdbc.target().fields(), reactive.target().fields());
        assertEquals(jdbc.targetIndexes(), reactive.targetIndexes());
        assertEquals(jdbc.targetForeignKeys(), reactive.targetForeignKeys());
        assertEquals(jdbc.tableExists(), reactive.tableExists());
        assertEquals(jdbc.requests(), reactive.requests());
        assertEquals(jdbc.skippedChanges(), reactive.skippedChanges());
        assertEquals(jdbc.additionalCreatedTables(), reactive.additionalCreatedTables());
    }

    private static final class Harness {
        private final MetadataExecutor metadata;
        private final FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.h2());
        private final JdbcSchemaClient jdbc;
        private final ReactiveSchemaClient reactive;
        private final JdbcFormMetadataReader jdbcReader;
        private final ReactiveFormMetadataReader reactiveReader;

        private Harness(Map<String, DynamicForm> tables, Map<String, RuntimeException> failures) {
            metadata = new MetadataExecutor(tables, failures);
            ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> Flux.fromIterable(metadata.query(request)));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("planning must not execute DDL"));
                }
            };
            jdbc = JdbcSchemaClient.create(metadata, renderer);
            reactive = ReactiveSchemaClient.create(reactiveExecutor, renderer);
            jdbcReader = JdbcFormMetadataReaders.create(metadata, RdbDialect.h2());
            reactiveReader = ReactiveFormMetadataReaders.create(reactiveExecutor, RdbDialect.h2());
        }

        private ReviewedSchemaMigrationPlan assertParity(DynamicForm target) {
            SchemaMigrationPlan plan = jdbc.planCreateOrAlter(target, List.of(), jdbcReader);
            int beforeSubscription = metadata.tables.size();
            Mono<SchemaMigrationPlan> pending = reactive.planCreateOrAlter(target, List.of(), reactiveReader);
            assertEquals(beforeSubscription, metadata.tables.size());
            assertPlan(plan, pending.block());
            assertTrue(metadata.tables.size() > beforeSubscription);
            ReviewedSchemaMigrationPlan jdbcReview = jdbc.reviewCreateOrAlter(target, List.of(), List.of(),
                    jdbcReader, SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking());
            ReviewedSchemaMigrationPlan reactiveReview = reactive.reviewCreateOrAlter(target, List.of(), List.of(),
                    reactiveReader, SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking()).block();
            assertNotNull(reactiveReview);
            assertPlan(plan, jdbcReview.migration());
            assertPlan(jdbcReview.migration(), reactiveReview.migration());
            assertEquals(jdbcReview.rollback(), reactiveReview.rollback());
            assertEquals(jdbcReview.onlineDdl(), reactiveReview.onlineDdl());
            assertEquals(jdbcReview.fingerprint(), reactiveReview.fingerprint());
            return jdbcReview;
        }

        private void assertFailure(DynamicForm target, RuntimeException expected) {
            assertSame(expected, assertThrows(RuntimeException.class,
                    () -> jdbc.planCreateOrAlter(target, List.of(), jdbcReader)));
            assertSame(expected, assertThrows(RuntimeException.class,
                    () -> reactive.planCreateOrAlter(target, List.of(), reactiveReader).block()));
            assertSame(expected, assertThrows(RuntimeException.class, () -> jdbc.reviewCreateOrAlter(
                    target, List.of(), List.of(), jdbcReader,
                    SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking())));
            assertSame(expected, assertThrows(RuntimeException.class, () -> reactive.reviewCreateOrAlter(
                    target, List.of(), List.of(), reactiveReader,
                    SchemaMigrationOptions.safe(), SchemaMigrationReviewPolicy.allowBlocking()).block()));
        }
    }

    private static final class MetadataExecutor implements SyncSqlExecutor {
        private final Map<String, DynamicForm> forms;
        private final Map<String, RuntimeException> failures;
        private final List<String> tables = new ArrayList<>();

        private MetadataExecutor(Map<String, DynamicForm> forms, Map<String, RuntimeException> failures) {
            this.forms = forms;
            this.failures = failures;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            if (!request.sql().contains("from INFORMATION_SCHEMA.COLUMNS c")) {
                return List.of();
            }
            String table = (String) request.parameters().getFirst();
            tables.add(table);
            if (failures.containsKey(table)) {
                throw failures.get(table);
            }
            DynamicForm form = forms.get(table);
            return form == null ? List.of() : form.fields().stream().map(field -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("COLUMN_NAME", field.name());
                row.put("DATA_TYPE", field.databaseType().baseName().startsWith("PROTECTED_")
                        ? "BLOB" : field.databaseType().canonical());
                row.put("PRIMARY_KEY", field.primaryKey());
                row.put("IS_NULLABLE", field.nullable() ? "YES" : "NO");
                row.put("CHARACTER_MAXIMUM_LENGTH", field.length());
                return DynamicRow.copyOf(row);
            }).toList();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("planning must not execute DDL");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("planning must not execute DDL");
        }
    }
}
