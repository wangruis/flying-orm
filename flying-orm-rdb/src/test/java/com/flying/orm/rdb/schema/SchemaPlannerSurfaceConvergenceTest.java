package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
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
    void formAndRelationalCreationUseTheSamePhysicalDeclaration() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR").withLength(80)).build();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            RelationalTableDefinition table = form.toTableDefinition();
            SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE,
                    table.identity(), table.name(), null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
            assertEquals(RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation),
                    FormSchemaSqlRenderer.create(dialect).createTable(form), dialect.name());
        }
    }

    @Test
    void bothCreationEntrypointsPreserveTheDeclaredSequenceNamespace() {
        for (String sequence : List.of("items_seq", "sequences.items_seq")) {
            DynamicForm form = DynamicForm.builder("items", "tenant.items")
                    .addField(DynamicField.primaryKey("id", "BIGINT").generatedBySequence(sequence)).build();
            RelationalTableDefinition table = form.toTableDefinition();
            SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE,
                    table.identity(), table.name(), null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL);
            RdbDialect dialect = RdbDialect.postgresql();
            assertEquals(FormSchemaSqlRenderer.create(dialect).createTable(form),
                    RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation));
        }
    }

    @Test
    void simpleTableCommentUsesTheSameRelationResolution() {
        SchemaDialect dialect = RdbDialect.sqlServer().schema();
        assertEquals(dialect.columnCommentSql(RelationIdentity.table("items"), "note", "comment"),
                dialect.columnCommentSql("items", "note", "comment"));
    }

    @Test
    void schemaDialectDoesNotRetainUnreachableInternalOverloads() {
        assertThrows(NoSuchMethodException.class, () -> SchemaDialect.class.getDeclaredMethod(
                "tableCommentSql", String.class, String.class));
        assertThrows(NoSuchMethodException.class, () -> SchemaDialectDdlSupport.class.getDeclaredMethod(
                "tableCommentSql", String.class, String.class));
        assertThrows(NoSuchMethodException.class, () -> SchemaDialectDdlSupport.class.getDeclaredMethod(
                "sqlServerTableComment", String.class, String.class));
        assertThrows(NoSuchMethodException.class, () -> SchemaDialect.class.getDeclaredMethod(
                "alterColumnTypeSql", RelationIdentity.class,
                String.class, String.class));
        for (Class<?> tableType : List.of(String.class, RelationIdentity.class)) {
            assertThrows(NoSuchMethodException.class, () -> SchemaDialect.class.getDeclaredMethod(
                    "alterColumnNullabilitySql", tableType, String.class, String.class, String.class, boolean.class));
        }
    }

    @Test
    void jdbcPlanningHasNoSecondPlannerOwner() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.schema.JdbcSchemaMigrationPlanner", false,
                getClass().getClassLoader()));
    }

    @Test
    void schemaClientsHaveOneCompleteDialectConstructionPath() {
        assertThrows(NoSuchMethodException.class, () -> JdbcSchemaClient.class.getDeclaredMethod(
                "create", SyncSqlExecutor.class, FormSchemaSqlRenderer.class));
        assertThrows(NoSuchMethodException.class, () -> ReactiveSchemaClient.class.getDeclaredMethod(
                "create", ReactiveSqlExecutor.class, FormSchemaSqlRenderer.class));
    }

    @Test
    void auxiliaryTablesUseTheOrdinaryMigrationOwner() {
        assertTrue(ProtectedContainsLayout.resolve(protectedForm()).orElseThrow().table().protections().isEmpty());
        for (java.lang.reflect.Method method : ProtectedSchemaMigrationPlanner.class.getDeclaredMethods()) {
            assertFalse(List.of("readReactivePlan", "readJdbcPlan", "migrateContainsPlan", "createContainsPlan")
                    .contains(method.getName()), "auxiliary metadata and migration must use the ordinary planner");
        }
    }

    @Test
    void sqlServerCommentsShareRelationIdentityAndSql() {
        SchemaDialect dialect = RdbDialect.sqlServer().schema();
        RelationIdentity relation =
                RelationIdentity.of(null, "app", "items");
        String suffix = ", @level0type = N'SCHEMA', @level0name = N'app'"
                + ", @level1type = N'TABLE', @level1name = N'items'"
                + ", @level2type = N'COLUMN', @level2name = N'note'";
        for (String previous : java.util.Arrays.asList(null, "old")) {
            for (String target : java.util.Arrays.asList(null, "it's 名称")) {
                java.util.Optional<String> rendered = dialect.columnCommentChangeSql(relation, "note", previous, target);
                if (previous == null && target == null) {
                    assertTrue(rendered.isEmpty());
                } else {
                    String procedure = previous == null ? "sp_addextendedproperty"
                            : target == null ? "sp_dropextendedproperty" : "sp_updateextendedproperty";
                    assertEquals("exec " + procedure + " @name = N'MS_Description'"
                            + (target == null ? "" : ", @value = N'it''s 名称'") + suffix, rendered.orElseThrow());
                }
            }
        }
        assertEquals(dialect.columnCommentSql("app.items", "note", "it's 名称"),
                dialect.columnCommentSql(relation, "note", "it's 名称"));
        assertEquals("exec sp_addextendedproperty @name = N'MS_Description', @value = N'it''s 名称'"
                        + suffix.substring(0, suffix.indexOf(", @level2type")),
                dialect.tableCommentSql(relation, "it's 名称").orElseThrow());
        assertTrue(dialect.tableCommentSql(relation, null).isEmpty());
        assertThrows(UnsupportedOperationException.class,
                () -> dialect.tableCommentSql(RelationIdentity.of("catalog", "app", "items"), "comment"));
        String implicitTableComment = dialect.tableCommentSql(
                RelationIdentity.table("item.part"), "it's 名称").orElseThrow();
        assertTrue(implicitTableComment.startsWith("exec sp_executesql "));
        assertFalse(implicitTableComment.contains("@level2type"));
        assertThrows(NullPointerException.class, () -> dialect.columnCommentSql(relation, null, "comment"));
        assertThrows(NullPointerException.class,
                () -> dialect.columnCommentChangeSql(relation, null, "old", "new"));
        assertThrows(UnsupportedOperationException.class,
                () -> dialect.columnCommentSql("catalog.app.items", "note", "comment"));
        assertTrue(dialect.columnCommentSql(RelationIdentity.table("item.part"),
                "note", "comment").orElseThrow().startsWith("exec sp_executesql "));
    }

    @Test
    void bothClientsReuseTheRendererOwnedPlanner() throws ReflectiveOperationException {
        Harness harness = new Harness(Map.of(), Map.of());
        Object jdbcPlanner = field(field(harness.jdbc, "renderer"), "migrations");
        Object reactivePlanner = field(field(harness.reactive, "renderer"), "migrations");
        assertSame(jdbcPlanner, field(harness.jdbc, "planner"));
        assertSame(reactivePlanner, field(harness.reactive, "planner"));
        assertSame(jdbcPlanner, field(harness.jdbc.withMigrationObserver(SchemaMigrationObserver.noop()), "planner"));
        assertSame(reactivePlanner, field(harness.reactive.withMigrationObserver(SchemaMigrationObserver.noop()), "planner"));
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
            SchemaMigrationPlan review = harness.assertParity(target);
            assertEquals(exists, review.tableExists());
            assertEquals(1, review.requests().size());
            assertEquals(exists ? "alter table items add column note VARCHAR(80)"
                    : "create table items (id BIGINT not null, note VARCHAR(80), constraint pk_items primary key (id))",
                    review.requests().getFirst().sql());
            assertEquals(List.of("items", "items", "items", "items"), harness.metadata.tables);
        }
    }

    @Test
    void containsCreateAndExistingOwnerKeepAuxiliaryDdlAndInvalidationFacts() {
        DynamicForm target = protectedForm();
        String side = ProtectedContainsLayout.resolve(target).orElseThrow().table().table();
        for (boolean ownerExists : List.of(false, true)) {
            Harness harness = new Harness(ownerExists
                    ? Map.of(target.table(), ProtectedFormLayout.physical(target)) : Map.of(), Map.of());
            SchemaMigrationPlan review = harness.assertParity(target);
            assertEquals(ownerExists, review.tableExists());
            assertEquals(List.of(RelationIdentity.parse(side)), review.additionalCreatedTables());
            assertTrue(review.sqlTexts().stream()
                    .anyMatch(sql -> sql.startsWith("create table " + side + " (")));
            assertEquals(ownerExists ? List.of(target.table(), side, target.table(), side,
                    target.table(), side, target.table(), side)
                    : List.of(target.table(), target.table(), target.table(), target.table()),
                    harness.metadata.tables);
        }
    }

    @Test
    void existingContainsTableIsInvalidatedWhenIndexesAreAdded() {
        DynamicForm target = protectedForm();
        DynamicForm side = ProtectedContainsLayout.resolve(target).orElseThrow().table();
        Harness harness = new Harness(Map.of(target.table(), ProtectedFormLayout.physical(target),
                side.table(), side), Map.of());
        SchemaMigrationPlan jdbc = harness.jdbc.planCreateOrAlter(
                target, List.of(), List.of(), harness.jdbcReader,
                SchemaMigrationOptions.safe());
        SchemaMigrationPlan reactive = harness.reactive.planCreateOrAlter(
                target, List.of(), List.of(), harness.reactiveReader,
                SchemaMigrationOptions.safe()).block();

        assertNotNull(reactive);
        assertPlan(jdbc, reactive);
        assertTrue(jdbc.additionalCreatedTables().contains(side.relationIdentity()));
        assertFalse(jdbc.requests().isEmpty(), "missing side indexes must be planned");
    }

    @Test
    void onlyTheExistingMissingTableProtocolTriggersCreate() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        for (String message : List.of("table metadata not found", "table metadata not found: items")) {
            Harness harness = new Harness(Map.of(),
                    Map.of(target.table(), new IllegalArgumentException(message)));
            assertFalse(harness.assertParity(target).tableExists());
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
        IndexDefinition firstIndex = IndexDefinition.builder("ix_items_first")
                .addColumn("first_parent").build();
        IndexDefinition secondIndex = IndexDefinition.builder("ix_items_second")
                .addColumn("second_parent").build();
        ForeignKeyDefinition firstForeignKey = foreignKey(
                "fk_items_first", "first_parent", "first_parents");
        ForeignKeyDefinition secondForeignKey = foreignKey(
                "fk_items_second", "second_parent", "second_parents");

        for (boolean exists : List.of(false, true)) {
            Harness harness = new Harness(exists ? Map.of(target.table(), target) : Map.of(), Map.of());
            List<IndexDefinition> indexes = new ArrayList<>(List.of(firstIndex));
            List<ForeignKeyDefinition> foreignKeys = new ArrayList<>(List.of(firstForeignKey));
            Mono<SchemaMigrationPlan> plan = harness.reactive.planCreateOrAlter(
                    target, indexes, foreignKeys, harness.reactiveReader, SchemaMigrationOptions.safe());
            Mono<SchemaMigrationPlan> review = harness.reactive.planCreateOrAlter(
                    target, indexes, foreignKeys, harness.reactiveReader,
                    SchemaMigrationOptions.safe());

            assertTrue(harness.metadata.tables.isEmpty());
            indexes.set(0, secondIndex);
            foreignKeys.set(0, secondForeignKey);

            SchemaMigrationPlan planned = plan.block();
            SchemaMigrationPlan reviewed = review.block();
            assertNotNull(planned);
            assertNotNull(reviewed);
            assertEquals(List.of("ix_items_first"),
                    planned.target().indexes().stream().map(IndexDefinition::name).toList());
            assertEquals(List.of("fk_items_first"),
                    planned.target().foreignKeys().stream().map(ForeignKeyDefinition::name).toList());
            assertEquals(List.of("ix_items_first"), reviewed.target().indexes().stream()
                    .map(IndexDefinition::name).toList());
            assertEquals(List.of("fk_items_first"), reviewed.target().foreignKeys().stream()
                    .map(ForeignKeyDefinition::name).toList());
        }
    }

    private static ForeignKeyDefinition foreignKey(String name, String column, String referenceTable) {
        return ForeignKeyDefinition.builder(name)
                .addColumn(column)
                .reference(RelationIdentity.table(referenceTable))
                .addReferenceColumn("id")
                .build();
    }

    @Test
    void customFormDdlRendersEachPhysicalTableOnceAcrossPlanAndReview() {
        DynamicForm target = protectedForm();
        DynamicForm side = ProtectedContainsLayout.resolve(target).orElseThrow().table();
        for (int existing = 0; existing <= 2; existing++) {
            Map<String, DynamicForm> tables = new LinkedHashMap<>();
            if (existing > 0) tables.put(target.table(), ProtectedFormLayout.physical(target));
            if (existing > 1) tables.put(side.table(), side);
            List<String> calls = new ArrayList<>();
            SchemaDialect schema = RdbDialect.h2().schema()
                    .withFormRenderer((current, snapshot, plan) -> {
                        calls.add(plan.target().name());
                        return plan.requests().stream().map(request ->
                                new SqlRequest("/* extended */ " + request.sql(), request.parameters())).toList();
                    });
            Harness harness = new Harness(tables, Map.of(), RdbDialect.h2().withSchema(schema));
            SchemaMigrationPlan planned = harness.jdbc.planCreateOrAlter(target, List.of(), harness.jdbcReader);
            assertPlan(planned, harness.reactive.planCreateOrAlter(target, List.of(), harness.reactiveReader).block());
            SchemaMigrationPlan review = harness.jdbc.planCreateOrAlter(target, List.of(), List.of(),
                    harness.jdbcReader, SchemaMigrationOptions.safe());
            SchemaMigrationPlan reactive = harness.reactive.planCreateOrAlter(target, List.of(), List.of(),
                    harness.reactiveReader, SchemaMigrationOptions.safe()).block();
            assertNotNull(reactive);
            assertPlan(review, reactive);
            assertEquals(review.requests(), reactive.requests());
            assertEquals(planned.requests(), review.requests());
            assertTrue(review.additionalCreatedTables().contains(side.relationIdentity()));
            // JDBC/reactive plan and JDBC/reactive review each render a table once.
            assertEquals(existing == 0 ? 8 : 4, calls.size());
            assertEquals(4, calls.stream().filter(side.table()::equals).count());
            assertTrue(review.requests().stream()
                    .allMatch(request -> request.sql().startsWith("/* extended */ ")));
        }
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
        assertEquals(jdbc.target().name(), reactive.target().name());
        assertEquals(jdbc.target().columns(), reactive.target().columns());
        assertEquals(jdbc.target().indexes(), reactive.target().indexes());
        assertEquals(jdbc.target().foreignKeys(), reactive.target().foreignKeys());
        assertEquals(jdbc.tableExists(), reactive.tableExists());
        assertEquals(jdbc.requests(), reactive.requests());
        assertEquals(jdbc.skippedChanges(), reactive.skippedChanges());
        assertEquals(jdbc.additionalCreatedTables(), reactive.additionalCreatedTables());
    }

    private static final class Harness {
        private final MetadataExecutor metadata;
        private final JdbcSchemaClient jdbc;
        private final ReactiveSchemaClient reactive;
        private final JdbcFormMetadataReader jdbcReader;
        private final ReactiveFormMetadataReader reactiveReader;

        private Harness(Map<String, DynamicForm> tables, Map<String, RuntimeException> failures) {
            this(tables, failures, RdbDialect.h2());
        }

        private Harness(Map<String, DynamicForm> tables, Map<String, RuntimeException> failures,
                        RdbDialect dialect) {
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
            jdbc = JdbcSchemaClient.create(metadata, dialect);
            reactive = ReactiveSchemaClient.create(reactiveExecutor, dialect);
            jdbcReader = JdbcFormMetadataReaders.create(metadata, RdbDialect.h2());
            reactiveReader = ReactiveFormMetadataReaders.create(reactiveExecutor, RdbDialect.h2());
        }

        private SchemaMigrationPlan assertParity(DynamicForm target) {
            SchemaMigrationPlan plan = jdbc.planCreateOrAlter(target, List.of(), jdbcReader);
            int beforeSubscription = metadata.tables.size();
            Mono<SchemaMigrationPlan> pending = reactive.planCreateOrAlter(target, List.of(), reactiveReader);
            assertEquals(beforeSubscription, metadata.tables.size());
            assertPlan(plan, pending.block());
            assertTrue(metadata.tables.size() > beforeSubscription);
            SchemaMigrationPlan jdbcReview = jdbc.planCreateOrAlter(target, List.of(), List.of(),
                    jdbcReader, SchemaMigrationOptions.safe());
            SchemaMigrationPlan reactiveReview = reactive.planCreateOrAlter(target, List.of(), List.of(),
                    reactiveReader, SchemaMigrationOptions.safe()).block();
            assertNotNull(reactiveReview);
            assertPlan(plan, jdbcReview);
            assertPlan(jdbcReview, reactiveReview);
            assertEquals(jdbcReview.requests(), reactiveReview.requests());
            return jdbcReview;
        }

        private void assertFailure(DynamicForm target, RuntimeException expected) {
            assertSame(expected, assertThrows(RuntimeException.class,
                    () -> jdbc.planCreateOrAlter(target, List.of(), jdbcReader)));
            assertSame(expected, assertThrows(RuntimeException.class,
                    () -> reactive.planCreateOrAlter(target, List.of(), reactiveReader).block()));
            assertSame(expected, assertThrows(RuntimeException.class, () -> jdbc.planCreateOrAlter(
                    target, List.of(), List.of(), jdbcReader,
                    SchemaMigrationOptions.safe())));
            assertSame(expected, assertThrows(RuntimeException.class, () -> reactive.planCreateOrAlter(
                    target, List.of(), List.of(), reactiveReader,
                    SchemaMigrationOptions.safe()).block()));
        }
    }

    private static final class MetadataExecutor implements com.flying.orm.rdb.support.ListQueryFixture {
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
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("planning must not execute DDL");
        }
    }
}
