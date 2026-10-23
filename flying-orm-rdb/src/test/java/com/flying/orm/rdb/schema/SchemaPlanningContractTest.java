package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.operator.SyncDdlOperator;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaPlanningContractTest {
    @Test
    void foreignKeyReplacementCanSurroundItsColumnTypeChangeForBothInputs() {
        ForeignKeyDefinition before = ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                .reference(RelationIdentity.table("parents")).addReferenceColumn("id").build();
        ForeignKeyDefinition after = ForeignKeyDefinition.builder("fk_parent").addColumn("parent_id")
                .reference(RelationIdentity.table("owners")).addReferenceColumn("id").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build()).addForeignKey(before).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("parent_id", "BIGINT")).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder("items")
                .addColumn(target.field("parent_id").column()).addForeignKey(after).build();
        List<String> expected = List.of("alter table \"items\" drop constraint \"fk_parent\"",
                "alter table \"items\" alter column \"parent_id\" type BIGINT",
                "alter table \"items\" add constraint \"fk_parent\" foreign key (\"parent_id\") references \"owners\" (\"id\")");

        assertEquals(expected, FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(current, target,
                List.of(), List.of(after), SchemaMigrationOptions.safe().allowColumnChange()).sqlTexts());
        assertEquals(expected, RelationalSchemaPlanReviewer.create(DIALECT).review(DATABASE, desired,
                SchemaSnapshot.present(current), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT)
                .sqlTexts());
    }

    @Test
    void indexRemovalHasTheSameStructuralMeaningForBothInputs() {
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "BIGINT")).build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(target.field("id").column())
                .addIndex(IndexDefinition.builder("ix_id").addColumn("id").build()).build();
        SchemaMigrationPlan ordinary = FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(
                current, target, List.of(), SchemaMigrationOptions.safe().allowDropIndex());
        SchemaMigrationPlan relational = RelationalSchemaPlanReviewer.create(DIALECT).review(
                DATABASE, target.toTableDefinition(), SchemaSnapshot.present(current),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);

        assertEquals(relational.sqlTexts(), ordinary.sqlTexts());
        assertEquals(1, ordinary.operations().size());
        assertEquals(relational.operations().getFirst().compatibility(),
                ordinary.operations().getFirst().compatibility());
    }

    @Test
    void unresolvedDependencyFailsPlanningInsteadOfPublishingAPartialSqlList() {
        ForeignKeyDefinition parent = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_id").reference(RelationIdentity.table("items"))
                .addReferenceColumn("id").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_items", "id")).addForeignKey(parent).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("parent_id", "BIGINT"))
                .addField(DynamicField.of("added", "VARCHAR").withLength(32)).build();

        assertThrows(UnsupportedOperationException.class, () -> FormSchemaSqlRenderer.create(DIALECT)
                .migrateSafelyPlan(current, target, List.of(), SchemaMigrationOptions.safe().allowColumnChange()));
        assertThrows(UnsupportedOperationException.class, () -> RelationalSchemaPlanReviewer.create(DIALECT)
                .planIncremental(DATABASE, target.toTableDefinition(), SchemaSnapshot.present(current),
                        SchemaSnapshotCoverage.complete()));
    }

    @Test
    void incompleteCoverageCannotPublishAnExecutablePlan() {
        RelationalTableDefinition target = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build();
        assertThrows(IllegalStateException.class, () -> RelationalSchemaPlanReviewer.create(DIALECT).review(
                DATABASE, target, SchemaSnapshot.absent(target.identity()), SchemaSnapshotCoverage.none(),
                SchemaCompatibilityMode.EXACT));
    }

    private static final RdbDialect DIALECT = RdbDialect.postgresql();
    private static final DatabaseDescriptor DATABASE = DatabaseDescriptor.of("PostgreSQL", "16", DIALECT);

    @Test
    void bothInputsKeepAnUnresolvedSelfReferenceOutOfExecutableSql() {
        ForeignKeyDefinition parent = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_code").reference(RelationIdentity.table("items"))
                .addReferenceColumn("code").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("code", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("parent_code", "INTEGER").build())
                .addUnique(UniqueConstraintDefinition.of("uk_code", "code"))
                .addForeignKey(parent).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("parent_code", "BIGINT")).build();
        assertThrows(UnsupportedOperationException.class, () -> FormSchemaSqlRenderer.create(DIALECT)
                .migrateSafelyPlan(current, target, List.of(), SchemaMigrationOptions.safe().allowColumnChange()));
        assertThrows(UnsupportedOperationException.class, () -> RelationalSchemaPlanReviewer.create(DIALECT)
                .planIncremental(DATABASE, target.toTableDefinition(), SchemaSnapshot.present(current),
                        SchemaSnapshotCoverage.complete()));
    }

    @Test
    void aRenameDoesNotHideTheExistingSelfReferenceFromColumnPlanning() {
        ForeignKeyDefinition parent = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_code").reference(RelationIdentity.table("items"))
                .addReferenceColumn("code").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("code", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("parent_code", "INTEGER").build())
                .addUnique(UniqueConstraintDefinition.of("uk_code", "code"))
                .addForeignKey(parent).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("parent", "BIGINT")).build();
        assertThrows(UnsupportedOperationException.class, () -> FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(current,
                target, List.of(), SchemaMigrationOptions.safe().allowColumnChange()
                        .renameColumn("parent_code", "parent")));
    }

    @Test
    void unchangedColumnsDoNotPreventOrdinaryIndexReplacement() {
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("code", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("other", "INTEGER").build())
                .addIndex(IndexDefinition.builder("ix_code").addColumn("code").build()).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("code", "INTEGER"))
                .addField(DynamicField.of("other", "INTEGER")).build();
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(current, target,
                List.of(IndexDefinition.builder("ix_code").addColumn("other").build()),
                SchemaMigrationOptions.safe().allowRebuildIndex());

        assertEquals(List.of("drop index \"ix_code\"", "create index \"ix_code\" on \"items\" (\"other\" asc)"),
                plan.sqlTexts());
    }

    @Test
    void aNewSelfReferenceIsAddedAfterTheColumnTypeChange() {
        ForeignKeyDefinition parent = ForeignKeyDefinition.builder("fk_parent")
                .addColumn("parent_code").reference(RelationIdentity.table("items"))
                .addReferenceColumn("code").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("code", "BIGINT").build())
                .addColumn(ColumnDefinition.builder("parent_code", "INTEGER").build())
                .addUnique(UniqueConstraintDefinition.of("uk_code", "code")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("parent_code", "BIGINT")).build();
        SchemaMigrationPlan ordinary = FormSchemaSqlRenderer.create(DIALECT).migrateSafelyPlan(current,
                target, List.of(), List.of(parent), SchemaMigrationOptions.safe().allowColumnChange());
        RelationalTableDefinition declared = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("code", "BIGINT").build())
                .addColumn(ColumnDefinition.builder("parent_code", "BIGINT").build())
                .addForeignKey(parent).build();
        SchemaMigrationPlan relational = RelationalSchemaPlanReviewer.create(DIALECT).planIncremental(
                DATABASE, declared, SchemaSnapshot.present(current), SchemaSnapshotCoverage.complete());

        for (SchemaMigrationPlan plan : List.of(ordinary, relational)) {
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_COLUMN, SchemaOperation.Kind.ADD_FOREIGN_KEY),
                    plan.operations().stream().map(SchemaOperation::kind).toList());
            assertEquals(List.of("alter table \"items\" alter column \"parent_code\" type BIGINT",
                    "alter table \"items\" add constraint \"fk_parent\" foreign key (\"parent_code\") "
                            + "references \"items\" (\"code\")"), plan.sqlTexts());
        }
    }

    @Test
    void bothInputsPublishTheSamePlanAndExecutionResult() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, DIALECT);
        JdbcFormMetadataReader reader = reader();
        SchemaMigrationPlan relational = client.planCreateOrAlter(DATABASE, target(true), reader);
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(com.flying.orm.core.form.DynamicField.of("new_value", "INTEGER")).build();
        SchemaMigrationPlan ordinary = client.planCreateOrAlter(form, List.of(), reader);
        assertEquals(List.of("alter table \"items\" add column \"new_value\" INTEGER"), ordinary.sqlTexts());
        assertEquals(ordinary.sqlTexts(), relational.sqlTexts());
        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN),
                ordinary.operations().stream().map(SchemaOperation::kind).toList());
        assertEquals(ordinary.operations().stream().map(SchemaOperation::kind).toList(),
                relational.operations().stream().map(SchemaOperation::kind).toList());
        assertTrue(ordinary.tableExists());
        assertTrue(relational.tableExists());
        assertFalse(ordinary.snapshotCoverage().isComplete(), "ordinary reader did not declare full relational coverage");
        for (SchemaMigrationPlan plan : List.of(ordinary, relational)) {
            SchemaMigrationResult result = client.execute(plan, reader);
            assertEquals(plan, result.plan());
            assertEquals(1, result.steps().size());
            assertEquals(0L, result.rowsUpdated());
        }
    }

    @Test
    void incrementalDeclarationKeepsUnmentionedColumnsAndIndexes() {
        RecordingExecutor executor = new RecordingExecutor();
        SyncDdlOperator ddl = SyncDdlOperator.create(JdbcSchemaClient.create(executor, DIALECT), reader());

        ddl.createOrAlter(DATABASE, target(false));

        assertEquals(List.of(), executor.sql);
    }

    @Test
    void incrementalAdditionDoesNotReorderOrDropExistingColumns() {
        RecordingExecutor executor = new RecordingExecutor();
        SyncDdlOperator ddl = SyncDdlOperator.create(JdbcSchemaClient.create(executor, DIALECT), reader());

        SchemaMigrationPlan plan = ddl.plan(DATABASE, target(true));
        assertTrue(executor.sql.isEmpty());
        ddl.execute(plan);

        assertEquals(List.of("alter table \"items\" add column \"new_value\" INTEGER"), executor.sql);
    }

    @Test
    void reactiveIncrementalPlanningReadsOncePerSubscriptionAndSharesTheSqlPath() {
        RecordingExecutor executor = new RecordingExecutor();
        AtomicInteger reads = new AtomicInteger();
        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, RelationIdentity relation) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            @Override
            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                String table = relation.qualifiedName();
                return Mono.fromSupplier(() -> {
                    reads.incrementAndGet();
                    return reader().readSnapshot(table);
                });
            }

            @Override
            public SchemaSnapshotCoverage snapshotCoverage() {
                return SchemaSnapshotCoverage.complete();
            }
        };
        ReactiveSqlExecutor reactiveExecutor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> executor.rowsUpdated(request));
            }
        };
        ReactiveSchemaClient client = ReactiveSchemaClient.create(reactiveExecutor, DIALECT);
        Mono<SchemaMigrationPlan> plan = client.planCreateOrAlter(DATABASE, target(true), reader);
        assertEquals(0, reads.get());
        assertNotNull(plan.flatMap(value -> client.execute(value, reader)).block());
        assertNotNull(plan.flatMap(value -> client.execute(value, reader)).block());
        assertEquals(2, reads.get());
        assertEquals(List.of("alter table \"items\" add column \"new_value\" INTEGER",
                "alter table \"items\" add column \"new_value\" INTEGER"), executor.sql);
    }

    @Test
    void incrementalDeclarationRetainsUnmentionedPrimaryKeyAndTableComment() {
        RelationalTableDefinition current = RelationalTableDefinition.builder("items")
                .comment("existing table")
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_items", "id"))
                .build();
        RelationalTableDefinition declared = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("extra", "INTEGER").build()).build();
        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(DIALECT).planIncremental(
                DATABASE, declared, SchemaSnapshot.present(current), SchemaSnapshotCoverage.complete());

        assertEquals(List.of(SchemaOperation.Kind.ADD_COLUMN), plan.operations().stream()
                .map(SchemaOperation::kind).toList());
        assertEquals(List.of("id"), plan.desiredTable().orElseThrow().primaryKey().orElseThrow().columns());
        assertEquals("existing table", plan.desiredTable().orElseThrow().comment());
    }

    @Test
    void mysqlIncrementalUniqueIndexDoesNotDuplicateTheObservedConstraint() {
        RdbDialect dialect = RdbDialect.mysql();
        ColumnDefinition id = ColumnDefinition.builder("id", "INTEGER").build();
        RelationalTableDefinition current = RelationalTableDefinition.builder("items").addColumn(id)
                .addUnique(UniqueConstraintDefinition.of("uk_id", "id")).build();
        RelationalTableDefinition declared = RelationalTableDefinition.builder("items").addColumn(id)
                .addIndex(IndexDefinition.builder("uk_id").unique().addColumn("id").build()).build();

        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).planIncremental(
                DatabaseDescriptor.of("MySQL", "8.4", dialect), declared,
                SchemaSnapshot.present(current), SchemaSnapshotCoverage.complete());

        assertTrue(plan.steps().isEmpty());
    }

    @Test
    void partialSnapshotDoesNotBecomeCompleteWhenIncrementalTargetIsMerged() {
        SchemaSnapshot partial = SchemaSnapshot.fromPartial(target(false));
        assertThrows(IllegalStateException.class, () -> RelationalSchemaPlanReviewer.create(DIALECT).planIncremental(
                DATABASE, target(true), partial, SchemaSnapshotCoverage.none()));
    }

    @Test
    void explicitReplacementStillRemovesUnmentionedColumn() {
        RecordingExecutor executor = new RecordingExecutor();
        JdbcFormMetadataReader reader = readerWithoutIndex();
        JdbcSchemaClient client = JdbcSchemaClient.create(executor, DIALECT);

        SyncDdlOperator ddl = SyncDdlOperator.create(client, reader);
        ddl.execute(ddl.planReplacement(DATABASE, target(false)));

        assertEquals(List.of("alter table \"items\" drop column \"extra\""), executor.sql);
    }

    private static RelationalTableDefinition target(boolean addColumn) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder("items");
        // An incremental declaration need not repeat the physical order of existing columns.
        if (addColumn) {
            builder.addColumn(ColumnDefinition.builder("new_value", "INTEGER").build());
        }
        return builder.addColumn(ColumnDefinition.builder("id", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("name", "VARCHAR").length(32).build()).build();
    }

    private static JdbcFormMetadataReader reader() {
        return reader(true);
    }

    private static JdbcFormMetadataReader readerWithoutIndex() {
        return reader(false);
    }

    private static JdbcFormMetadataReader reader(boolean index) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder("items")
                .addColumn(ColumnDefinition.builder("id", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("name", "VARCHAR").length(32).build())
                .addColumn(ColumnDefinition.builder("extra", "INTEGER").build());
        if (index) {
            builder.addIndex(IndexDefinition.builder("ix_extra").addColumn("extra").build());
        }
        SchemaSnapshot snapshot = SchemaSnapshot.present(builder.build());
        return new JdbcFormMetadataReader() {
            @Override
            public RelationalTableDefinition readTableForSchema(RelationIdentity relation) {
                String table = relation.qualifiedName();
                assertEquals("items", table);
                return snapshot.knownDefinition();
            }

            @Override
            public DynamicForm readForm(String formId, RelationIdentity relation) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            @Override
            public SchemaSnapshot readSnapshot(RelationIdentity relation) {
                String table = relation.qualifiedName();
                assertEquals("items", table);
                return snapshot;
            }

            @Override
            public SchemaSnapshotCoverage snapshotCoverage() {
                return SchemaSnapshotCoverage.complete();
            }
        };
    }

    private static final class RecordingExecutor implements com.flying.orm.rdb.support.ListQueryFixture {
        private final List<String> sql = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            sql.add(request.sql());
            return 0;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new UnsupportedOperationException();
        }
    }
}
