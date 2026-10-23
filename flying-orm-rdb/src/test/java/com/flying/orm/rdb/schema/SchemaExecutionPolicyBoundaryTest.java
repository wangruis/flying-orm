package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaExecutionPolicyBoundaryTest {

    @Test
    void ordinaryPlansReportCleanupFailureWithAllCompletedStatements() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        SqlRequest request = new SqlRequest("alter table items add note varchar(80)", List.of());
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true, List.of(request), List.of());
        RuntimeException cause = new IllegalStateException("invalidation failed");
        for (boolean reactive : List.of(false, true)) {
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class, () -> {
                if (reactive) {
                    new SchemaMigrationExecutor(new SchemaExecutionFixtures.ReactiveExecutor(sql -> Mono.just(0L)),
                            SchemaMigrationObserver.noop())
                            .execute(plan, List.of("items"), ignored -> { throw cause; },
                                    SqlExecutionOptions.unlimited()).block();
                } else {
                    new JdbcSchemaMigrationExecutor(new SchemaExecutionFixtures.SyncExecutor(sql -> 0L),
                            SchemaMigrationObserver.noop())
                            .execute(plan, List.of("items"), ignored -> { throw cause; }, SqlExecutionOptions.unlimited());
                }
            });
            assertSame(cause, failure.getCause().getCause());
            assertEquals(SqlExecutionPhase.CLEANUP, failure.phase());
            assertEquals(1, failure.stepIndex());
            assertEquals(List.of(request),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void ordinaryPlansPublishTheSamePartialExecutionFailureInBothModes() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        List<SqlRequest> requests = List.of(
                new SqlRequest("alter table items add note varchar(80)", List.of()),
                new SqlRequest("alter table items add tag varchar(40)", List.of()));
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true, requests, List.of());
        RuntimeException cause = new IllegalStateException("second DDL failed");
        for (boolean reactive : List.of(false, true)) {
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class, () -> {
                if (reactive) {
                    new SchemaMigrationExecutor(new SchemaExecutionFixtures.ReactiveExecutor(request ->
                            request == requests.getLast() ? Mono.error(cause) : Mono.just(1L)),
                            SchemaMigrationObserver.noop())
                            .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited()).block();
                } else {
                    new JdbcSchemaMigrationExecutor(new SchemaExecutionFixtures.SyncExecutor(request -> {
                        if (request == requests.getLast()) throw cause;
                        return 1L;
                    }), SchemaMigrationObserver.noop())
                            .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited());
                }
            });
            assertSame(cause, failure.getCause());
            assertEquals(SqlExecutionPhase.WORK, failure.phase());
            assertEquals(1, failure.stepIndex());
            assertEquals(List.of(requests.getFirst()),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void jdbcAndReactivePlanningShareExplicitConcurrentIndexOptions() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        com.flying.orm.rdb.metadata.JdbcFormMetadataReader jdbcReader =
                new com.flying.orm.rdb.metadata.JdbcFormMetadataReader() {
            public DynamicForm readForm(String id, RelationIdentity relation) {
                return form;
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

                };
        ReactiveFormMetadataReader reactiveReader = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(form);
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

        };
        SchemaExecutionFixtures.SyncExecutor sync = new SchemaExecutionFixtures.SyncExecutor(request -> {
            throw new AssertionError("planning must not execute DDL");
        });
        SchemaExecutionFixtures.ReactiveExecutor async = new SchemaExecutionFixtures.ReactiveExecutor(request -> {
            throw new AssertionError("planning must not execute DDL");
        });
        JdbcSchemaClient jdbc = JdbcSchemaClient.create(sync, RdbDialect.postgresql());
        ReactiveSchemaClient reactive = ReactiveSchemaClient.create(async, RdbDialect.postgresql());
        List<com.flying.orm.core.metadata.IndexDefinition> indexes = List.of(
                com.flying.orm.core.metadata.IndexDefinition.builder("ix_items_id").addColumn("id").build());

        for (boolean concurrent : List.of(false, true)) {
            SchemaMigrationOptions options = concurrent
                    ? SchemaMigrationOptions.safe().preferConcurrentIndexes().allowDropIndex()
                    : SchemaMigrationOptions.safe();
            SchemaMigrationPlan jdbcPlan = jdbc.planCreateOrAlter(form, indexes, jdbcReader, options);
            SchemaMigrationPlan reactivePlan = reactive.planCreateOrAlter(form, indexes, reactiveReader, options).block();
            String sql = "create index " + (concurrent ? "concurrently " : "")
                    + "\"ix_items_id\" on \"items\" (\"id\" asc)";
            assertEquals(List.of(sql), jdbcPlan.sqlTexts());
            assertEquals(List.of(sql), reactivePlan.sqlTexts());
            assertEquals(List.of(sql), jdbcPlan.steps().stream().flatMap(step -> step.requests().stream())
                    .map(SqlRequest::sql).toList());
            assertEquals(List.of(sql), reactivePlan.steps().stream().flatMap(step -> step.requests().stream())
                    .map(SqlRequest::sql).toList());
        }
        assertEquals(List.of(), sync.requests);
        assertEquals(List.of(), async.requests);
    }

    @Test
    void executesExplicitDdlWithoutInferringAnApplicationLockingPolicy() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                "alter table items add column note varchar(80)", List.of());
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true, List.of(request), List.of());
        SchemaExecutionFixtures.SyncExecutor jdbc = new SchemaExecutionFixtures.SyncExecutor(sql -> 0L);
        SchemaExecutionFixtures.ReactiveExecutor reactive =
                new SchemaExecutionFixtures.ReactiveExecutor(sql -> Mono.just(0L));

        new JdbcSchemaMigrationExecutor(jdbc, SchemaMigrationObserver.noop())
                .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited());
        new SchemaMigrationExecutor(reactive, SchemaMigrationObserver.noop())
                .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited()).block();

        assertEquals(List.of(request), jdbc.requests);
        assertEquals(List.of(request), reactive.requests);
    }

    @Test
    void executionUsesPlannedSqlWithoutSerializingAdvisoryDetails() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        com.flying.orm.core.sql.render.SqlRequest request = new com.flying.orm.core.sql.render.SqlRequest(
                "alter table items add column note varchar(80)", List.of());
        SkippedSchemaChange advisory = new SkippedSchemaChange(SkippedSchemaChange.Kind.DROP_INDEX,
                "legacy_index", "not requested", Map.of("applicationContext", new Object()));
        SchemaMigrationPlan plan = new SchemaMigrationPlan(form.toTableDefinition(), true, List.of(request), List.of(advisory));
        SchemaExecutionFixtures.SyncExecutor jdbc = new SchemaExecutionFixtures.SyncExecutor(sql -> 0L);
        SchemaExecutionFixtures.ReactiveExecutor reactive =
                new SchemaExecutionFixtures.ReactiveExecutor(sql -> Mono.just(0L));

        SchemaMigrationResult sync = new JdbcSchemaMigrationExecutor(jdbc, SchemaMigrationObserver.noop())
                .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited());
        SchemaMigrationResult async = new SchemaMigrationExecutor(reactive, SchemaMigrationObserver.noop())
                .execute(plan, List.of("items"), ignored -> { }, SqlExecutionOptions.unlimited()).block();

        assertEquals(List.of(request), jdbc.requests);
        assertEquals(List.of(request), reactive.requests);
        assertEquals(1, sync.steps().size());
        assertEquals(1, async.steps().size());
    }

    @Test
    void skippedChangesContainFactsButNoApplicationRecoveryWorkflow() {
        assertEquals(List.of("kind", "name", "reason", "details"),
                java.util.Arrays.stream(SkippedSchemaChange.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getName).toList());
    }

    @Test
    void migrationOptionsOnlyExposeImplementedSelections() {
        assertEquals(List.of("dropColumnAllowed", "columnChangeAllowed", "dropIndexAllowed",
                        "rebuildIndexAllowed", "columnRenames", "concurrentIndexesPreferred"),
                java.util.Arrays.stream(SchemaMigrationOptions.class.getRecordComponents())
                        .map(java.lang.reflect.RecordComponent::getName).toList());
    }

    @Test
    void forwardDropDoesNotReadMetadataForReverseRecovery() {
        DynamicForm source = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER"))
                .addField(DynamicField.of("obsolete", "INTEGER")).build();
        DynamicForm target = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("id", "INTEGER")).build();
        ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(source);
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

            public Mono<SchemaSnapshot> readSnapshot(RelationIdentity relation) {
                return Mono.error(new AssertionError("forward PostgreSQL DROP needs no recovery snapshot"));
            }
        };
        SchemaMigrationPlan plan = FormSchemaSqlRenderer.create(RdbDialect.postgresql())
                .migrationPlanner().plan(target, List.of(), List.of(), reader,
                        SchemaMigrationOptions.safe().allowDropColumn()).block();

        assertEquals(List.of("alter table \"items\" drop column \"obsolete\""),
                plan.requestsForExecution().stream().map(request -> request.sql()).toList());
    }

    @Test
    void explicitDropExecutesWithoutAnOrmApprovalWorkflow() {
        RdbDialect dialect = RdbDialect.postgresql();
        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).reviewAbsent(
                DatabaseDescriptor.of("PostgreSQL", "17", dialect), SchemaExecutionFixtures.ACCOUNTS,
                SchemaSnapshot.present(SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID)),
                SchemaSnapshotCoverage.complete());
        SchemaExecutionFixtures.SyncExecutor jdbc = new SchemaExecutionFixtures.SyncExecutor(request -> 0L);
        SchemaExecutionFixtures.ReactiveExecutor reactive =
                new SchemaExecutionFixtures.ReactiveExecutor(request -> Mono.just(0L));

        SchemaExecutionFixtures.executeJdbc(plan, jdbc, () -> { }, SqlExecutionOptions.unlimited());
        SchemaExecutionFixtures.executeReactive(plan, reactive, () -> { }, SqlExecutionOptions.unlimited()).block();

        assertEquals(List.of("drop table \"accounts\""), jdbc.requests.stream().map(request -> request.sql()).toList());
        assertEquals(jdbc.requests, reactive.requests);
    }
}
