package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaDirectExecutionTest {

    @Test
    void detailedCreateOrAlterUsesPlanExecutionAndPublishesCompletedSql() {
        DynamicForm source = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DynamicForm partial = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.of("note", "VARCHAR").withLength(80))
                .addField(DynamicField.of("tag", "VARCHAR").withLength(40)).build();
        DynamicForm complete = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR").withLength(80))
                .addField(DynamicField.of("tag", "VARCHAR").withLength(40)).build();
        for (DynamicForm target : List.of(partial, complete)) {
            for (boolean reactive : List.of(false, true)) {
                List<SqlRequest> executed = new ArrayList<>();
                List<SchemaMigrationObservation> observations = new ArrayList<>();
                AtomicInteger invalidated = new AtomicInteger();
                SchemaMigrationResult result = detailed(reactive, source, target, executed, observations, invalidated);
                assertEquals(2, result.steps().size());
                assertEquals(result.plan().requests(), result.steps().stream().map(step -> step.request()).toList());
                assertEquals(result.plan().requests(), executed);
                assertEquals(2L, result.rowsUpdated());
                assertEquals(1, invalidated.get());
                assertEquals(1, observations.size());
                assertEquals(2, observations.getFirst().completedSteps());
            }
        }
    }

    @Test
    void detailedCreateOrAlterDoesNotBypassUnresolvedPrimaryKeyChanges() {
        DynamicForm source = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        DynamicForm target = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("note", "VARCHAR").withLength(80)).build();
        for (boolean reactive : List.of(false, true)) {
            List<SqlRequest> executed = new ArrayList<>();
            AtomicInteger invalidated = new AtomicInteger();
            assertThrows(SchemaMigrationRejectedException.class, () ->
                    detailed(reactive, source, target, executed, new ArrayList<>(), invalidated));
            assertEquals(List.of(), executed);
            assertEquals(0, invalidated.get());
        }
    }

    private static SchemaMigrationResult detailed(boolean reactive, DynamicForm source, DynamicForm target,
                                                   List<SqlRequest> executed,
                                                   List<SchemaMigrationObservation> observations,
                                                   AtomicInteger invalidated) {
        java.util.function.Function<SqlRequest, Long> work = request -> { executed.add(request); return 1L; };
        if (reactive) {
            ReactiveFormMetadataReader reader = new ReactiveFormMetadataReader() {
            public Mono<DynamicForm> readForm(String id, RelationIdentity relation) {
                return Mono.just(source);
            }

            @Override
            public Mono<RelationalTableDefinition> readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).map(form -> form.toTableDefinition(relation));
            }

                public void invalidate(RelationIdentity relation) { invalidated.incrementAndGet(); }
            };
            return ReactiveSchemaClient.create(new SchemaExecutionFixtures.ReactiveExecutor(
                    request -> Mono.fromSupplier(() -> work.apply(request))), RdbDialect.postgresql())
                    .withMigrationObserver(observations::add)
                    .createOrAlterDetailed(target, List.of(), reader).block();
        }
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
            public DynamicForm readForm(String id, RelationIdentity relation) {
                return source;
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            public void invalidate(RelationIdentity relation) { invalidated.incrementAndGet(); }
        };
        return JdbcSchemaClient.create(new SchemaExecutionFixtures.SyncExecutor(work), RdbDialect.postgresql())
                .withMigrationObserver(observations::add).createOrAlterDetailed(target, List.of(), reader);
    }

    @Test
    void jdbcClientExecutesTheSqlWithoutReadingSnapshots() {
        SchemaMigrationPlan plan = plan();
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        SchemaExecutionFixtures.SyncExecutor jdbc = new SchemaExecutionFixtures.SyncExecutor(request -> 0L);
        JdbcFormMetadataReader reader = new JdbcFormMetadataReader() {
            @Override
            public DynamicForm readForm(String formId, RelationIdentity relation) {
                throw new AssertionError("execution must not read metadata");
            }

            @Override
            public RelationalTableDefinition readTable(RelationIdentity relation) {
                return readForm(relation.table(), relation).toTableDefinition(relation);
            }

            @Override
            public SchemaSnapshot readSnapshot(RelationIdentity relation) {
                reads.incrementAndGet();
                return SchemaSnapshot.absent(SchemaExecutionFixtures.ACCOUNTS);
            }

            @Override
            public void invalidate(RelationIdentity relation) {
                assertEquals(RelationIdentity.table("accounts"), relation);
                invalidations.incrementAndGet();
            }
        };
        JdbcSchemaClient.create(jdbc, RdbDialect.h2()).execute(plan, reader);

        assertEquals(plan.requests(), jdbc.requests);
        assertEquals(0, reads.get());
        assertEquals(1, invalidations.get());
    }

    @Test
    void propagatesFailureWithCompletedSqlInsteadOfReturningAnUnknownStatus() {
        for (int mode = 0; mode < 3; mode++) {
            SchemaMigrationPlan plan = plan();
            AtomicInteger calls = new AtomicInteger();
            AtomicInteger invalidations = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("second DDL failed");
            java.util.function.Function<SqlRequest, Long> work = request -> {
                if (calls.incrementAndGet() == 2) {
                    throw failure;
                }
                return 0L;
            };
            int executionMode = mode;
            SqlExecutionSequenceException thrown = assertThrows(SqlExecutionSequenceException.class, () -> {
                if (executionMode > 0) {
                    SchemaExecutionFixtures.executeReactive(plan,
                            new SchemaExecutionFixtures.ReactiveExecutor(request -> executionMode == 1
                                    ? Mono.fromSupplier(() -> work.apply(request)) : Mono.just(work.apply(request))),
                            invalidations::incrementAndGet, SqlExecutionOptions.unlimited()).block();
                } else {
                    SchemaExecutionFixtures.executeJdbc(plan, new SchemaExecutionFixtures.SyncExecutor(work),
                            invalidations::incrementAndGet, SqlExecutionOptions.unlimited());
                }
            });
            assertSame(failure, thrown.getCause());
            assertEquals(1, thrown.stepIndex());
            assertEquals(List.of(plan.requests().getFirst()),
                    thrown.completedWorkSteps().stream().map(step -> step.request()).toList());
            assertEquals(2, calls.get());
            assertEquals(1, invalidations.get());
        }
    }

    private static SchemaMigrationPlan plan() {
        return SchemaExecutionFixtures.plan(
                SchemaSnapshot.present(SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID)),
                SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID,
                        SchemaExecutionFixtures.NOTE, SchemaExecutionFixtures.TAG),
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()),
                new SqlRequest("alter table accounts add column tag varchar(40)", List.of()));
    }
}
