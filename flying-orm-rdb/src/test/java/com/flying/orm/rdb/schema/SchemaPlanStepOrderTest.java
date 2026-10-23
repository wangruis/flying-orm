package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import reactor.core.publisher.Mono;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaPlanStepOrderTest {

    @Test
    void oneOperationOwnsItsSqlWithoutDuplicatingOperationFacts() {
        RdbDialect dialect = RdbDialect.postgresql();
        DatabaseDescriptor database = DatabaseDescriptor.of("PostgreSQL", "17.2", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition.builder("users")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").comment("identifier").build()).build();
        SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).planIncremental(
                database, desired, SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete());

        assertEquals(2, plan.requests().size());
        assertEquals(1, plan.operations().size());
        assertEquals(SchemaOperation.Kind.CREATE_TABLE, plan.operations().getFirst().kind());
        assertSame(plan.requests(), plan.requests());
        assertEquals(plan.requests(), plan.steps().getFirst().requests());

        RuntimeException cause = new IllegalStateException("comment DDL failed");
        SqlRequest second = plan.requests().getLast();
        for (boolean reactive : List.of(false, true)) {
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class, () -> {
                if (reactive) {
                    SchemaExecutionFixtures.executeReactive(plan, new SchemaExecutionFixtures.ReactiveExecutor(
                            request -> request == second ? Mono.error(cause) : Mono.just(0L)),
                            () -> { }, SqlExecutionOptions.unlimited()).block();
                } else {
                    SchemaExecutionFixtures.executeJdbc(plan, new SchemaExecutionFixtures.SyncExecutor(request -> {
                        if (request == second) throw cause;
                        return 0L;
                    }), () -> { }, SqlExecutionOptions.unlimited());
                }
            });
            assertSame(cause, failure.getCause());
            assertEquals(1, failure.stepIndex(), "failure index addresses SQL, not its parent operation");
            assertEquals(List.of(plan.requests().getFirst()),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void operationSqlIsCopiedAndCannotCarryParameters() {
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.ADD_COLUMN,
                RelationIdentity.table("users"), "id", null,
                ColumnDefinition.builder("id", "BIGINT").build(), SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        List<SqlRequest> requests = new ArrayList<>(List.of(new SqlRequest("alter table users add id bigint", List.of())));
        SchemaPlanStep step = new SchemaPlanStep(operation, requests);
        assertThrows(IllegalArgumentException.class, () -> new SchemaPlanStep(operation, List.of()));
        requests.clear();
        assertEquals(1, step.requests().size());
        assertThrows(UnsupportedOperationException.class, () -> step.requests().clear());
        assertThrows(IllegalArgumentException.class,
                () -> new SchemaPlanStep(operation, List.of(new SqlRequest("select ?", List.of(1)))));
    }

    @Test
    void preservesStepOrderAndExactSql() {
        DatabaseDescriptor database = DatabaseDescriptor.of(
                "PostgreSQL", "17.2", "postgresql", DialectCapabilities.empty());
        SchemaOperation firstOperation = SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE,
                RelationIdentity.table("users"), "users", null,
                RelationalTableDefinition.builder("users")
                        .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build(),
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        SchemaOperation secondOperation = addColumn("orders", "id");

        SchemaMigrationPlan first = SchemaMigrationPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .addStep(new SchemaPlanStep(firstOperation,
                        List.of(new SqlRequest("create table users(id bigint)", List.of()))))
                .addStep(new SchemaPlanStep(secondOperation, List.of(new SqlRequest("alter table orders add id bigint", List.of()))))
                .build();
        SchemaMigrationPlan same = SchemaMigrationPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .addStep(new SchemaPlanStep(firstOperation,
                        List.of(new SqlRequest("create table users(id bigint)", List.of()))))
                .addStep(new SchemaPlanStep(secondOperation, List.of(new SqlRequest("alter table orders add id bigint", List.of()))))
                .build();

        assertEquals(first.requests(), same.requests());
        assertEquals("create table users(id bigint)",
                     first.steps().getFirst().requests().getFirst().sql());

        SchemaMigrationPlan changedSql = SchemaMigrationPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .addStep(new SchemaPlanStep(firstOperation,
                        List.of(new SqlRequest("create table users(id integer)", List.of()))))
                .addStep(new SchemaPlanStep(secondOperation, List.of(new SqlRequest("alter table orders add id bigint", List.of()))))
                .build();

        assertNotEquals(first.requests(), changedSql.requests());
    }

    @Test
    void listOrderDefinesOperationOrderWithoutSeparateStepNumbers() {
        DatabaseDescriptor database = DatabaseDescriptor.of(
                "H2", "2.3", "h2", DialectCapabilities.empty());
        SchemaOperation first = addColumn("sample", "first");
        SchemaOperation second = addColumn("sample", "second");
        SchemaMigrationPlan plan = SchemaMigrationPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .addStep(new SchemaPlanStep(first, List.of(new SqlRequest("alter table sample add first bigint", List.of()))))
                .addStep(new SchemaPlanStep(second, List.of(new SqlRequest("alter table sample add second bigint", List.of())))).build();
        assertEquals(List.of(first, second), plan.operations());
        assertEquals(List.of("alter table sample add first bigint", "alter table sample add second bigint"),
                plan.sqlTexts());
    }

    private static SchemaOperation addColumn(String table, String name) {
        return SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                RelationIdentity.table(table),
                name,
                null,
                ColumnDefinition.builder(name, "BIGINT").build(),
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
    }
}
