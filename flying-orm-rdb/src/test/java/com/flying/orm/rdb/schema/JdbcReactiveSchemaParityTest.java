package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReader;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JdbcReactiveSchemaParityTest {

    @Test
    void jdbcAndReactiveExecutionPublishTheSameVerifiedFacts() throws Exception {
        RelationalTableDefinition before = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID);
        RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        SqlRequest request = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        SchemaMigrationPlan plan = SchemaExecutionFixtures.plan(
                SchemaSnapshot.present(before), desired, request);
        AtomicReference<SchemaSnapshot> jdbcSnapshot =
                new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicReference<SchemaSnapshot> reactiveSnapshot =
                new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger jdbcInvalidations = new AtomicInteger();
        AtomicInteger reactiveInvalidations = new AtomicInteger();
        SchemaExecutionFixtures.SyncExecutor jdbc =
                new SchemaExecutionFixtures.SyncExecutor(ignored -> {
                    jdbcSnapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                });
        SchemaExecutionFixtures.ReactiveExecutor reactive =
                new SchemaExecutionFixtures.ReactiveExecutor(ignored -> Mono.fromSupplier(() -> {
                    reactiveSnapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                }));

        SchemaMigrationResult jdbcReport = SchemaExecutionFixtures.executeJdbc(
                plan, jdbc,
                jdbcInvalidations::incrementAndGet,
                SqlExecutionOptions.unlimited());
        SchemaMigrationResult reactiveReport = SchemaExecutionFixtures.executeReactive(
                plan, reactive,
                reactiveInvalidations::incrementAndGet,
                SqlExecutionOptions.unlimited()).block();

        assertNotNull(reactiveReport);
        assertEquals(List.of(request), jdbcReport.steps().stream().map(step -> step.request()).toList());
        assertEquals(List.of(request), reactiveReport.steps().stream().map(step -> step.request()).toList());
        assertEquals(1L, jdbcReport.rowsUpdated());
        assertEquals(1L, reactiveReport.rowsUpdated());
        assertEquals(1, jdbcInvalidations.get());
        assertEquals(1, reactiveInvalidations.get());

        Method jdbcEntry = JdbcSchemaClient.class.getMethod(
                "execute", SchemaMigrationPlan.class,
                JdbcFormMetadataReader.class, SqlExecutionOptions.class);
        Method reactiveEntry = ReactiveSchemaClient.class.getMethod(
                "execute", SchemaMigrationPlan.class,
                ReactiveFormMetadataReader.class, SqlExecutionOptions.class);
        assertEquals(SchemaMigrationResult.class, jdbcEntry.getReturnType());
        assertEquals(Mono.class, reactiveEntry.getReturnType());
    }
}
