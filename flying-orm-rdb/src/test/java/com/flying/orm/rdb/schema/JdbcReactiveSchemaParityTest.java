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
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SqlRequest request = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                SchemaSnapshot.present(before), desired, request);
        AtomicReference<SchemaSnapshot> jdbcSnapshot =
                new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicReference<SchemaSnapshot> reactiveSnapshot =
                new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger jdbcInvalidations = new AtomicInteger();
        AtomicInteger reactiveInvalidations = new AtomicInteger();
        VerifiedSchemaPlanFixtures.SyncExecutor jdbc =
                new VerifiedSchemaPlanFixtures.SyncExecutor(ignored -> {
                    jdbcSnapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                });
        VerifiedSchemaPlanFixtures.ReactiveExecutor reactive =
                new VerifiedSchemaPlanFixtures.ReactiveExecutor(ignored -> Mono.fromSupplier(() -> {
                    reactiveSnapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                }));

        SchemaExecutionReport jdbcReport = VerifiedSchemaPlanExecutor.executeJdbc(
                plan, jdbc, jdbcSnapshot::get, SchemaSnapshotCoverage::complete,
                jdbcInvalidations::incrementAndGet,
                SqlExecutionOptions.safeDefaults());
        SchemaExecutionReport reactiveReport = VerifiedSchemaPlanExecutor.executeReactive(
                plan, reactive, () -> Mono.just(reactiveSnapshot.get()),
                SchemaSnapshotCoverage::complete,
                reactiveInvalidations::incrementAndGet,
                SqlExecutionOptions.safeDefaults()).block();

        assertNotNull(reactiveReport);
        assertEquals(jdbcReport.status(), reactiveReport.status());
        assertEquals(jdbcReport.steps().getFirst().status(),
                     reactiveReport.steps().getFirst().status());
        assertEquals(jdbcReport.steps().getFirst().rowsUpdated(),
                     reactiveReport.steps().getFirst().rowsUpdated());
        assertEquals(jdbcReport.verification().orElseThrow().status(),
                     reactiveReport.verification().orElseThrow().status());
        assertEquals(1, jdbcInvalidations.get());
        assertEquals(1, reactiveInvalidations.get());

        Method jdbcEntry = JdbcSchemaClient.class.getMethod(
                "executeReviewed", ReviewedSchemaPlan.class,
                JdbcFormMetadataReader.class, SchemaMigrationExecutionOptions.class);
        Method reactiveEntry = ReactiveSchemaClient.class.getMethod(
                "executeReviewed", ReviewedSchemaPlan.class,
                ReactiveFormMetadataReader.class, SchemaMigrationExecutionOptions.class);
        assertEquals(SchemaExecutionReport.class, jdbcEntry.getReturnType());
        assertEquals(Mono.class, reactiveEntry.getReturnType());
    }
}
