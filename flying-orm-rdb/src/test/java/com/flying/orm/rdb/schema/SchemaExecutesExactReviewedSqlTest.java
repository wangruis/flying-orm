package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchemaExecutesExactReviewedSqlTest {

    @Test
    void sendsTheFrozenRequestInstanceWithoutReplanningOrRendering() {
        RelationalTableDefinition before = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID);
        RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        SqlRequest reviewedRequest = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        SchemaMigrationPlan plan = SchemaExecutionFixtures.plan(
                snapshot.get(), desired, reviewedRequest);
        SchemaExecutionFixtures.SyncExecutor executor =
                new SchemaExecutionFixtures.SyncExecutor(request -> {
                    snapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                });
        AtomicInteger invalidations = new AtomicInteger();

        SchemaMigrationResult report = SchemaExecutionFixtures.executeJdbc(
                plan,
                executor,
                invalidations::incrementAndGet,
                SqlExecutionOptions.unlimited());

        assertNotNull(report);
        assertEquals(1, executor.requests.size());
        assertSame(reviewedRequest, executor.requests.getFirst());
        assertEquals(1, invalidations.get());
        assertEquals(1L, report.steps().getFirst().rowsUpdated());
    }

    @Test
    void treatsAnUnavailableDdlUpdateCountAsSuccessfulZeroRows() {
        RelationalTableDefinition before = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID);
        RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        SchemaMigrationPlan plan = SchemaExecutionFixtures.plan(
                snapshot.get(), desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        SchemaExecutionFixtures.SyncExecutor executor =
                new SchemaExecutionFixtures.SyncExecutor(request -> {
                    snapshot.set(SchemaSnapshot.present(desired));
                    return -1L;
                });

        SchemaMigrationResult report = SchemaExecutionFixtures.executeJdbc(
                plan,
                executor,
                () -> { },
                SqlExecutionOptions.unlimited());

        assertNotNull(report);
        assertEquals(0L, report.steps().getFirst().rowsUpdated());
    }
}
