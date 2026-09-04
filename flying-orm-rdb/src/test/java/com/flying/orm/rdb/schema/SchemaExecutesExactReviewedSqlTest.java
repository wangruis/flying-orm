package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class SchemaExecutesExactReviewedSqlTest {

    @Test
    void sendsTheFrozenRequestInstanceWithoutReplanningOrRendering() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        SqlRequest reviewedRequest = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot.get(), desired, reviewedRequest);
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    snapshot.set(SchemaSnapshot.present(desired));
                    return 1L;
                });
        AtomicInteger invalidations = new AtomicInteger();

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                executor,
                snapshot::get,
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.SUCCESS, report.status());
        assertEquals(1, executor.requests.size());
        assertSame(reviewedRequest, executor.requests.getFirst());
        assertEquals(1, invalidations.get());
        assertEquals(1L, report.steps().getFirst().rowsUpdated().orElseThrow());
    }

    @Test
    void treatsAnUnavailableDdlUpdateCountAsSuccessfulZeroRows() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot.get(), desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    snapshot.set(SchemaSnapshot.present(desired));
                    return -1L;
                });

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                executor,
                snapshot::get,
                SchemaSnapshotCoverage::complete,
                () -> { },
                SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.SUCCESS, report.status());
        assertEquals(0L, report.steps().getFirst().rowsUpdated().orElseThrow());
    }
}
