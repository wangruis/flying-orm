package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SchemaPartialAndUnknownOutcomeTest {

    @Test
    void reportsCompletedPrefixAndRedactsTheAmbiguousFailure() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition afterFirst = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID,
                VerifiedSchemaPlanFixtures.NOTE,
                VerifiedSchemaPlanFixtures.TAG);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot.get(),
                desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()),
                new SqlRequest("alter table accounts add column tag varchar(40)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    if (request.sql().contains("note")) {
                        snapshot.set(SchemaSnapshot.present(afterFirst));
                        return 1L;
                    }
                    throw new IllegalStateException(
                            "password=secret; alter table accounts add column tag varchar(40)");
                });

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan, executor, snapshot::get, SchemaSnapshotCoverage::complete, () -> {
                }, SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.PARTIAL, report.status());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().get(0).status());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().get(1).status());
        assertEquals("IllegalStateException",
                     report.steps().get(1).failureSummary().orElseThrow());
        assertFalse(report.steps().get(1).failureSummary().orElseThrow().contains("secret"));
    }

    @Test
    void reportsUnknownWhenTheFirstSentStatementHasNoCertainOutcome() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot snapshot = SchemaSnapshot.present(before);
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot, desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    throw new IllegalStateException("connection lost after send");
                });

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan, executor, () -> snapshot, SchemaSnapshotCoverage::complete, () -> {
                }, SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().getFirst().status());
        assertEquals(1, executor.requests.size());
    }
}
