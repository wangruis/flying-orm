package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaPostExecutionVerificationTest {

    @Test
    void rowsUpdatedCannotMasqueradeAsSchemaConvergence() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot unchanged = SchemaSnapshot.present(before);
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                unchanged, desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        AtomicInteger reads = new AtomicInteger();
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> 1L);

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                executor,
                () -> {
                    reads.incrementAndGet();
                    return unchanged;
                },
                SchemaSnapshotCoverage::complete,
                () -> {
                },
                SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.VERIFICATION_FAILED, report.status());
        assertFalse(report.successful());
        assertEquals(2, reads.get());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
        assertTrue(report.verification().isPresent());
        assertFalse(report.verification().orElseThrow().compatible());
    }
}
