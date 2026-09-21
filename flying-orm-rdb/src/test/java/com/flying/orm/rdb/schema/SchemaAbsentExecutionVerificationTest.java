package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaAbsentExecutionVerificationTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final RelationalTableDefinition PRESENT = RelationalTableDefinition.builder(ACCOUNTS)
            .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
            .build();
    private static final DatabaseDescriptor DATABASE = DatabaseDescriptor.of(
            "PostgreSQL", "17", RdbDialect.postgresql());

    @Test
    void jdbcAndReactiveRequireThePostExecutionTableStateToBeAbsent() {
        List<SchemaSnapshot> outcomes = List.of(
                SchemaSnapshot.absent(ACCOUNTS),
                SchemaSnapshot.unknown(ACCOUNTS),
                SchemaSnapshot.present(PRESENT));

        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.h2(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
            for (SchemaSnapshot outcome : outcomes) {
                SchemaExecutionStatus expected = outcome.tableState() == SchemaSnapshot.State.ABSENT
                        ? SchemaExecutionStatus.SUCCESS : SchemaExecutionStatus.VERIFICATION_FAILED;
                assertEquals(expected, executeJdbc(database, outcome).status());
                assertEquals(expected, executeReactive(database, outcome).status());
            }
        }
    }

    @Test
    void legacyPlansFailBeforeMetadataOrSql() {
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger executions = new AtomicInteger();
        ReviewedSchemaPlan legacy = ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredFingerprint("legacy-fingerprint-only")
                .actualFingerprint("actual")
                .build();

        SchemaExecutionReport legacyReport = VerifiedSchemaPlanExecutor.executeJdbc(
                legacy,
                syncExecutor(executions, new AtomicReference<>(SchemaSnapshot.absent(ACCOUNTS))),
                () -> {
                    reads.incrementAndGet();
                    return SchemaSnapshot.absent(ACCOUNTS);
                },
                SchemaSnapshotCoverage::complete,
                () -> { },
                SqlExecutionOptions.safeDefaults());
        assertEquals(SchemaExecutionStatus.FAILED, legacyReport.status());
        assertEquals(0, reads.get());
        assertEquals(0, executions.get());
    }

    @Test
    void explicitlyManualAbsentPlansStillExecuteNothing() {
        for (RdbDialect dialect : List.of(
                RdbDialect.h2(),
                RdbDialect.mysql(),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            DatabaseDescriptor database = DatabaseDescriptor.of(dialect.name(), "test", dialect);
            ReviewedSchemaPlan manual = manualAbsentPlan(database);
            AtomicInteger reads = new AtomicInteger();
            AtomicInteger executions = new AtomicInteger();
            SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                    manual,
                    syncExecutor(executions, new AtomicReference<>(SchemaSnapshot.present(PRESENT))),
                    () -> {
                        reads.incrementAndGet();
                        return SchemaSnapshot.present(PRESENT);
                    },
                    SchemaSnapshotCoverage::complete,
                    () -> { },
                    SqlExecutionOptions.safeDefaults());

            assertEquals(SchemaExecutionStatus.FAILED, report.status());
            assertEquals(0, reads.get());
            assertEquals(0, executions.get());
        }
    }

    private static SchemaExecutionReport executeJdbc(DatabaseDescriptor database, SchemaSnapshot outcome) {
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(PRESENT));
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ReviewedSchemaPlan plan = absentPlan(database, snapshot.get());

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                syncExecutor(executions, snapshot, outcome),
                snapshot::get,
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                approved(plan));

        assertEquals(1, executions.get());
        assertEquals(1, invalidations.get());
        assertFalse(report.verification().isEmpty());
        return report;
    }

    private static SchemaExecutionReport executeReactive(DatabaseDescriptor database, SchemaSnapshot outcome) {
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(PRESENT));
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ReviewedSchemaPlan plan = absentPlan(database, snapshot.get());
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("schema execution must not query through SQL executor"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.fromSupplier(() -> {
                    executions.incrementAndGet();
                    snapshot.set(outcome);
                    return 0L;
                });
            }
        };

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeReactive(
                plan,
                executor,
                () -> Mono.just(snapshot.get()),
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                approved(plan)).block();

        assertNotNull(report);
        assertEquals(1, executions.get());
        assertEquals(1, invalidations.get());
        assertFalse(report.verification().isEmpty());
        return report;
    }

    private static ReviewedSchemaPlan absentPlan(DatabaseDescriptor database,
                                                 SchemaSnapshot actual) {
        String actualFingerprint = SchemaSnapshotFingerprint.of(actual);
        SchemaOperation drop = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                ACCOUNTS,
                ACCOUNTS.table(),
                PRESENT,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint(SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(ACCOUNTS)))
                .actualFingerprint(actualFingerprint)
                .addStep(SchemaPlanStep.executable(
                        0,
                        drop,
                        new SqlRequest("drop table accounts", List.of()),
                        SchemaMigrationRiskLevel.CRITICAL,
                        List.of(SchemaPlanPrecondition.actualSnapshot(actualFingerprint))))
                .build();
    }

    private static ReviewedSchemaPlan manualAbsentPlan(DatabaseDescriptor database) {
        SchemaSnapshot actual = SchemaSnapshot.present(PRESENT);
        String actualFingerprint = SchemaSnapshotFingerprint.of(actual);
        SchemaOperation drop = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                ACCOUNTS,
                ACCOUNTS.table(),
                PRESENT,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return ReviewedSchemaPlan.builder(database)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint(SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(ACCOUNTS)))
                .actualFingerprint(actualFingerprint)
                .addStep(SchemaPlanStep.manual(
                        0, drop, SchemaMigrationRiskLevel.CRITICAL, List.of()))
                .build();
    }

    private static SchemaMigrationExecutionOptions approved(ReviewedSchemaPlan plan) {
        return new SchemaMigrationExecutionOptions(
                SqlExecutionOptions.safeDefaults(),
                SchemaMigrationApproval.approve(plan, "drop approved for test"));
    }

    private static SyncSqlExecutor syncExecutor(AtomicInteger executions,
                                                AtomicReference<SchemaSnapshot> snapshot) {
        return syncExecutor(executions, snapshot, snapshot.get());
    }

    private static SyncSqlExecutor syncExecutor(AtomicInteger executions,
                                                AtomicReference<SchemaSnapshot> snapshot,
                                                SchemaSnapshot outcome) {
        return new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("schema execution must not query through SQL executor");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                executions.incrementAndGet();
                snapshot.set(outcome);
                return 0L;
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                           SqlExecutionOptions options) {
                throw new AssertionError("schema DDL must not request generated keys");
            }
        };
    }
}
