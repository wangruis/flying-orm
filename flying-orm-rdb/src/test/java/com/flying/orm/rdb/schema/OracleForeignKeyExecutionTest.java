package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleForeignKeyExecutionTest {

    private static final RdbDialect DIALECT = RdbDialect.oracle();
    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");
    private static final RelationIdentity PARENT = RelationIdentity.of(null, "app", "parents");

    @Test
    void publicReviewFreezesOracleForeignKeyReplacementAndValidation() {
        ReviewedSchemaPlan plan = plan();
        assertFalse(plan.requiresManualAction(), "Oracle action changes need an executable approved path");
        assertTrue(plan.requiresWritesQuiesced());
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"fk_accounts\"",
                "alter table \"app\".\"accounts\" add constraint \"fk_accounts\" foreign key (\"parent_id\")"
                        + " references \"app\".\"parents\" (\"id\") on delete cascade enable validate"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void ordinaryApprovalCannotAssertWritesQuiescedByItsReasonText() {
        ReviewedSchemaPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            for (SchemaMigrationApproval approval : List.of(
                    SchemaMigrationApproval.approve(plan, "writes are quiesced"),
                    new SchemaMigrationApproval(plan.fingerprint(), "writes are quiesced"))) {
                assertFalse(approval.writesQuiesced());
                Harness database = new Harness();
                assertEquals(SchemaExecutionStatus.FAILED, database.execute(plan, approval, reactive).status());
                assertEquals(0, database.reads);
                assertTrue(database.sent.isEmpty());
            }
            Harness database = new Harness();
            assertEquals(SchemaExecutionStatus.FAILED, database.execute(plan, null, reactive).status());
            assertEquals(0, database.reads);
            assertTrue(database.sent.isEmpty());
        }
    }

    @Test
    void explicitApprovalExecutesFrozenStepsAndRequiresMatchingReadbackOnBothPorts() {
        ReviewedSchemaPlan plan = plan();
        SchemaMigrationApproval approval = SchemaMigrationApproval.approveWithWritesQuiesced(
                plan, "isolated contract fixture has no writers");
        assertTrue(approval.writesQuiesced());
        for (boolean reactive : List.of(false, true)) {
            Harness database = new Harness();
            SchemaExecutionReport report = database.execute(plan, approval, reactive);
            assertEquals(SchemaExecutionStatus.SUCCESS, report.status());
            assertTrue(report.verification().orElseThrow().compatible());
            assertEquals(plan.requests(), database.sent);
            assertEquals(2, database.reads);
            assertEquals(1, database.invalidations);
            assertTrue(report.steps().stream().allMatch(step -> step.status() == SchemaExecutionStatus.SUCCESS));
        }
    }

    @Test
    void quiescenceDoesNotBypassExactApprovalOrTheActualSchemaFingerprint() {
        ReviewedSchemaPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            Harness staleApproval = new Harness();
            SchemaExecutionReport rejected = staleApproval.execute(plan,
                    new SchemaMigrationApproval("another-plan", "isolated fixture", true), reactive);
            assertEquals(SchemaExecutionStatus.FAILED, rejected.status());
            assertEquals(0, staleApproval.reads);
            assertTrue(staleApproval.sent.isEmpty());

            Harness drifted = new Harness();
            drifted.snapshot = SchemaSnapshot.present(table(null));
            SchemaExecutionReport drift = drifted.execute(plan,
                    SchemaMigrationApproval.approveWithWritesQuiesced(plan, "isolated fixture"), reactive);
            assertEquals(SchemaExecutionStatus.PRECONDITION_FAILED, drift.status());
            assertEquals(1, drifted.reads);
            assertTrue(drifted.sent.isEmpty());
        }
    }

    @Test
    void ddlCompletionAloneDoesNotSatisfyTheTargetRelationship() {
        ReviewedSchemaPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            Harness database = new Harness();
            database.postExecutionSnapshot = SchemaSnapshot.present(table(ReferentialAction.NO_ACTION));
            SchemaExecutionReport report = database.execute(plan,
                    SchemaMigrationApproval.approveWithWritesQuiesced(plan, "isolated fixture"), reactive);
            assertEquals(SchemaExecutionStatus.VERIFICATION_FAILED, report.status());
            assertFalse(report.successful());
            assertEquals(plan.requests(), database.sent);
        }
    }

    @Test
    void failedAddKeepsTheCommittedDropEvidenceWithoutRetryOrCompensation() {
        ReviewedSchemaPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            Harness database = new Harness();
            database.failAdd = true;
            SchemaExecutionReport report = database.execute(plan,
                    SchemaMigrationApproval.approveWithWritesQuiesced(plan, "isolated fixture"), reactive);
            assertEquals(SchemaExecutionStatus.PARTIAL, report.status());
            assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
            assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().get(1).status());
            assertTrue(report.steps().get(1).sqlSent());
            assertFalse(report.successful());
            assertEquals(plan.requests(), database.sent, "no automatic restore or retry DDL");
            assertEquals(2, database.reads);
            assertEquals(1, database.invalidations);
        }
    }

    @Test
    void anUnsupportedOracleUpdateActionStillRequiresManualAction() {
        ForeignKeyDefinition key = ForeignKeyDefinition.builder("fk_accounts").addColumn("parent_id")
                .reference(PARENT).addReferenceColumn("id").onUpdate(ReferentialAction.CASCADE).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build()).addForeignKey(key).build();
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("Oracle", "19", DIALECT), desired,
                SchemaSnapshot.present(table(ReferentialAction.NO_ACTION)),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
        assertFalse(plan.acceptsApproval(SchemaMigrationApproval.approveWithWritesQuiesced(plan, "isolated fixture")));
    }

    private static ReviewedSchemaPlan plan() {
        return RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("Oracle", "19", DIALECT), table(ReferentialAction.CASCADE),
                SchemaSnapshot.present(table(ReferentialAction.NO_ACTION)),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static RelationalTableDefinition table(ReferentialAction action) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build());
        if (action != null) {
            builder.addForeignKey(ForeignKeyDefinition.builder("fk_accounts").addColumn("parent_id")
                    .reference(PARENT).addReferenceColumn("id").onDelete(action).build());
        }
        return builder.build();
    }

    /** 只替代外部数据库端口；计划、批准、逐步执行、回读 diff 和结果状态都运行真实 ORM 实现。 */
    private static final class Harness {
        private final List<SqlRequest> sent = new ArrayList<>();
        private SchemaSnapshot snapshot = SchemaSnapshot.present(table(ReferentialAction.NO_ACTION));
        private SchemaSnapshot postExecutionSnapshot = SchemaSnapshot.present(table(ReferentialAction.CASCADE));
        private boolean failAdd;
        private int reads;
        private int invalidations;

        private SchemaSnapshot read() {
            reads++;
            return snapshot;
        }

        private long execute(SqlRequest request) {
            sent.add(request);
            if (sent.size() == 1) {
                snapshot = SchemaSnapshot.present(table(null));
            } else {
                if (failAdd) {
                    throw new IllegalStateException("simulated driver validation failure");
                }
                snapshot = postExecutionSnapshot;
            }
            return 0L;
        }

        private SchemaExecutionReport execute(ReviewedSchemaPlan plan, SchemaMigrationApproval approval,
                                              boolean reactive) {
            var options = new SchemaMigrationExecutionOptions(SqlExecutionOptions.safeDefaults(), approval);
            if (reactive) {
                ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                    @Override
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.error(new AssertionError("no DML query expected"));
                    }

                    @Override
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.fromSupplier(() -> Harness.this.execute(request));
                    }
                };
                return VerifiedSchemaPlanExecutor.executeReactive(plan, executor, () -> Mono.fromSupplier(this::read),
                        SchemaSnapshotCoverage::complete, () -> invalidations++, options).block();
            }
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("no DML query expected");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return Harness.this.execute(request);
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions sqlOptions) {
                    throw new AssertionError("no generated keys expected");
                }
            };
            return VerifiedSchemaPlanExecutor.executeJdbc(plan, executor, this::read,
                    SchemaSnapshotCoverage::complete, () -> invalidations++, options);
        }
    }
}
