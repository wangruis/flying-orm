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

import static org.junit.jupiter.api.Assertions.*;

class MySqlForeignKeyExecutionTest {

    @Test
    void reviewRequiresExplicitQuiescenceOnBothExecutionPorts() {
        ReviewedSchemaPlan plan = plan();
        assertFalse(plan.requiresManualAction());
        assertTrue(plan.requiresWritesQuiesced());
        assertEquals(2, plan.requests().size());
        for (boolean reactive : List.of(false, true)) {
            for (SchemaMigrationApproval approval : java.util.Arrays.asList(null,
                    SchemaMigrationApproval.approve(plan, "writes are quiesced"),
                    new SchemaMigrationApproval("different-plan", "isolated fixture", true))) {
                Harness harness = new Harness();
                assertEquals(SchemaExecutionStatus.FAILED, harness.execute(plan, approval, reactive).status());
                assertEquals(0, harness.reads);
                assertTrue(harness.sent.isEmpty());
            }
        }
    }

    @Test
    void approvedReplacementVerifiesTargetOrReportsPartialFailureWithoutCompensation() {
        ReviewedSchemaPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            for (boolean failAdd : List.of(false, true)) {
                Harness harness = new Harness();
                harness.failAdd = failAdd;
                SchemaExecutionReport report = harness.execute(plan,
                        SchemaMigrationApproval.approveWithWritesQuiesced(plan, "isolated fixture"), reactive);
                assertEquals(failAdd ? SchemaExecutionStatus.PARTIAL : SchemaExecutionStatus.SUCCESS, report.status());
                assertEquals(plan.requests(), harness.sent);
                assertEquals(2, harness.reads);
                assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
                assertEquals(failAdd ? SchemaExecutionStatus.UNKNOWN : SchemaExecutionStatus.SUCCESS,
                        report.steps().get(1).status());
                assertEquals(!failAdd, report.successful());
            }
        }
    }

    private static ReviewedSchemaPlan plan() {
        RdbDialect dialect = RdbDialect.mysql();
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("MySQL", "8.4", dialect), table(ReferentialAction.CASCADE),
                SchemaSnapshot.present(table(ReferentialAction.NO_ACTION)), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
    }

    private static RelationalTableDefinition table(ReferentialAction action) {
        var table = RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build());
        if (action != null) {
            table.addForeignKey(ForeignKeyDefinition.builder("fk_accounts").addColumn("parent_id")
                    .reference(RelationIdentity.table("parents")).addReferenceColumn("id").onDelete(action).build());
        }
        return table.build();
    }

    private static final class Harness {
        private final List<SqlRequest> sent = new ArrayList<>();
        private SchemaSnapshot snapshot = SchemaSnapshot.present(table(ReferentialAction.NO_ACTION));
        private boolean failAdd;
        private int reads;

        private SchemaSnapshot read() {
            reads++;
            return snapshot;
        }

        private long send(SqlRequest request) {
            sent.add(request);
            if (sent.size() == 1) {
                snapshot = SchemaSnapshot.present(table(null));
            } else {
                if (failAdd) throw new IllegalStateException("simulated ADD failure");
                snapshot = SchemaSnapshot.present(table(ReferentialAction.CASCADE));
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
                        return Flux.error(new AssertionError("no DML expected"));
                    }

                    @Override
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.fromSupplier(() -> send(request));
                    }
                };
                return VerifiedSchemaPlanExecutor.executeReactive(plan, executor, () -> Mono.fromSupplier(this::read),
                        SchemaSnapshotCoverage::complete, () -> { }, options).block();
            }
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("no DML expected");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return send(request);
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions sqlOptions) {
                    throw new AssertionError("no generated keys expected");
                }
            };
            return VerifiedSchemaPlanExecutor.executeJdbc(plan, executor, this::read,
                    SchemaSnapshotCoverage::complete, () -> { }, options);
        }
    }
}
