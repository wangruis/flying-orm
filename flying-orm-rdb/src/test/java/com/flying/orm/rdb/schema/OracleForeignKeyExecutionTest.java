package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OracleForeignKeyExecutionTest {

    private static final RdbDialect DIALECT = RdbDialect.oracle();
    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");
    private static final RelationIdentity PARENT = RelationIdentity.of(null, "app", "parents");

    @Test
    void publicReviewFreezesOracleForeignKeyReplacementAndValidation() {
        SchemaMigrationPlan plan = plan();
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"fk_accounts\"",
                "alter table \"app\".\"accounts\" add constraint \"fk_accounts\" foreign key (\"parent_id\")"
                        + " references \"app\".\"parents\" (\"id\") on delete cascade enable validate"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void explicitPlanExecutesFrozenStepsOnBothPorts() {
        SchemaMigrationPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            Harness database = new Harness();
            SchemaMigrationResult report = database.execute(plan, reactive);
            assertNotNull(report);
            assertEquals(2, report.steps().size());
            assertEquals(plan.requests(), database.sent);
            assertEquals(1, database.invalidations);
            assertEquals(plan.requests(), report.steps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void failedAddKeepsTheExecutedDropEvidenceWithoutRetryOrCompensation() {
        SchemaMigrationPlan plan = plan();
        for (boolean reactive : List.of(false, true)) {
            Harness database = new Harness();
            database.failAdd = true;
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                    () -> database.execute(plan, reactive));
            assertEquals(1, failure.stepIndex());
            assertEquals(List.of(plan.requests().getFirst()),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
            assertEquals(plan.requests(), database.sent);
            assertEquals(1, database.invalidations);
        }
    }

    @Test
    void anUnsupportedOracleUpdateActionFailsDuringPlanning() {
        ForeignKeyDefinition key = ForeignKeyDefinition.builder("fk_accounts").addColumn("parent_id")
                .reference(PARENT).addReferenceColumn("id").onUpdate(ReferentialAction.CASCADE).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build()).addForeignKey(key).build();
        UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class,
                () -> RelationalSchemaPlanReviewer.create(DIALECT).review(
                DatabaseDescriptor.of("Oracle", "19", DIALECT), desired,
                SchemaSnapshot.present(table(ReferentialAction.NO_ACTION)),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT));
        assertTrue(failure.getMessage().contains("ON UPDATE"));
    }

    private static SchemaMigrationPlan plan() {
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

    /** 只替代外部数据库端口；DDL 规划、顺序执行与错误传播运行真实 ORM 实现。 */
    private static final class Harness {
        private final List<SqlRequest> sent = new ArrayList<>();
        private boolean failAdd;
        private int invalidations;

        private long execute(SqlRequest request) {
            sent.add(request);
            if (sent.size() == 2 && failAdd) {
                throw new IllegalStateException("simulated driver validation failure");
            }
            return 0L;
        }

        private SchemaMigrationResult execute(SchemaMigrationPlan plan,
                                              boolean reactive) {
            var options = SqlExecutionOptions.unlimited();
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
                return SchemaExecutionFixtures.executeReactive(plan, executor, () -> invalidations++, options).block();
            }
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("no DML query expected");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return Harness.this.execute(request);
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions sqlOptions, String generatedKeyColumn) {
                    throw new AssertionError("no generated keys expected");
                }
            };
            return SchemaExecutionFixtures.executeJdbc(plan, executor, () -> invalidations++, options);
        }
    }
}
