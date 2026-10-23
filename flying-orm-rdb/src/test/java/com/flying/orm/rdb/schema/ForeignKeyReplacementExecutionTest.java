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

import static org.junit.jupiter.api.Assertions.*;

class ForeignKeyReplacementExecutionTest {

    @Test
    void replacementReportsExecutedSqlOrPartialFailureWithoutCompensation() {
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.h2(),
                RdbDialect.sqlServer(), RdbDialect.oracle())) {
            SchemaMigrationPlan plan = plan(dialect);
            assertEquals(2, plan.requests().size(), dialect.name());
            for (boolean reactive : List.of(false, true)) {
                for (boolean failAdd : List.of(false, true)) {
                    Harness harness = new Harness();
                    harness.failAdd = failAdd;
                    if (failAdd) {
                        SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                                () -> harness.execute(plan, reactive));
                        assertEquals(1, failure.stepIndex());
                        assertEquals(List.of(plan.requests().getFirst()),
                                failure.completedWorkSteps().stream().map(step -> step.request()).toList());
                    } else {
                        assertEquals(plan.requests(), harness.execute(plan, reactive).steps()
                                .stream().map(step -> step.request()).toList());
                    }
                    assertEquals(plan.requests(), harness.sent);
                }
            }
        }
    }

    private static SchemaMigrationPlan plan(RdbDialect dialect) {
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), table(ReferentialAction.CASCADE),
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
        private boolean failAdd;

        private long send(SqlRequest request) {
            sent.add(request);
            if (sent.size() == 2 && failAdd) {
                throw new IllegalStateException("simulated ADD failure");
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
                        return Flux.error(new AssertionError("no DML expected"));
                    }

                    @Override
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.fromSupplier(() -> send(request));
                    }
                };
                return SchemaExecutionFixtures.executeReactive(plan, executor, () -> { }, options).block();
            }
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("no DML expected");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return send(request);
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions sqlOptions, String generatedKeyColumn) {
                    throw new AssertionError("no generated keys expected");
                }
            };
            return SchemaExecutionFixtures.executeJdbc(plan, executor, () -> { }, options);
        }
    }
}
