package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SchemaPlanToctouPreconditionTest {

    @Test
    void refusesDriftBeforeSendingAnyReviewedSql() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        RelationalTableDefinition drifted = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID,
                ColumnDefinition.builder("external_change", "VARCHAR").build());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                SchemaSnapshot.present(before), desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> 1L);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan,
                executor,
                () -> {
                    reads.incrementAndGet();
                    return SchemaSnapshot.present(drifted);
                },
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.PRECONDITION_FAILED, report.status());
        assertEquals(1, reads.get());
        assertEquals(0, invalidations.get());
        assertEquals(0, executor.requests.size());
        assertEquals(SchemaExecutionStatus.PRECONDITION_FAILED, report.steps().getFirst().status());
        assertFalse(report.steps().getFirst().sqlSent());
    }
}

final class VerifiedSchemaPlanFixtures {

    static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    static final ColumnDefinition ID = ColumnDefinition.builder("id", "BIGINT")
            .nullable(false)
            .build();
    static final ColumnDefinition NOTE = ColumnDefinition.builder("note", "VARCHAR")
            .length(80)
            .build();
    static final ColumnDefinition TAG = ColumnDefinition.builder("tag", "VARCHAR")
            .length(40)
            .build();

    private VerifiedSchemaPlanFixtures() {
    }

    static RelationalTableDefinition table(ColumnDefinition... columns) {
        RelationalTableDefinition.Builder table = RelationalTableDefinition.builder(ACCOUNTS);
        for (ColumnDefinition column : columns) {
            table.addColumn(column);
        }
        return table.build();
    }

    static ReviewedSchemaPlan plan(SchemaSnapshot before,
                                   RelationalTableDefinition desired,
                                   SqlRequest... requests) {
        String actualFingerprint = SchemaSnapshotFingerprint.of(before);
        ReviewedSchemaPlan.Builder plan = ReviewedSchemaPlan.builder(DatabaseDescriptor.of(
                        "H2", "2.3", "h2", DialectCapabilities.empty()))
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(desired)
                .desiredFingerprint(RelationalMetadataFingerprint.of(desired))
                .actualFingerprint(actualFingerprint);
        int existingColumns = before.tableState() == SchemaSnapshot.State.PRESENT
                ? before.columns().value().size() : 0;
        for (int index = 0; index < requests.length; index++) {
            ColumnDefinition column = desired.columns().get(existingColumns + index);
            SchemaOperation operation = SchemaOperation.of(
                    SchemaOperation.Kind.ADD_COLUMN,
                    desired.identity(),
                    column.name(),
                    null,
                    column,
                    SchemaOperation.Compatibility.SAFE_INCREMENTAL);
            plan.addStep(SchemaPlanStep.executable(
                    index,
                    operation,
                    requests[index],
                    SchemaMigrationRiskLevel.LOW,
                    List.of(SchemaPlanPrecondition.actualSnapshot(actualFingerprint))));
        }
        return plan.build();
    }

    static final class SyncExecutor implements SyncSqlExecutor {

        private final Function<SqlRequest, Long> behavior;
        final List<SqlRequest> requests = new ArrayList<>();

        SyncExecutor(Function<SqlRequest, Long> behavior) {
            this.behavior = behavior;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            requests.add(request);
            return behavior.apply(request);
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    static final class ReactiveExecutor implements ReactiveSqlExecutor {

        private final Function<SqlRequest, Mono<Long>> behavior;
        final List<SqlRequest> requests = new ArrayList<>();

        ReactiveExecutor(Function<SqlRequest, Mono<Long>> behavior) {
            this.behavior = behavior;
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            requests.add(request);
            return behavior.apply(request);
        }
    }
}
