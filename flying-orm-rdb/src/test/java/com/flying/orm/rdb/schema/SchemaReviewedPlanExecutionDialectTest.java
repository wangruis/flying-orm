package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.metadata.ReactiveFormMetadataReaders;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaReviewedPlanExecutionDialectTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final RelationalTableDefinition PRESENT = RelationalTableDefinition
            .builder(ACCOUNTS)
            .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
            .build();

    @Test
    void jdbcAndReactiveRejectPlansForAnotherConfiguredDialectBeforeMetadataOrSql() {
        ReviewedSchemaPlan plan = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", RdbDialect.postgresql()));
        AtomicInteger jdbcReads = new AtomicInteger();
        AtomicInteger jdbcWrites = new AtomicInteger();
        CountingSyncExecutor jdbcExecutor = new CountingSyncExecutor(jdbcReads, jdbcWrites);
        JdbcSchemaClient jdbc = JdbcSchemaClient.create(jdbcExecutor, RdbDialect.h2());

        assertThrows(IllegalArgumentException.class, () -> jdbc.executeReviewed(
                plan,
                JdbcFormMetadataReaders.create(jdbcExecutor, RdbDialect.h2()),
                SchemaMigrationApproval.approve(plan, "test")));
        assertEquals(0, jdbcReads.get());
        assertEquals(0, jdbcWrites.get());

        AtomicInteger reactiveReads = new AtomicInteger();
        AtomicInteger reactiveWrites = new AtomicInteger();
        CountingReactiveExecutor reactiveExecutor =
                new CountingReactiveExecutor(reactiveReads, reactiveWrites);
        ReactiveSchemaClient reactive = ReactiveSchemaClient.create(
                reactiveExecutor, RdbDialect.h2());

        assertThrows(IllegalArgumentException.class, () -> reactive.executeReviewed(
                plan,
                ReactiveFormMetadataReaders.create(reactiveExecutor, RdbDialect.h2()),
                SchemaMigrationApproval.approve(plan, "test")).block());
        assertEquals(0, reactiveReads.get());
        assertEquals(0, reactiveWrites.get());
    }

    @Test
    void customRendererAndCapabilityMismatchCannotClaimPostgreSqlEvolutionSupport() {
        ReviewedSchemaPlan regular = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", RdbDialect.postgresql()));
        ReviewedSchemaPlan mismatchedCapabilities = absentPlan(DatabaseDescriptor.of(
                "PostgreSQL", "17", "postgresql", DialectCapabilities.empty()));
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        CountingSyncExecutor executor = new CountingSyncExecutor(reads, writes);

        JdbcSchemaClient custom = JdbcSchemaClient.create(
                executor, FormSchemaSqlRenderer.create(SchemaDialect.standard()));
        assertThrows(IllegalArgumentException.class, () -> custom.executeReviewed(
                regular,
                JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql()),
                SchemaMigrationApproval.approve(regular, "test")));

        JdbcSchemaClient postgres = JdbcSchemaClient.create(executor, RdbDialect.postgresql());
        assertThrows(IllegalArgumentException.class, () -> postgres.executeReviewed(
                mismatchedCapabilities,
                JdbcFormMetadataReaders.create(executor, RdbDialect.postgresql()),
                SchemaMigrationApproval.approve(mismatchedCapabilities, "test")));
        assertEquals(0, reads.get());
        assertEquals(0, writes.get());
    }

    private static ReviewedSchemaPlan absentPlan(DatabaseDescriptor database) {
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
                .addStep(SchemaPlanStep.executable(
                        0,
                        drop,
                        new SqlRequest("drop table \"accounts\"", List.of()),
                        SchemaMigrationRiskLevel.CRITICAL,
                        List.of(SchemaPlanPrecondition.actualSnapshot(actualFingerprint))))
                .build();
    }

    private record CountingSyncExecutor(AtomicInteger reads, AtomicInteger writes)
            implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            reads.incrementAndGet();
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            writes.incrementAndGet();
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest request, SqlExecutionOptions options) {
            throw new AssertionError("schema DDL must not request generated keys");
        }
    }

    private record CountingReactiveExecutor(AtomicInteger reads, AtomicInteger writes)
            implements ReactiveSqlExecutor {

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            reads.incrementAndGet();
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            writes.incrementAndGet();
            return Mono.just(0L);
        }
    }
}
