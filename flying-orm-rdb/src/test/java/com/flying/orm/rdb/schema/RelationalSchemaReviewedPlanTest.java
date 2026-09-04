package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RelationalSchemaReviewedPlanTest {

    @Test
    void pendingExternalTransactionCannotBeReportedWithoutPostVerification() {
        assertThrows(IllegalArgumentException.class, () -> SchemaExecutionReport.of(
                "plan",
                SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING,
                List.of(),
                null,
                null,
                null));
    }

    @Test
    void jdbcReviewedPlanRejectsImplicitCommitExternalTransactionBeforeSql() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger executions = new AtomicInteger();
        SqlRequest request = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(snapshot.get(), desired, request);
        SyncSqlExecutor sqlExecutor = syncExecutor(sql -> {
            executions.incrementAndGet();
            snapshot.set(SchemaSnapshot.present(desired));
            return 1L;
        });
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.mysql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.IMPLICIT_COMMIT,
                () -> Optional.of(JdbcTransactionContext.external(jdbcConnection())),
                ignored -> { });

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(
                        plan,
                        snapshot::get,
                        SchemaSnapshotCoverage::complete,
                        () -> { },
                        executionOptions(Duration.ZERO)));

        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
        assertEquals(0, executions.get());
    }

    @Test
    void reactiveReviewedPlanRejectsUnknownExternalTransactionBeforeSql() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger executions = new AtomicInteger();
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot.get(), desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return Mono.just(R2dbcTransactionContext.external(r2dbcConnection()));
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                executions.incrementAndGet();
                snapshot.set(SchemaSnapshot.present(desired));
                return Mono.just(1L);
            }
        };
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.h2()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.UNKNOWN);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(
                        plan,
                        () -> Mono.just(snapshot.get()),
                        SchemaSnapshotCoverage::complete,
                        () -> { },
                        executionOptions(Duration.ZERO)).block());

        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
        assertEquals(0, executions.get());
    }

    @Test
    void jdbcReviewedPlanDoesNotReportFinalSuccessBeforeTheExternalTransactionCompletes() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        SqlRequest work = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(snapshot.get(), desired, work);
        java.sql.Connection connection = jdbcConnection();
        com.flying.orm.rdb.transaction.JdbcTransactionParticipant participant =
                () -> Optional.of(JdbcTransactionContext.external(connection, ignored -> true));
        List<SqlRequest> requests = new ArrayList<>();
        List<java.sql.Connection> connections = new ArrayList<>();
        SyncSqlExecutor sqlExecutor = syncExecutor(request -> {
            requests.add(request);
            connections.add(participant.currentTransaction().orElseThrow().connection());
            if (request == work) {
                snapshot.set(SchemaSnapshot.present(desired));
            }
            return 0L;
        });
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                participant,
                ignored -> { });

        SchemaExecutionReport report = executor.executeReviewed(
                plan,
                snapshot::get,
                SchemaSnapshotCoverage::complete,
                () -> { },
                executionOptions(Duration.ofSeconds(1)));

        assertFalse(report.successful());
        assertEquals(SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING, report.status());
        assertTrue(report.verification().orElseThrow().compatible());
        assertEquals(List.of("set lock_timeout = '1000ms'", work.sql(), "reset lock_timeout"),
                     requests.stream().map(SqlRequest::sql).toList());
        assertEquals(List.of(connection, connection, connection), connections);
    }

    @Test
    void reactiveReviewedPlanDoesNotReportFinalSuccessBeforeTheExternalTransactionCompletes() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        SqlRequest work = new SqlRequest(
                "alter table accounts add column note varchar(80)", List.of());
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(snapshot.get(), desired, work);
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return Mono.just(R2dbcTransactionContext.external(
                        r2dbcConnection(), ignored -> true));
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                snapshot.set(SchemaSnapshot.present(desired));
                return Mono.just(1L);
            }
        };
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL);

        SchemaExecutionReport report = executor.executeReviewed(
                plan,
                () -> Mono.just(snapshot.get()),
                SchemaSnapshotCoverage::complete,
                () -> { },
                executionOptions(Duration.ZERO)).block();

        assertFalse(report.successful());
        assertEquals(SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING, report.status());
        assertTrue(report.verification().orElseThrow().compatible());
    }

    @Test
    void freezesDiffSqlAndAllExecutionPreconditionsInOneReviewedPlan() {
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.3", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();
        SchemaSnapshot actual = SchemaSnapshot.absent(desired.identity());
        RelationalSchemaPlanReviewer reviewer = RelationalSchemaPlanReviewer.create(dialect);

        ReviewedSchemaPlan first = reviewer.review(
                database, desired, actual, SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);
        ReviewedSchemaPlan second = reviewer.review(
                database, desired, actual, SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertSame(desired, first.desiredTable().orElseThrow());
        assertEquals(RelationalMetadataFingerprint.of(desired), first.desiredFingerprint());
        assertEquals(SchemaSnapshotFingerprint.of(actual), first.actualFingerprint());
        assertEquals(first.fingerprint(), second.fingerprint());
        assertFalse(first.requests().isEmpty());
        assertFalse(first.requiresManualAction());
        assertEquals(List.of(
                             SchemaPlanPrecondition.Kind.DATABASE_DESCRIPTOR,
                             SchemaPlanPrecondition.Kind.CAPABILITIES,
                             SchemaPlanPrecondition.Kind.ACTUAL_SCHEMA,
                             SchemaPlanPrecondition.Kind.SNAPSHOT_COVERAGE),
                     first.steps().getFirst().preconditions().stream()
                             .map(SchemaPlanPrecondition::kind).toList());
    }

    @Test
    void rendersUnqualifiedSqlServerCommentsAsReviewedExecutableSql() {
        RdbDialect dialect = RdbDialect.sqlServer();
        DatabaseDescriptor database = DatabaseDescriptor.of("SQL Server", "16", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .comment("账户表")
                .addColumn(ColumnDefinition.builder("id", "BIGINT")
                        .nullable(false)
                        .comment("主键")
                        .build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertFalse(plan.requiresManualAction());
        assertTrue(plan.requests().stream()
                .anyMatch(request -> request.sql().startsWith("exec sp_executesql ")));
    }

    @Test
    void keepsInvalidMysqlIdentityLayoutAsManualWork() {
        RdbDialect dialect = RdbDialect.mysql();
        DatabaseDescriptor database = DatabaseDescriptor.of("MySQL", "8.4", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("sequence_no", "BIGINT")
                        .nullable(false)
                        .generation(ValueGeneration.identity())
                        .build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database,
                desired,
                SchemaSnapshot.absent(desired.identity()),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.SAFE_INCREMENTAL);

        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
    }

    @Test
    void treatsMysqlPrimaryAsThePhysicalNameOfAnAnnotatedPrimaryKey() {
        RdbDialect dialect = RdbDialect.mysql();
        DatabaseDescriptor database = DatabaseDescriptor.of("MySQL", "8.4", dialect);
        RelationIdentity identity = RelationIdentity.table("accounts");
        ColumnDefinition id = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(identity)
                .addColumn(id)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .build();
        RelationalTableDefinition observed = RelationalTableDefinition.builder(identity)
                .addColumn(id)
                .primaryKey(PrimaryKeyDefinition.of("PRIMARY", "id"))
                .build();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                database, desired, SchemaSnapshot.present(observed),
                SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);

        assertTrue(plan.steps().isEmpty());
    }

    @Test
    void rejectsOverlongExplicitObjectNamesBeforeBuildingAnExecutablePlan() {
        RdbDialect dialect = RdbDialect.oracle(com.flying.orm.rdb.dialect.OracleVersion.V12C);
        DatabaseDescriptor database = DatabaseDescriptor.of("Oracle", "12c", dialect);
        RelationalTableDefinition desired = RelationalTableDefinition
                .builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts_name_that_exceeds_oracle_12c_limit", "id"))
                .build();

        assertThrows(IllegalArgumentException.class, () ->
                RelationalSchemaPlanReviewer.create(dialect).review(
                        database,
                        desired,
                        SchemaSnapshot.absent(desired.identity()),
                        SchemaSnapshotCoverage.complete(),
                        SchemaCompatibilityMode.SAFE_INCREMENTAL));
    }

    private static SchemaMigrationExecutionOptions executionOptions(Duration lockTimeout) {
        return new SchemaMigrationExecutionOptions(
                SqlExecutionOptions.safeDefaults(), null, lockTimeout);
    }

    private static SyncSqlExecutor syncExecutor(Function<SqlRequest, Long> execution) {
        return new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                return execution.apply(request);
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not execute");
            }
        };
    }

    private static java.sql.Connection jdbcConnection() {
        return (java.sql.Connection) Proxy.newProxyInstance(
                java.sql.Connection.class.getClassLoader(),
                new Class<?>[]{java.sql.Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "reviewed-schema-jdbc-transaction";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection r2dbcConnection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "reviewed-schema-r2dbc-transaction";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
