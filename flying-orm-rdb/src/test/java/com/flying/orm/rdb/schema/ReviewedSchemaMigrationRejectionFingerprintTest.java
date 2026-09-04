package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
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
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReviewedSchemaMigrationRejectionFingerprintTest {

    @Test
    void bindsR2dbcLockTimeoutCapabilityRejectionToTheReviewedPlan() {
        ReviewedSchemaMigrationPlan plan = reviewedPlan();
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(
                reactiveExecutor(Mono.empty()),
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL);
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults()
                                                                                  .withLockTimeout(Duration.ofSeconds(1));

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(plan, List.of("orders"), ignored -> {
                }, options).block());

        assertPlanBinding(plan, failure);
        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
    }

    @Test
    void bindsJdbcExternalTransactionRejectionToTheReviewedPlan() {
        ReviewedSchemaMigrationPlan plan = reviewedPlan();
        java.sql.Connection connection = jdbcConnection();
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                syncExecutor(),
                FormSchemaSqlRenderer.create(RdbDialect.mysql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.IMPLICIT_COMMIT,
                () -> Optional.of(JdbcTransactionContext.external(connection, ignored -> true)),
                ignored -> {
                });
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults()
                                                                                  .withLockTimeout(Duration.ZERO);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(plan, List.of("orders"), options));

        assertPlanBinding(plan, failure);
        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
    }

    @Test
    void bindsR2dbcCompletionRegistrationRejectionToTheReviewedPlan() {
        ReviewedSchemaMigrationPlan plan = reviewedPlan();
        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(
                r2dbcConnection(), ignored -> false);
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(
                reactiveExecutor(Mono.just(transaction)),
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL);
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults()
                                                                                  .withLockTimeout(Duration.ZERO);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(plan, List.of("orders"), ignored -> {
                }, options).block());

        assertPlanBinding(plan, failure);
        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
    }

    private static void assertPlanBinding(ReviewedSchemaMigrationPlan plan,
                                          SchemaMigrationRejectedException failure) {
        assertEquals(plan.fingerprint(), failure.planFingerprint());
        assertEquals(plan.fingerprint(), failure.toErrorReport().resource());
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target,
                List.of(),
                List.of(),
                true,
                List.of(new SqlRequest("alter table orders add column note varchar(20)", List.of())),
                List.of());
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static ReactiveSqlExecutor reactiveExecutor(Mono<R2dbcTransactionContext> transaction) {
        return new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return transaction;
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("DDL must be rejected before execution"));
            }
        };
    }

    private static SyncSqlExecutor syncExecutor() {
        return new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("DDL must be rejected before execution");
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("DDL must be rejected before execution");
            }
        };
    }

    private static java.sql.Connection jdbcConnection() {
        return (java.sql.Connection) Proxy.newProxyInstance(
                ReviewedSchemaMigrationRejectionFingerprintTest.class.getClassLoader(),
                new Class<?>[]{java.sql.Connection.class},
                (ignored, method, arguments) -> null);
    }

    private static Connection r2dbcConnection() {
        return (Connection) Proxy.newProxyInstance(
                ReviewedSchemaMigrationRejectionFingerprintTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> null);
    }
}
