package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaMigrationExecutorTest {

    private static final SqlRequest REQUEST = new SqlRequest("alter table orders add note varchar(20)", List.of());

    @Test
    void reactiveDdlTreatsAnUnavailableUpdateCountAsZero() {
        Long rows = executor(Mono.just(-1L)).execute(List.of(REQUEST)).block();

        assertEquals(0L, rows);
    }

    @Test
    void jdbcDdlTreatsAnUnavailableUpdateCountAsZero() {
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                return -1L;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request,
                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                com.flying.orm.rdb.transaction.JdbcTransactionParticipant.none(),
                ignored -> { });

        long rows = executor.execute(
                List.of(REQUEST), com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults());

        assertEquals(0L, rows);
    }

    @Test
    void invalidatesExactlyOnceBeforeSuccessIsVisible() {
        AtomicInteger invalidations = new AtomicInteger();
        SchemaMigrationExecutor executor = executor(Mono.just(1L));

        long rows = executor.executeWithInvalidation(
                                    List.of(REQUEST),
                                    List.of("orders"),
                                    ignored -> invalidations.incrementAndGet(),
                                    com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                            .doOnNext(ignored -> assertEquals(1, invalidations.get()))
                            .block();

        assertEquals(1L, rows);
        assertEquals(1, invalidations.get());
    }

    @Test
    void reactiveInvalidationAttemptsEveryTableAndDoesNotSilenceFailures() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException first = new IllegalStateException("orders invalidation failed");
        IllegalArgumentException second = new IllegalArgumentException("orders audit invalidation failed");
        SchemaMigrationExecutor executor = executor(Mono.just(1L));

        RdbException thrown = assertThrows(
                RdbException.class,
                () -> executor.executeWithInvalidation(
                                      List.of(REQUEST),
                                      List.of("orders", "orders_audit"),
                                      ignored -> {
                                          int attempt = attempts.getAndIncrement();
                                          throw attempt == 0 ? first : second;
                                      },
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                              .block());

        assertEquals(2, attempts.get());
        assertSame(first, thrown.getCause());
        assertTrue(java.util.Arrays.stream(thrown.getSuppressed())
                                   .anyMatch(suppressed -> suppressed == second));
    }

    @Test
    void jdbcInvalidationAttemptsEveryTableAndDoesNotSilenceFailures() {
        AtomicInteger attempts = new AtomicInteger();
        IllegalStateException first = new IllegalStateException("orders invalidation failed");
        IllegalArgumentException second = new IllegalArgumentException("orders audit invalidation failed");
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                return 1L;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request,
                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                com.flying.orm.rdb.transaction.JdbcTransactionParticipant.none(),
                ignored -> { });

        RdbException thrown = assertThrows(
                RdbException.class,
                () -> executor.executeWithInvalidation(
                        List.of(REQUEST),
                        List.of("orders", "orders_audit"),
                        ignored -> {
                            int attempt = attempts.getAndIncrement();
                            throw attempt == 0 ? first : second;
                        },
                        com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults()));

        assertEquals(2, attempts.get());
        assertSame(first, thrown.getCause());
        assertEquals(1, thrown.getSuppressed().length);
        assertSame(second, thrown.getSuppressed()[0]);
    }

    @Test
    void reactiveDdlFailureRemainsPrimaryWhenInvalidationAlsoFails() {
        IllegalStateException ddlFailure = new IllegalStateException("ddl failed");
        IllegalArgumentException invalidationFailure = new IllegalArgumentException("invalidation failed");
        SchemaMigrationExecutor executor = executor(Mono.error(ddlFailure));

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> executor.executeWithInvalidation(
                                      List.of(REQUEST),
                                      List.of("orders"),
                                      ignored -> { throw invalidationFailure; },
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                              .block());

        assertSame(ddlFailure, thrown);
        assertTrue(java.util.Arrays.stream(thrown.getSuppressed())
                                   .filter(RdbException.class::isInstance)
                                   .map(RdbException.class::cast)
                                   .anyMatch(cleanup -> cleanup.getCause() == invalidationFailure));
    }

    @Test
    void jdbcDdlFailureRemainsPrimaryWhenInvalidationAlsoFails() {
        IllegalStateException ddlFailure = new IllegalStateException("ddl failed");
        IllegalArgumentException invalidationFailure = new IllegalArgumentException("invalidation failed");
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                throw ddlFailure;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request,
                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                com.flying.orm.rdb.transaction.JdbcTransactionParticipant.none(),
                ignored -> { });

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> executor.executeWithInvalidation(
                        List.of(REQUEST),
                        List.of("orders"),
                        ignored -> { throw invalidationFailure; },
                        com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults()));

        assertSame(ddlFailure, thrown);
        assertEquals(1, thrown.getSuppressed().length);
        RdbException cleanup = (RdbException) thrown.getSuppressed()[0];
        assertSame(invalidationFailure, cleanup.getCause());
    }

    @Test
    void jdbcExternalTransactionDefersInvalidationUntilCompletion() {
        AtomicInteger invalidations = new AtomicInteger();
        TestJdbcTransactionCompletion completion = new TestJdbcTransactionCompletion();
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                return 1L;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request,
                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        java.sql.Connection connection = (java.sql.Connection) Proxy.newProxyInstance(
                java.sql.Connection.class.getClassLoader(),
                new Class<?>[]{java.sql.Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "jdbc-schema-transaction-connection";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                () -> Optional.of(JdbcTransactionContext.external(connection, completion)),
                ignored -> { });

        long rows = executor.executeWithInvalidation(
                List.of(REQUEST),
                List.of("orders"),
                ignored -> invalidations.incrementAndGet(),
                com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults());

        assertEquals(1L, rows);
        assertEquals(0, invalidations.get());
        completion.complete();
        assertEquals(1, invalidations.get());
    }

    @Test
    void invalidatesExactlyOnceOnError() {
        AtomicInteger invalidations = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("ddl failed");
        SchemaMigrationExecutor executor = executor(Mono.error(failure));

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> executor.executeWithInvalidation(
                                      List.of(REQUEST),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                              .block());

        assertEquals(failure, thrown);
        assertEquals(1, invalidations.get());
    }

    @Test
    void invalidatesExactlyOnceOnCancelAfterExecutionStarts() {
        AtomicInteger invalidations = new AtomicInteger();
        SchemaMigrationExecutor executor = executor(Mono.never());

        Disposable subscription = executor.executeWithInvalidation(
                                          List.of(REQUEST),
                                          List.of("orders"),
                                          ignored -> invalidations.incrementAndGet(),
                                          com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                                  .subscribe();
        subscription.dispose();

        assertEquals(1, invalidations.get());
    }

    @Test
    void reviewedRelationalCancellationAfterSqlStartsInvalidatesExactlyOnce() {
        com.flying.orm.core.metadata.RelationalTableDefinition before =
                VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID);
        com.flying.orm.core.metadata.RelationalTableDefinition desired =
                VerifiedSchemaPlanFixtures.table(
                        VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(snapshot.get(), desired, REQUEST);
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                executions.incrementAndGet();
                snapshot.set(SchemaSnapshot.present(desired));
                return Mono.never();
            }
        };
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL);

        Disposable subscription = executor.executeReviewed(
                plan,
                () -> Mono.just(snapshot.get()),
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                new SchemaMigrationExecutionOptions(
                        com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults(),
                        null,
                        Duration.ZERO))
                .subscribe();

        assertEquals(1, executions.get());
        subscription.dispose();
        assertEquals(1, invalidations.get());
    }

    @Test
    void externalTransactionSuccessInvalidatesOnlyOnceAtTransactionCompletion() {
        AtomicInteger invalidations = new AtomicInteger();
        TestTransactionCompletion completion = TestTransactionCompletion.available();
        SchemaMigrationExecutor executor = executor(Mono.just(1L), completion);

        long rows = executor.executeWithInvalidation(
                                    List.of(REQUEST),
                                    List.of("orders"),
                                    ignored -> invalidations.incrementAndGet(),
                                    com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                            .block();

        assertEquals(1L, rows);
        assertEquals(0, invalidations.get());
        completion.complete();
        assertEquals(1, invalidations.get());
    }

    @Test
    void externalTransactionErrorInvalidatesOnlyOnceAtTransactionCompletion() {
        AtomicInteger invalidations = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("ddl failed");
        TestTransactionCompletion completion = TestTransactionCompletion.available();
        SchemaMigrationExecutor executor = executor(Mono.error(failure), completion);

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> executor.executeWithInvalidation(
                                      List.of(REQUEST),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                              .block());

        assertEquals(failure, thrown);
        assertEquals(0, invalidations.get());
        completion.complete();
        assertEquals(1, invalidations.get());
    }

    @Test
    void externalTransactionCancelInvalidatesOnlyOnceAtTransactionCompletion() {
        AtomicInteger invalidations = new AtomicInteger();
        TestTransactionCompletion completion = TestTransactionCompletion.available();
        SchemaMigrationExecutor executor = executor(Mono.never(), completion);

        Disposable subscription = executor.executeWithInvalidation(
                                          List.of(REQUEST),
                                          List.of("orders"),
                                          ignored -> invalidations.incrementAndGet(),
                                          com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                                  .subscribe();
        subscription.dispose();

        assertEquals(0, invalidations.get());
        completion.complete();
        assertEquals(1, invalidations.get());
    }

    @Test
    void externalTransactionCompletionDoesNotInvalidateWhenExecutionNeverStarts() {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        TestTransactionCompletion completion = TestTransactionCompletion.available();
        SchemaMigrationExecutor executor = executor(
                Mono.defer(() -> {
                    executions.incrementAndGet();
                    return Mono.just(1L);
                }),
                completion);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(
                                      reviewedPlan(),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      SchemaMigrationExecutionOptions.defaults()
                                                                             .withLockTimeout(Duration.ofSeconds(1)))
                              .block());

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(0, executions.get());
        assertEquals(0, invalidations.get());
        completion.complete();
        assertEquals(0, invalidations.get());
    }

    @Test
    void rejectsUnavailableExternalTransactionCompletionBeforeSqlExecution() {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        SchemaMigrationExecutor executor = executor(
                Mono.defer(() -> {
                    executions.incrementAndGet();
                    return Mono.just(1L);
                }),
                TestTransactionCompletion.unavailable());

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeWithInvalidation(
                                      List.of(REQUEST),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults())
                              .block());

        assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
        assertEquals(0, executions.get());
        assertEquals(0, invalidations.get());
    }

    private static SchemaMigrationExecutor executor(Mono<Long> execution) {
        return executor(execution, null);
    }

    private static SchemaMigrationExecutor executor(Mono<Long> execution,
                                                    R2dbcTransactionCompletion completion) {
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return completion == null
                        ? Mono.empty()
                        : Mono.just(R2dbcTransactionContext.external(connection(), completion));
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return execution;
            }
        };
        return new SchemaMigrationExecutor(
                sqlExecutor,
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL);
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
                List.of(REQUEST),
                List.of());
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "schema-transaction-connection";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class TestTransactionCompletion implements R2dbcTransactionCompletion {

        private final boolean acceptsRegistration;

        private final AtomicReference<Listener> listener = new AtomicReference<>();

        private TestTransactionCompletion(boolean acceptsRegistration) {
            this.acceptsRegistration = acceptsRegistration;
        }

        private static TestTransactionCompletion available() {
            return new TestTransactionCompletion(true);
        }

        private static TestTransactionCompletion unavailable() {
            return new TestTransactionCompletion(false);
        }

        @Override
        public boolean register(Listener listener) {
            return acceptsRegistration && this.listener.compareAndSet(null, listener);
        }

        private void complete() {
            Listener registered = listener.getAndSet(null);
            if (registered == null) {
                throw new AssertionError("transaction completion listener was not registered");
            }
            Mono.from(registered.afterCompletion(TransactionOutcome.COMMITTED)).block();
        }
    }

    private static final class TestJdbcTransactionCompletion implements JdbcTransactionCompletion {

        private final AtomicReference<Listener> listener = new AtomicReference<>();

        @Override
        public boolean register(Listener listener) {
            return this.listener.compareAndSet(null, listener);
        }

        private void complete() {
            Listener registered = listener.getAndSet(null);
            if (registered == null) {
                throw new AssertionError("transaction completion listener was not registered");
            }
            Mono.from(registered.afterCompletion(TransactionOutcome.COMMITTED)).block();
        }
    }
}
