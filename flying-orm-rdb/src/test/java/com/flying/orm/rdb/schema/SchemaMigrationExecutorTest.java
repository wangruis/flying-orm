package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
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
    void reviewedReactiveMigrationKeepsExecutedSteps() {
        SchemaMigrationExecutor executor = executor(Mono.just(1L));

        SchemaMigrationResult result = executor.executeReviewed(
                reviewedPlan(REQUEST),
                List.of("orders"),
                ignored -> { },
                SchemaMigrationExecutionOptions.defaults()).block();

        assertEquals(1, result.steps().size());
        assertSame(REQUEST, result.steps().getFirst().request());
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
                SchemaMigrationObserver.noop(),
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
                SchemaMigrationObserver.noop(),
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
                SchemaMigrationObserver.noop(),
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
                SchemaMigrationObserver.noop());

        Disposable subscription = executor.executeReviewed(
                plan,
                () -> Mono.just(snapshot.get()),
                SchemaSnapshotCoverage::complete,
                invalidations::incrementAndGet,
                new SchemaMigrationExecutionOptions(
                        com.flying.orm.rdb.execution.SqlExecutionOptions.safeDefaults(),
                        null))
                .subscribe();

        assertEquals(1, executions.get());
        subscription.dispose();
        assertEquals(1, invalidations.get());
    }

    @Test
    void preExecutionRejectionDoesNotInvalidate() {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        SchemaMigrationExecutor executor = executor(
                Mono.defer(() -> {
                    executions.incrementAndGet();
                    return Mono.just(1L);
                }));

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(
                                      reviewedPlan(new SqlRequest(
                                              "alter table orders comment "
                                                      + "/*flying-orm:mysql-comment-no-backslash-escapes*/ 'C:\\data'",
                                              List.of())),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      SchemaMigrationExecutionOptions.defaults())
                              .block());

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(0, executions.get());
        assertEquals(0, invalidations.get());
    }

    private static SchemaMigrationExecutor executor(Mono<Long> execution) {
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public <T> Mono<T> withConnection(SqlRequest firstRequest,
                                              java.util.function.Function<ReactiveSqlExecutor, Mono<T>> work) {
                return Mono.defer(() -> work.apply(this));
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                if (request.sql().equals("select @@SESSION.sql_mode as sql_mode")) {
                    return Flux.empty();
                }
                return Flux.error(new AssertionError("query must not execute"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return execution;
            }
        };
        return new SchemaMigrationExecutor(
                sqlExecutor,
                SchemaMigrationObserver.noop());
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan(SqlRequest request) {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target,
                List.of(),
                List.of(),
                true,
                List.of(request),
                List.of());
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

}
