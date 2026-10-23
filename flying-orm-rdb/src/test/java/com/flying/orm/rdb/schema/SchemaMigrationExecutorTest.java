package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.observation.SqlExecutionStatus;

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
    void reactiveObservationReportsCleanupFailureAfterCompletedDdl() {
        AtomicReference<SchemaMigrationObservation> observed = new AtomicReference<>();
        IllegalStateException cleanup = new IllegalStateException("metadata invalidation failed");
        SchemaMigrationExecutor executor = executor(Mono.just(1L), observed::set);

        RuntimeException failure = assertThrows(RuntimeException.class, () -> executor.execute(
                reviewedPlan(REQUEST), List.of("orders"), ignored -> { throw cleanup; },
                SqlExecutionOptions.unlimited()).block());

        SchemaMigrationObservation observation = observed.get();
        assertEquals(SqlExecutionStatus.ERROR, observation.status());
        assertEquals(SqlExecutionPhase.CLEANUP, observation.failedPhase());
        assertEquals(1, observation.failedStepIndex());
        assertEquals(1, observation.completedSteps());
        assertEquals(1L, observation.rowsUpdated());
        assertSame(failure, observation.error());
    }

    @Test
    void eachSubscriptionPublishesSuccessOnlyAfterItsOwnInvalidation() {
        AtomicInteger invalidations = new AtomicInteger();
        AtomicInteger observations = new AtomicInteger();
        SchemaMigrationExecutor executor = executor(Mono.just(1L), event -> {
            assertEquals(observations.incrementAndGet(), invalidations.get());
            assertEquals(SqlExecutionStatus.SUCCESS, event.status());
            assertEquals(1, event.completedSteps());
        });
        Mono<SchemaMigrationResult> execution = executor.execute(reviewedPlan(REQUEST), List.of("orders"),
                ignored -> invalidations.incrementAndGet(), SqlExecutionOptions.unlimited());

        assertEquals(0, invalidations.get());
        assertEquals(1, execution.block().steps().size());
        assertEquals(1, execution.block().steps().size());
        assertEquals(2, observations.get());
        assertEquals(2, invalidations.get());
    }

    @Test
    void reactiveDdlTreatsAnUnavailableUpdateCountAsZero() {
        Long rows = executor(Mono.just(-1L)).execute(List.of(REQUEST)).block();

        assertEquals(0L, rows);
    }

    @Test
    void reviewedReactiveMigrationKeepsExecutedSteps() {
        SchemaMigrationExecutor executor = executor(Mono.just(1L));

        SchemaMigrationResult result = executor.execute(
                reviewedPlan(REQUEST),
                List.of("orders"),
                ignored -> { },
                SqlExecutionOptions.unlimited()).block();

        assertEquals(1, result.steps().size());
        assertSame(REQUEST, result.steps().getFirst().request());
    }

    @Test
    void jdbcDdlTreatsAnUnavailableUpdateCountAsZero() {
        SyncSqlExecutor sqlExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
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
                    com.flying.orm.rdb.execution.SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                SchemaMigrationObserver.noop());

        long rows = executor.execute(
                List.of(REQUEST), com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited());

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
                                    com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
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
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
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
        SyncSqlExecutor sqlExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
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
                    com.flying.orm.rdb.execution.SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                SchemaMigrationObserver.noop());

        RdbException thrown = assertThrows(
                RdbException.class,
                () -> executor.executeWithInvalidation(
                        List.of(REQUEST),
                        List.of("orders", "orders_audit"),
                        ignored -> {
                            int attempt = attempts.getAndIncrement();
                            throw attempt == 0 ? first : second;
                        },
                        com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited()));

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
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
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
        SyncSqlExecutor sqlExecutor = new com.flying.orm.rdb.support.ListQueryFixture() {
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
                    com.flying.orm.rdb.execution.SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                SchemaMigrationObserver.noop());

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> executor.executeWithInvalidation(
                        List.of(REQUEST),
                        List.of("orders"),
                        ignored -> { throw invalidationFailure; },
                        com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited()));

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
                                      com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
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
                                          com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
                                  .subscribe();
        subscription.dispose();

        assertEquals(1, invalidations.get());
    }

    @Test
    void reviewedRelationalCancellationAfterSqlStartsInvalidatesExactlyOnce() {
        com.flying.orm.core.metadata.RelationalTableDefinition before =
                SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID);
        com.flying.orm.core.metadata.RelationalTableDefinition desired =
                SchemaExecutionFixtures.table(
                        SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        SchemaMigrationPlan plan = SchemaExecutionFixtures.plan(snapshot.get(), desired, REQUEST);
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

        Disposable subscription = executor.execute(
                plan,
                SchemaMigrationSupport.metadataRelations(plan), ignored -> invalidations.incrementAndGet(),
                com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited())
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
                () -> executor.execute(
                                      reviewedPlan(new SqlRequest(
                                              "alter table orders comment "
                                                      + "/*flying-orm:mysql-comment-no-backslash-escapes*/ 'C:\\data'",
                                              List.of())),
                                      List.of("orders"),
                                      ignored -> invalidations.incrementAndGet(),
                                      SqlExecutionOptions.unlimited())
                              .block());

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(0, executions.get());
        assertEquals(0, invalidations.get());
    }

    private static SchemaMigrationExecutor executor(Mono<Long> execution) {
        return executor(execution, SchemaMigrationObserver.noop());
    }

    private static SchemaMigrationExecutor executor(Mono<Long> execution, SchemaMigrationObserver observer) {
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
                observer);
    }

    private static SchemaMigrationPlan reviewedPlan(SqlRequest request) {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target.toTableDefinition(), true,
                List.of(request),
                List.of());
        return migration;
    }

}
