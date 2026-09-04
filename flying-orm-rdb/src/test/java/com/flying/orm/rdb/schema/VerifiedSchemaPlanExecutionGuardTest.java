package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
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
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VerifiedSchemaPlanExecutionGuardTest {

    @Test
    void directEntriesRetainUnknownReportWhenInvalidationFails() {
        for (boolean reactive : new boolean[]{false, true}) {
            for (boolean fullOptions : new boolean[]{false, true}) {
                Fixture fixture = new Fixture(RdbDialect.postgresql());
                fixture.invalidator = () -> { throw new IllegalStateException("cache invalidation failed"); };

                SchemaExecutionReport report = reactive
                        ? fixture.executeReactive(false, fullOptions, Duration.ZERO)
                        : fixture.executeJdbc(false, fullOptions, Duration.ZERO);

                assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
                assertFalse(report.successful());
                assertEquals(1, fixture.executions.get());
            }
        }
    }

    @Test
    void directEntriesDoNotTurnCleanupErrorsIntoReports() {
        for (boolean reactive : new boolean[]{false, true}) {
            for (boolean sqlFailed : new boolean[]{false, true}) {
                Fixture fixture = new Fixture(RdbDialect.postgresql());
                AssertionError fatal = new AssertionError("cache cleanup error");
                fixture.invalidator = () -> { throw fatal; };
                if (sqlFailed) {
                    fixture.executionFailure = new IllegalStateException("SQL failed");
                }
                if (reactive) {
                    assertSame(fatal, fixture.reactiveExecution(false, true, Duration.ZERO)
                            .materialize().block().getThrowable());
                } else {
                    assertSame(fatal, assertThrows(AssertionError.class,
                            () -> fixture.executeJdbc(false, true, Duration.ZERO)));
                }
            }
        }
    }

    @Test
    void directEntriesPreserveSqlErrorWhenCleanupAlsoFails() {
        for (boolean reactive : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            AssertionError fatal = new AssertionError("SQL error");
            fixture.executionFailure = fatal;
            fixture.invalidator = () -> { throw new IllegalStateException("cache cleanup failed"); };

            if (reactive) {
                assertSame(fatal, fixture.reactiveExecution(false, true, Duration.ZERO)
                        .materialize().block().getThrowable());
            } else {
                assertSame(fatal, assertThrows(AssertionError.class,
                        () -> fixture.executeJdbc(false, true, Duration.ZERO)));
            }
        }
    }

    @Test
    void directJdbcEntryReportsExternalTransactionPending() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            SchemaExecutionReport report = fixture.executeJdbc(true, fullOptions, Duration.ZERO);

            assertEquals(SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING, report.status());
            assertFalse(report.successful());
            assertEquals(1, fixture.completionRegistrations.get());
        }
    }

    @Test
    void directReactiveEntryReportsExternalTransactionPending() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            SchemaExecutionReport report = fixture.executeReactive(true, fullOptions, Duration.ZERO);

            assertEquals(SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING, report.status());
            assertFalse(report.successful());
            assertEquals(1, fixture.completionRegistrations.get());
        }
    }

    @Test
    void directJdbcEntryRejectsImplicitCommitInExternalTransaction() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.mysql());
            SchemaMigrationRejectedException failure = assertThrows(
                    SchemaMigrationRejectedException.class,
                    () -> fixture.executeJdbc(true, fullOptions, Duration.ZERO));

            assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
            assertEquals(0, fixture.executions.get());
        }
    }

    @Test
    void directReactiveEntryRejectsImplicitCommitInExternalTransaction() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.mysql());
            SchemaMigrationRejectedException failure = assertThrows(
                    SchemaMigrationRejectedException.class,
                    () -> fixture.executeReactive(true, fullOptions, Duration.ZERO));

            assertEquals(SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED, failure.failureCode());
            assertEquals(0, fixture.executions.get());
        }
    }

    @Test
    void directJdbcEntryRequiresConnectionCapabilityForLockTimeout() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> fixture.executeJdbc(false, true, Duration.ofSeconds(1)));

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(0, fixture.executions.get());
    }

    @Test
    void directReactiveEntryRequiresConnectionCapabilityForLockTimeout() {
        Fixture fixture = new Fixture(RdbDialect.postgresql());
        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> fixture.executeReactive(false, true, Duration.ofSeconds(1)));

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(0, fixture.executions.get());
    }

    private static final class Fixture {
        private final RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        private final AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(
                SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID)));
        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicInteger completionRegistrations = new AtomicInteger();
        private final ReviewedSchemaPlan plan;
        private Runnable invalidator = () -> { };
        private Throwable executionFailure;

        private Fixture(RdbDialect dialect) {
            plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of(dialect.name(), "test", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.SAFE_INCREMENTAL);
        }

        private SchemaExecutionReport executeJdbc(boolean external, boolean fullOptions, Duration timeout) {
            SyncSqlExecutor executor = new SyncSqlExecutor() {
                @Override
                public Optional<JdbcTransactionContext> currentTransaction() {
                    return external ? Optional.of(JdbcTransactionContext.external(
                            connection(java.sql.Connection.class), listener -> {
                                completionRegistrations.incrementAndGet();
                                return true;
                            })) : Optional.empty();
                }

                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("query must not execute");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return execute();
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                    throw new AssertionError("generated keys must not execute");
                }
            };
            return fullOptions ? VerifiedSchemaPlanExecutor.executeJdbc(
                    plan, executor, snapshot::get, SchemaSnapshotCoverage::complete,
                    invalidator, options(timeout)) : VerifiedSchemaPlanExecutor.executeJdbc(
                    plan, executor, snapshot::get, SchemaSnapshotCoverage::complete,
                    invalidator, SqlExecutionOptions.safeDefaults());
        }

        private SchemaExecutionReport executeReactive(boolean external, boolean fullOptions, Duration timeout) {
            return reactiveExecution(external, fullOptions, timeout).block();
        }

        private Mono<SchemaExecutionReport> reactiveExecution(
                boolean external, boolean fullOptions, Duration timeout) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                @Override
                public Mono<R2dbcTransactionContext> currentTransaction() {
                    return external ? Mono.just(R2dbcTransactionContext.external(
                            connection(io.r2dbc.spi.Connection.class), listener -> {
                                completionRegistrations.incrementAndGet();
                                return true;
                            })) : Mono.empty();
                }

                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("query must not execute"));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(Fixture.this::execute);
                }
            };
            return fullOptions ? VerifiedSchemaPlanExecutor.executeReactive(
                    plan, executor, () -> Mono.just(snapshot.get()), SchemaSnapshotCoverage::complete,
                    invalidator, options(timeout)) : VerifiedSchemaPlanExecutor.executeReactive(
                    plan, executor, () -> Mono.just(snapshot.get()), SchemaSnapshotCoverage::complete,
                    invalidator, SqlExecutionOptions.safeDefaults());
        }

        private long execute() {
            executions.incrementAndGet();
            if (executionFailure instanceof RuntimeException failure) {
                throw failure;
            }
            if (executionFailure instanceof Error failure) {
                throw failure;
            }
            snapshot.set(SchemaSnapshot.present(desired));
            return 0L;
        }
    }

    private static SchemaMigrationExecutionOptions options(Duration timeout) {
        return new SchemaMigrationExecutionOptions(SqlExecutionOptions.safeDefaults(), null, timeout);
    }

    private static <T> T connection(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, arguments) -> {
                    throw new AssertionError("connection must remain externally owned: " + method);
                }));
    }
}
