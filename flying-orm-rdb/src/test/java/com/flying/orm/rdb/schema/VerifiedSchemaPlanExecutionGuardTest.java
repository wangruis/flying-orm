package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VerifiedSchemaPlanExecutionGuardTest {

    @Test
    void directEntriesPropagateInvalidationFailure() {
        for (boolean reactive : new boolean[]{false, true}) {
            for (boolean fullOptions : new boolean[]{false, true}) {
                Fixture fixture = new Fixture(RdbDialect.postgresql());
                IllegalStateException failure = new IllegalStateException("cache invalidation failed");
                fixture.invalidator = () -> { throw failure; };
                assertSame(failure, assertThrows(SqlExecutionSequenceException.class, () -> {
                    if (reactive) fixture.executeReactive(fullOptions);
                    else fixture.executeJdbc(fullOptions);
                }).getCause().getCause());
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
                    assertSame(fatal, fixture.reactiveExecution(true)
                            .materialize().block().getThrowable());
                } else {
                    assertSame(fatal, assertThrows(AssertionError.class,
                            () -> fixture.executeJdbc(true)));
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
                assertSame(fatal, fixture.reactiveExecution(true)
                        .materialize().block().getThrowable());
            } else {
                assertSame(fatal, assertThrows(AssertionError.class,
                        () -> fixture.executeJdbc(true)));
            }
        }
    }

    @Test
    void directJdbcEntryReportsExecutedFactsWithoutInspectingTransactions() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            SchemaMigrationResult report = fixture.executeJdbc(fullOptions);

            assertNotNull(report);
            assertEquals(1, fixture.invalidations.get());
        }
    }

    @Test
    void directReactiveEntryReportsExecutedFactsWithoutInspectingTransactions() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.postgresql());
            SchemaMigrationResult report = fixture.executeReactive(fullOptions);

            assertNotNull(report);
            assertEquals(1, fixture.invalidations.get());
        }
    }

    @Test
    void directJdbcEntryExecutesMysqlWithoutTransactionAdmission() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.mysql());
            SchemaMigrationResult report = fixture.executeJdbc(fullOptions);
            assertNotNull(report);
            assertEquals(1, fixture.executions.get());
            assertEquals(1, fixture.invalidations.get());
        }
    }

    @Test
    void directReactiveEntryExecutesMysqlWithoutTransactionAdmission() {
        for (boolean fullOptions : new boolean[]{false, true}) {
            Fixture fixture = new Fixture(RdbDialect.mysql());
            SchemaMigrationResult report = fixture.executeReactive(fullOptions);
            assertNotNull(report);
            assertEquals(1, fixture.executions.get());
            assertEquals(1, fixture.invalidations.get());
        }
    }



    private static final class Fixture {
        private final RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        private final AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(
                SchemaSnapshot.present(SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID)));
        private final AtomicInteger executions = new AtomicInteger();
        private final SchemaMigrationPlan plan;
        private final AtomicInteger invalidations = new AtomicInteger();
        private Runnable invalidator = invalidations::incrementAndGet;
        private Throwable executionFailure;

        private Fixture(RdbDialect dialect) {
            plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of(dialect.name(), "test", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.SAFE_INCREMENTAL);
        }

        private SchemaMigrationResult executeJdbc(boolean fullOptions) {
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                @Override
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("query must not execute");
                }

                @Override
                public long rowsUpdated(SqlRequest request) {
                    return execute();
                }

                @Override
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("generated keys must not execute");
                }
            };
            return fullOptions ? SchemaExecutionFixtures.executeJdbc(
                    plan, executor,
                    invalidator, SqlExecutionOptions.unlimited()) : SchemaExecutionFixtures.executeJdbc(
                    plan, executor,
                    invalidator, SqlExecutionOptions.unlimited());
        }

        private SchemaMigrationResult executeReactive(boolean fullOptions) {
            return reactiveExecution(fullOptions).block();
        }

        private Mono<SchemaMigrationResult> reactiveExecution(
                boolean fullOptions) {
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.error(new AssertionError("query must not execute"));
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.fromSupplier(Fixture.this::execute);
                }
            };
            return fullOptions ? SchemaExecutionFixtures.executeReactive(
                    plan, executor,
                    invalidator, SqlExecutionOptions.unlimited()) : SchemaExecutionFixtures.executeReactive(
                    plan, executor,
                    invalidator, SqlExecutionOptions.unlimited());
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



}
