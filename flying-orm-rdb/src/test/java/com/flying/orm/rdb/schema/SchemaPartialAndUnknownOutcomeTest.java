package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaPartialAndUnknownOutcomeTest {

    @Test
    void reportsCompletedPrefixAndRedactsTheAmbiguousFailure() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition afterFirst = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID,
                VerifiedSchemaPlanFixtures.NOTE,
                VerifiedSchemaPlanFixtures.TAG);
        AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot.get(),
                desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()),
                new SqlRequest("alter table accounts add column tag varchar(40)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    if (request.sql().contains("note")) {
                        snapshot.set(SchemaSnapshot.present(afterFirst));
                        return 1L;
                    }
                    throw new IllegalStateException(
                            "password=secret; alter table accounts add column tag varchar(40)");
                });

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan, executor, snapshot::get, SchemaSnapshotCoverage::complete, () -> {
                }, SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.PARTIAL, report.status());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().get(0).status());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().get(1).status());
        assertEquals("IllegalStateException",
                     report.steps().get(1).failureSummary().orElseThrow());
        assertFalse(report.steps().get(1).failureSummary().orElseThrow().contains("secret"));
    }

    @Test
    void reportsUnknownWhenTheFirstSentStatementHasNoCertainOutcome() {
        RelationalTableDefinition before = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID);
        RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        SchemaSnapshot snapshot = SchemaSnapshot.present(before);
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(
                snapshot, desired,
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        VerifiedSchemaPlanFixtures.SyncExecutor executor =
                new VerifiedSchemaPlanFixtures.SyncExecutor(request -> {
                    throw new IllegalStateException("connection lost after send");
                });

        SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeJdbc(
                plan, executor, () -> snapshot, SchemaSnapshotCoverage::complete, () -> {
                }, SqlExecutionOptions.safeDefaults());

        assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().getFirst().status());
        assertEquals(1, executor.requests.size());
    }

    @Test
    void reactiveSequenceWorkDeadlinePreservesTheCompletedSchemaStep() {
        SequenceDeadlineFixture fixture = new SequenceDeadlineFixture(false);

        SchemaExecutionReport report = fixture.execute();

        assertEquals(SchemaExecutionStatus.PARTIAL, report.status());
        assertEquals(2, report.steps().size());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
        assertEquals(1L, report.steps().getFirst().rowsUpdated().orElseThrow());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().get(1).status());
        assertTrue(report.steps().get(1).sqlSent());
        assertFalse(report.verification().orElseThrow().compatible());
        fixture.assertSingleCleanupAndClose();
    }

    @Test
    void reactiveSequenceCleanupDeadlinePreservesEveryCompletedSchemaStep() {
        SequenceDeadlineFixture fixture = new SequenceDeadlineFixture(true);

        SchemaExecutionReport report = fixture.execute();

        assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
        assertEquals(2, report.steps().size());
        for (SchemaExecutionReport.StepResult step : report.steps()) {
            assertEquals(SchemaExecutionStatus.SUCCESS, step.status());
            assertEquals(1L, step.rowsUpdated().orElseThrow());
            assertTrue(step.sqlSent());
        }
        assertTrue(report.verification().orElseThrow().compatible());
        fixture.assertSingleCleanupAndClose();
    }

    private static final class SequenceDeadlineFixture {

        private final boolean cleanupPending;
        private final RelationalTableDefinition afterFirst = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        private final RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE, VerifiedSchemaPlanFixtures.TAG);
        private final AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(
                VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID)));
        private final List<String> executed = new ArrayList<>();
        private final AtomicInteger pendingSubscriptions = new AtomicInteger();
        private final AtomicInteger pendingCancellations = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger invalidations = new AtomicInteger();
        private final ReviewedSchemaPlan plan;

        private SequenceDeadlineFixture(boolean cleanupPending) {
            this.cleanupPending = cleanupPending;
            RdbDialect dialect = RdbDialect.postgresql();
            plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of("PostgreSQL", "16", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
            assertEquals(2, plan.steps().size());
            assertFalse(plan.requiresManualAction());
        }

        private SchemaExecutionReport execute() {
            SqlExecutionOptions sqlOptions = SqlExecutionOptions.safeDefaults()
                    .withTimeout(cleanupPending ? Duration.ZERO : Duration.ofSeconds(30))
                    .withCleanupTimeout(cleanupPending ? Duration.ofSeconds(30) : Duration.ZERO);
            SchemaMigrationExecutionOptions options = new SchemaMigrationExecutionOptions(sqlOptions, null)
                    .withLockTimeout(Duration.ofSeconds(1));
            AtomicReference<Runnable> deadline = new AtomicReference<>();
            AtomicReference<SchemaExecutionReport> report = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            String hook = getClass().getName() + ".schemaSequenceDeadline";
            Schedulers.onScheduleHook(hook, task -> {
                deadline.compareAndSet(null, task);
                return task;
            });
            Disposable subscription = null;
            try {
                subscription = VerifiedSchemaPlanExecutor.executeReactive(
                                plan, R2dbcSqlExecutor.create(factory()), () -> Mono.just(snapshot.get()),
                                SchemaSnapshotCoverage::complete, invalidations::incrementAndGet, options)
                        .subscribe(report::set, failure::set);
                assertEquals(1, pendingSubscriptions.get());
                assertNotNull(deadline.get());
                // Trigger the real timer only after the intended statement is pending; no wall-clock sleep.
                deadline.get().run();
                assertNull(failure.get());
                assertNotNull(report.get());
                return report.get();
            } finally {
                if (subscription != null) {
                    subscription.dispose();
                }
                Schedulers.resetOnScheduleHook(hook);
            }
        }

        private ConnectionFactory factory() {
            Connection connection = proxy(Connection.class, (object, method, arguments) -> switch (method.getName()) {
                case "createStatement" -> statement((String) arguments[0]);
                case "close" -> Mono.fromRunnable(closes::incrementAndGet);
                default -> throw new UnsupportedOperationException(method.getName());
            });
            return new ConnectionFactory() {
                @Override
                public Publisher<? extends Connection> create() {
                    return Mono.just(connection);
                }

                @Override
                public ConnectionFactoryMetadata getMetadata() {
                    return () -> "PostgreSQL";
                }
            };
        }

        private Statement statement(String sql) {
            return proxy(Statement.class, (object, method, arguments) -> {
                if (!method.getName().equals("execute")) {
                    throw new UnsupportedOperationException(method.getName());
                }
                executed.add(sql);
                boolean pending = cleanupPending ? sql.equals("reset lock_timeout")
                        : sql.equals(plan.requests().get(1).sql());
                Mono<Long> updated = pending ? Mono.<Long>never()
                        .doOnSubscribe(ignored -> pendingSubscriptions.incrementAndGet())
                        .doOnCancel(pendingCancellations::incrementAndGet)
                        : Mono.fromSupplier(() -> {
                            if (sql.equals(plan.requests().getFirst().sql())) {
                                snapshot.set(SchemaSnapshot.present(afterFirst));
                            } else if (sql.equals(plan.requests().get(1).sql())) {
                                snapshot.set(SchemaSnapshot.present(desired));
                            }
                            return 1L;
                        });
                Result result = proxy(Result.class, (value, resultMethod, resultArguments) -> {
                    if (resultMethod.getName().equals("getRowsUpdated")) {
                        return updated;
                    }
                    throw new UnsupportedOperationException(resultMethod.getName());
                });
                return Flux.just(result);
            });
        }

        private void assertSingleCleanupAndClose() {
            assertEquals(List.of("set lock_timeout = '1000ms'", plan.requests().getFirst().sql(),
                    plan.requests().get(1).sql(), "reset lock_timeout"), executed);
            assertEquals(1, pendingCancellations.get());
            assertEquals(1, closes.get());
            assertEquals(1, invalidations.get());
        }

        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
        }
    }
}
