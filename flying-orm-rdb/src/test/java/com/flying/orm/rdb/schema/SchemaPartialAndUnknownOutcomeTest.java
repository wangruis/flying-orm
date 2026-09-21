package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaPartialAndUnknownOutcomeTest {

    @Test
    void sqlSentRecordsAnAttemptEvenWhenConnectionAcquisitionFails() {
        SchemaSnapshot snapshot = SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID));
        ReviewedSchemaPlan plan = VerifiedSchemaPlanFixtures.plan(snapshot,
                VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE),
                new SqlRequest("alter table accounts add column note varchar(80)", List.of()));
        AtomicInteger acquisitions = new AtomicInteger();
        JdbcConnectionAccess jdbc = new JdbcConnectionAccess() {
            public java.sql.Connection getConnection(SqlRequest request) throws SQLException {
                acquisitions.incrementAndGet();
                throw new SQLException("acquisition failed");
            }
            public void releaseConnection(java.sql.Connection connection, SqlRequest request) {
                throw new AssertionError("unacquired connection must not be released");
            }
        };
        SchemaExecutionReport sync = VerifiedSchemaPlanExecutor.executeJdbc(plan,
                JdbcSqlExecutor.create(jdbc, RdbDialect.h2()), () -> snapshot,
                SchemaSnapshotCoverage::complete, () -> {}, SqlExecutionOptions.safeDefaults());
        assertEquals(1, acquisitions.get());

        acquisitions.set(0);
        R2dbcConnectionAccess reactive = new R2dbcConnectionAccess() {
                    public Publisher<? extends Connection> getConnection(SqlRequest request) {
                        return Mono.defer(() -> {
                            acquisitions.incrementAndGet();
                            return Mono.error(new IllegalStateException("acquisition failed"));
                        });
                    }
                    public Publisher<Void> releaseConnection(
                            SignalType signal, Connection connection, SqlRequest request) {
                        throw new AssertionError("unacquired connection must not be released");
                    }
                };
        SchemaExecutionReport async = VerifiedSchemaPlanExecutor.executeReactive(plan,
                R2dbcSqlExecutor.create(reactive, RdbDialect.h2()), () -> Mono.just(snapshot),
                SchemaSnapshotCoverage::complete, () -> {}, SqlExecutionOptions.safeDefaults()).block();
        assertEquals(1, acquisitions.get());
        for (SchemaExecutionReport report : List.of(sync, async)) {
            assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
            assertTrue(report.steps().getFirst().sqlSent());
            assertFalse(report.steps().getFirst().rowsUpdated().isPresent());
            assertFalse(report.successful());
        }
    }

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
    void reactiveStatementFailurePreservesTheCompletedSchemaStep() {
        ReactiveFailureFixture fixture = new ReactiveFailureFixture(false);

        SchemaExecutionReport report = fixture.execute();

        assertEquals(SchemaExecutionStatus.PARTIAL, report.status());
        assertEquals(2, report.steps().size());
        assertEquals(SchemaExecutionStatus.SUCCESS, report.steps().getFirst().status());
        assertEquals(1L, report.steps().getFirst().rowsUpdated().orElseThrow());
        assertEquals(SchemaExecutionStatus.UNKNOWN, report.steps().get(1).status());
        assertTrue(report.steps().get(1).sqlSent());
        assertEquals("IllegalStateException", report.steps().get(1).failureSummary().orElseThrow());
        assertFalse(report.verification().orElseThrow().compatible());
        fixture.assertResourcesReleased();
    }

    @Test
    void reactiveInvalidationFailurePreservesEveryCompletedSchemaStep() {
        ReactiveFailureFixture fixture = new ReactiveFailureFixture(true);

        SchemaExecutionReport report = fixture.execute();

        assertEquals(SchemaExecutionStatus.UNKNOWN, report.status());
        assertEquals(2, report.steps().size());
        for (SchemaExecutionReport.StepResult step : report.steps()) {
            assertEquals(SchemaExecutionStatus.SUCCESS, step.status());
            assertEquals(1L, step.rowsUpdated().orElseThrow());
            assertTrue(step.sqlSent());
        }
        assertTrue(report.verification().orElseThrow().compatible());
        fixture.assertResourcesReleased();
    }

    private static final class ReactiveFailureFixture {

        private final boolean invalidationFails;
        private final RelationalTableDefinition afterFirst = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        private final RelationalTableDefinition desired = VerifiedSchemaPlanFixtures.table(
                VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE, VerifiedSchemaPlanFixtures.TAG);
        private final AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(
                VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID)));
        private final List<String> executed = new ArrayList<>();
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger invalidations = new AtomicInteger();
        private final ReviewedSchemaPlan plan;

        private ReactiveFailureFixture(boolean invalidationFails) {
            this.invalidationFails = invalidationFails;
            RdbDialect dialect = RdbDialect.postgresql();
            plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of("PostgreSQL", "16", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
            assertEquals(2, plan.steps().size());
            assertFalse(plan.requiresManualAction());
        }

        private SchemaExecutionReport execute() {
            SchemaExecutionReport report = VerifiedSchemaPlanExecutor.executeReactive(
                            plan, R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(factory()), RdbDialect.postgresql()), () -> Mono.just(snapshot.get()),
                            SchemaSnapshotCoverage::complete, () -> {
                                invalidations.incrementAndGet();
                                if (invalidationFails) {
                                    throw new IllegalStateException("metadata invalidation failed");
                                }
                            }, SchemaMigrationExecutionOptions.defaults())
                    .block();
            assertNotNull(report);
            return report;
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
                    return Mono.fromSupplier(() -> {
                        acquisitions.incrementAndGet();
                        return connection;
                    });
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
                boolean failed = !invalidationFails && sql.equals(plan.requests().get(1).sql());
                Mono<Long> updated = failed
                        ? Mono.error(new IllegalStateException("connection lost after send"))
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

        private void assertResourcesReleased() {
            assertEquals(plan.requests().stream().map(SqlRequest::sql).toList(), executed);
            assertEquals(2, acquisitions.get());
            assertEquals(acquisitions.get(), closes.get());
            assertEquals(1, invalidations.get());
        }

        private static <T> T proxy(Class<T> type, InvocationHandler handler) {
            return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
        }
    }
}
