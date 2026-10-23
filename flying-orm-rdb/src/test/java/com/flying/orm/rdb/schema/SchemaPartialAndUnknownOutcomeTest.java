package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SchemaPartialAndUnknownOutcomeTest {

    @Test
    void reportsDefaultReplacementFailureWithoutPretendingTheDropWasRolledBack() {
        RdbDialect dialect = RdbDialect.sqlServer();
        RelationalTableDefinition before = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(32)
                        .defaultValue(ColumnDefault.literal("old")).defaultConstraintName("DF_note").build());
        RelationalTableDefinition afterDrop = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(32).build());
        RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(32)
                        .defaultValue(ColumnDefault.literal("new")).defaultConstraintName("DF_note").build());
        for (boolean reactive : List.of(false, true)) {
            AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
            SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of("SQL Server", "test", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
            assertEquals(2, plan.requests().size());
            List<SqlRequest> attempts = new ArrayList<>();
            java.util.function.Function<SqlRequest, Long> execute = request -> {
                attempts.add(request);
                if (attempts.size() == 1) {
                    snapshot.set(SchemaSnapshot.present(afterDrop));
                    return 0L;
                }
                throw new IllegalStateException("add default failed");
            };
            SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                    ;
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                    () -> { if (reactive) SchemaExecutionFixtures.executeReactive(plan,
                            new SchemaExecutionFixtures.ReactiveExecutor(request -> Mono.fromSupplier(() -> execute.apply(request))), () -> {}, options).block();
                    else SchemaExecutionFixtures.executeJdbc(plan, new SchemaExecutionFixtures.SyncExecutor(execute), () -> {}, options); });
            assertEquals(plan.requests(), attempts);
            assertEquals(1, failure.stepIndex());
            assertEquals(List.of(plan.requests().getFirst()),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void combinedColumnChangeKeepsCompletedTypeStepWhenNullabilityFails() {
        RdbDialect dialect = RdbDialect.postgresql();
        RelationalTableDefinition before = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(32).nullable(false).build());
        RelationalTableDefinition afterType = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(64).nullable(false).build());
        RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                ColumnDefinition.builder("note", "VARCHAR").length(64).nullable(true).build());
        for (boolean reactive : List.of(false, true)) {
            AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(before));
            SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of("PostgreSQL", "16", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
            assertEquals(2, plan.requests().size());
            List<SqlRequest> attempts = new ArrayList<>();
            java.util.function.Function<SqlRequest, Long> execute = request -> {
                attempts.add(request);
                if (attempts.size() == 1) {
                    snapshot.set(SchemaSnapshot.present(afterType));
                    return 0L;
                }
                throw new IllegalStateException("second DDL failed");
            };
            SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                    ;
            SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                    () -> { if (reactive) SchemaExecutionFixtures.executeReactive(plan,
                            new SchemaExecutionFixtures.ReactiveExecutor(request -> Mono.fromSupplier(() -> execute.apply(request))), () -> {}, options).block();
                    else SchemaExecutionFixtures.executeJdbc(plan, new SchemaExecutionFixtures.SyncExecutor(execute), () -> {}, options); });
            assertEquals(plan.requests(), attempts);
            assertEquals(1, failure.stepIndex());
            assertEquals(List.of(plan.requests().getFirst()),
                    failure.completedWorkSteps().stream().map(step -> step.request()).toList());
        }
    }

    @Test
    void sqlSentRecordsAnAttemptEvenWhenConnectionAcquisitionFails() {
        SchemaSnapshot snapshot = SchemaSnapshot.present(SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID));
        SchemaMigrationPlan plan = SchemaExecutionFixtures.plan(snapshot,
                SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE),
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
        assertThrows(SqlExecutionSequenceException.class, () -> SchemaExecutionFixtures.executeJdbc(plan,
                JdbcSqlExecutor.create(jdbc, RdbDialect.h2()), () -> {}, SqlExecutionOptions.unlimited()));
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
        assertThrows(SqlExecutionSequenceException.class, () -> SchemaExecutionFixtures.executeReactive(plan,
                R2dbcSqlExecutor.create(reactive, RdbDialect.h2()), () -> {}, SqlExecutionOptions.unlimited()).block());
        assertEquals(1, acquisitions.get());

    }





    @Test
    void reactiveStatementFailurePreservesTheCompletedSchemaStep() {
        ReactiveFailureFixture fixture = new ReactiveFailureFixture(false);
        SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class, fixture::execute);
        assertEquals(1, failure.stepIndex());
        assertEquals(1, failure.completedWorkSteps().size());
        assertEquals(1L, failure.completedWorkSteps().getFirst().rowsUpdated());
        assertEquals("connection lost after send", failure.getCause().getMessage());
        fixture.assertResourcesReleased();
    }

    @Test
    void reactiveInvalidationFailurePreservesEveryCompletedSchemaStep() {
        ReactiveFailureFixture fixture = new ReactiveFailureFixture(true);
        SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class, fixture::execute);
        assertEquals("metadata invalidation failed", failure.getCause().getCause().getMessage());
        assertEquals(2, failure.completedWorkSteps().size());
        fixture.assertResourcesReleased();
    }

    private static final class ReactiveFailureFixture {

        private final boolean invalidationFails;
        private final RelationalTableDefinition afterFirst = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE);
        private final RelationalTableDefinition desired = SchemaExecutionFixtures.table(
                SchemaExecutionFixtures.ID, SchemaExecutionFixtures.NOTE, SchemaExecutionFixtures.TAG);
        private final AtomicReference<SchemaSnapshot> snapshot = new AtomicReference<>(SchemaSnapshot.present(
                SchemaExecutionFixtures.table(SchemaExecutionFixtures.ID)));
        private final List<String> executed = new ArrayList<>();
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final AtomicInteger invalidations = new AtomicInteger();
        private final SchemaMigrationPlan plan;

        private ReactiveFailureFixture(boolean invalidationFails) {
            this.invalidationFails = invalidationFails;
            RdbDialect dialect = RdbDialect.postgresql();
            plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of("PostgreSQL", "16", dialect), desired, snapshot.get(),
                    SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
            assertEquals(2, plan.steps().size());
        }

        private SchemaMigrationResult execute() {
            SchemaMigrationResult report = SchemaExecutionFixtures.executeReactive(
                            plan, R2dbcSqlExecutor.create(ConnectionAccessTestSupport.reactive(factory()), RdbDialect.postgresql()), () -> {
                                invalidations.incrementAndGet();
                                if (invalidationFails) {
                                    throw new IllegalStateException("metadata invalidation failed");
                                }
                            }, SqlExecutionOptions.unlimited())
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
