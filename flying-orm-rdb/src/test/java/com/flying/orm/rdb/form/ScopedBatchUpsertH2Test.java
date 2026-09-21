package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ExternalTransactionTestSupport;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.mapping.FlyingTenant;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScopedBatchUpsertH2Test {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final SqlRenderer CONDITIONS = SqlRenderer.builder().addDefaultTerms()
            .addTerm(SqlTermHandler.of("deny-target", (term, context) -> SqlFragment.of("1 = 0"))).build();
    private static final DynamicForm FORM = DynamicForm.builder("scoped_rows", "scoped_rows")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("org_id", "VARCHAR"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .tenant("org_id", TenantStrategy.AUTO).build();
    private static final DataScope MATCH_OLD_NAME = DataScope.where(
            ConditionGroup.and().where("name", "=", "old").build());

    @TestFactory
    Stream<DynamicTest> updatesTheScopedConflictTargetAndInsertsNewRowsOnEveryPublicChannel() {
        return Stream.of(false, true).flatMap(reactive -> channels().map(channel -> DynamicTest.dynamicTest(
                "reactive=" + reactive + " " + channel, () -> {
                    try (Database database = new Database()) {
                        Object result = database.execute(spec(List.of(row(1L), row(4L)), channel), reactive, channel);

                        assertSuccessful(result);
                        assertEquals(List.of("A", "changed"), database.row(1L));
                        assertEquals(List.of("A", "changed"), database.row(4L));
                        assertEquals(List.of("B", "old"), database.row(2L));
                        assertEquals(List.of("A", "other"), database.row(3L));
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> falseAndUnknownTargetScopesFailAtTheDatabaseForFirstAndLaterRows() {
        return Stream.of(false, true).flatMap(reactive -> channels().flatMap(channel ->
                Stream.of(false, true).map(later -> DynamicTest.dynamicTest(
                        "reactive=" + reactive + " " + channel + " later=" + later, () -> {
                            // 2 violates the default tenant, 3 the explicit scope, and 5 yields SQL UNKNOWN.
                            for (long blockedId : List.of(2L, 3L, 5L)) {
                                try (Database database = new Database()) {
                                    List<String> before = database.row(blockedId);
                                    List<Map<String, Object>> rows = later
                                            ? List.of(row(1L), row(blockedId)) : List.of(row(blockedId));
                                    BatchSpec request = spec(rows, channel);

                                    BatchExecutionEvidenceException failure = assertThrows(
                                            BatchExecutionEvidenceException.class,
                                            () -> database.execute(request, reactive, channel));
                                    assertDatabaseTargetFailure(failure, reactive);
                                    if (later) {
                                        assertEquals(List.of("A", "old"), database.row(1L),
                                                "the test caller rolls back its transaction");
                                    }
                                    assertEquals(before, database.row(blockedId), "the conflicting target must survive");
                                }
                            }
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> constantFalseScopeRejectsConflictsWithoutRejectingTheInsertBranch() {
        // 启动期受信 term 可以返回恒假谓词；它仍只约束冲突更新，不能阻止本次无冲突新增。
        DataScope scope = DataScope.where(ConditionGroup.and(CONDITIONS.terms())
                .where("id", "deny-target", true).build());
        return Stream.of(false, true).flatMap(reactive -> channels().map(channel -> DynamicTest.dynamicTest(
                "constant scope reactive=" + reactive + " " + channel, () -> {
                    try (Database database = new Database()) {
                        BatchSpec insert = spec(List.of(row(4L)), channel).withScope(scope);
                        assertSuccessful(database.execute(insert, reactive, channel));
                        assertEquals(List.of("A", "changed"), database.row(4L));

                        BatchSpec conflict = spec(List.of(row(1L)), channel).withScope(scope);
                        assertThrows(BatchExecutionEvidenceException.class,
                                () -> database.execute(conflict, reactive, channel));
                        assertEquals(List.of("A", "old"), database.row(1L));
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> keyOnlyScopedUpsertLeavesAnExistingOutOfScopeRowUntouched() {
        DynamicForm keys = DynamicForm.builder("scoped_rows", "scoped_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive, () -> {
            try (Database database = new Database()) {
                BatchSpec request = BatchSpec.upsert(keys, Flux.just(Map.of("id", 2L), Map.of("id", 9L)))
                        .withScope(DataScope.where(ConditionGroup.and().where("org_id", "=", "A").build()));
                Object result = database.external(reactive,
                        () -> reactive ? database.base.forms().writeBatch(request).block(TIMEOUT)
                                : database.base.syncForms().writeBatch(request));

                assertSuccessful(result);
                assertEquals(List.of("B", "old"), database.row(2L));
                assertEquals(java.util.Arrays.asList(null, null), database.row(9L));
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> repositoriesUseTheSameNonPrimaryTenantTargetBoundary() {
        return Stream.of(false, true).map(reactive ->
                DynamicTest.dynamicTest("repository reactive=" + reactive, () -> {
                    try (Database database = new Database()) {
                        List<ScopedEntity> allowed = List.of(new ScopedEntity(1L, "A", "changed"),
                                new ScopedEntity(4L, "A", "inserted"));
                        assertSuccessful(database.executeRepository(allowed, reactive));
                        assertEquals(List.of("A", "changed"), database.row(1L));
                        assertEquals(List.of("A", "inserted"), database.row(4L));

                        List<ScopedEntity> denied = List.of(new ScopedEntity(2L, "A", "stolen"));
                        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                                () -> database.executeRepository(denied, reactive));
                        assertDatabaseTargetFailure(failure, reactive);
                        assertEquals(List.of("B", "old"), database.row(2L));
                    }
                }));
    }

    private static Stream<String> channels() {
        return Stream.of("result", "evidence");
    }

    private static BatchSpec spec(List<Map<String, Object>> rows, String channel) {
        return BatchSpec.upsert(FORM, Flux.fromIterable(rows)).withScope(MATCH_OLD_NAME)
                .withOptions(BatchWriteOptions.of(2));
    }

    private static Map<String, Object> row(long id) {
        return Map.of("id", id, "org_id", "A", "name", "changed");
    }

    private static void assertSuccessful(Object value) {
        BatchExecutionEvidence evidence = assertInstanceOf(BatchExecutionEvidence.class, value);
        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
    }

    private static void assertDatabaseTargetFailure(BatchExecutionEvidenceException failure, boolean reactive) {
        assertNotEquals(BatchExecutionState.SUCCESS, failure.evidence().state());
        assertNotNull(failure.evidence().failure());
        assertNotNull(failure.evidence().failure().sqlState(), "target guard must fail in the database");
    }

    @TableName("scoped_rows")
    @FlyingTenant(field = "org_id")
    private record ScopedEntity(@TableId(type = IdType.INPUT) Long id,
                                @TableField("org_id") String orgId, String name) {
    }

    private static final class Database implements AutoCloseable {
        private final Connection keeper;
        private final FlyingOrmClients base;
        private final FlyingOrmClients scoped;
        private final JdbcDataSource source;
        private final ConnectionFactory factory;
        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcTransaction;
        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveTransaction;

        private Database() throws Exception {
            String name = "scoped_upsert_" + UUID.randomUUID().toString().replace("-", "");
            source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
            source.setUser("sa");
            source.setPassword("");
            keeper = source.getConnection();
            try (Statement statement = keeper.createStatement()) {
                statement.execute("create table scoped_rows (id bigint primary key, org_id varchar(32), name varchar(64))");
                statement.execute("insert into scoped_rows values (1, 'A', 'old'), (2, 'B', 'old'),"
                        + " (3, 'A', 'other'), (5, null, 'old')");
            }
            factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                    .option(DATABASE, name).option(USER, "sa").option(PASSWORD, "")
                    .option(H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
            base = FlyingOrmClients.builder(jdbcAccess(), reactiveAccess()).configuredDialect("h2").renderer(CONDITIONS).build();
            scoped = base.withDefaultDataScope(DataScope.tenant("org_id", "A"));
        }

        private com.flying.orm.rdb.jdbc.JdbcConnectionAccess jdbcAccess() {
            var normal = com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source);
            return new com.flying.orm.rdb.jdbc.JdbcConnectionAccess() {
                public java.sql.Connection getConnection(com.flying.orm.core.sql.render.SqlRequest request)
                        throws java.sql.SQLException {
                    return (jdbcTransaction == null ? normal : jdbcTransaction).getConnection(request);
                }
                public void releaseConnection(java.sql.Connection connection,
                        com.flying.orm.core.sql.render.SqlRequest request) throws java.sql.SQLException {
                    (jdbcTransaction == null ? normal : jdbcTransaction).releaseConnection(connection, request);
                }
            };
        }

        private com.flying.orm.rdb.reactive.R2dbcConnectionAccess reactiveAccess() {
            var normal = com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(factory);
            return new com.flying.orm.rdb.reactive.R2dbcConnectionAccess() {
                public org.reactivestreams.Publisher<? extends io.r2dbc.spi.Connection> getConnection(
                        com.flying.orm.core.sql.render.SqlRequest request) {
                    return (reactiveTransaction == null ? normal : reactiveTransaction).getConnection(request);
                }
                public org.reactivestreams.Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                        io.r2dbc.spi.Connection connection, com.flying.orm.core.sql.render.SqlRequest request) {
                    return (reactiveTransaction == null ? normal : reactiveTransaction)
                            .releaseConnection(signal, connection, request);
                }
            };
        }

        private Object execute(BatchSpec request, boolean reactive, String channel) throws Exception {
            return external(reactive, () -> executeCurrent(request, reactive, channel));
        }

        private Object executeCurrent(BatchSpec request, boolean reactive, String channel) {
            if (reactive) {
                return switch (channel) {
                    case "result" -> scoped.forms().writeBatch(request).block(TIMEOUT);
                    case "evidence" -> scoped.forms().writeBatchEvidence(request).block(TIMEOUT);
                    default -> throw new AssertionError(channel);
                };
            }
            return switch (channel) {
                case "result" -> scoped.syncForms().writeBatch(request);
                case "evidence" -> scoped.syncForms().writeBatchEvidence(request);
                default -> throw new AssertionError(channel);
            };
        }

        private Object executeRepository(List<ScopedEntity> values, boolean reactive) throws Exception {
            return external(reactive, () -> executeRepositoryCurrent(values, reactive));
        }

        private Object executeRepositoryCurrent(List<ScopedEntity> values, boolean reactive) {
            BatchWriteOptions options = BatchWriteOptions.of(2);
            if (reactive) {
                var repository = scoped.repository(ScopedEntity.class);
                return repository.upsertBatch(Flux.fromIterable(values), options).block(TIMEOUT);
            }
            var repository = scoped.syncRepository(ScopedEntity.class);
            return repository.upsertBatch(Flux.fromIterable(values), options);
        }

        private <T> T external(boolean reactive, Supplier<T> operation) throws Exception {
            try {
                return reactive ? ExternalTransactionTestSupport.reactive(factory, transaction -> {
                    reactiveTransaction = transaction;
                    return Mono.fromSupplier(operation);
                }) : ExternalTransactionTestSupport.jdbc(source, transaction -> {
                    jdbcTransaction = transaction;
                    return operation.get();
                });
            } finally {
                jdbcTransaction = null;
                reactiveTransaction = null;
            }
        }

        private List<String> row(long id) throws Exception {
            try (Statement statement = keeper.createStatement();
                 ResultSet rows = statement.executeQuery("select org_id, name from scoped_rows where id = " + id)) {
                assertTrue(rows.next(), "expected row " + id);
                List<String> value = java.util.Arrays.asList(rows.getString(1), rows.getString(2));
                assertFalse(rows.next());
                return value;
            }
        }

        @Override
        public void close() throws Exception {
            scoped.close();
            base.close();
            keeper.close();
        }
    }
}
