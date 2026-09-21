package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.execution.ExternalTransactionTestSupport;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2UuidNullBindingTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final UUID ID = UUID.fromString("c037c161-5f5c-4b3a-b1bd-0dfb48808a01");
    private static final DynamicForm FORM = DynamicForm.builder("uuid_rows", "uuid_rows")
            .addField(DynamicField.primaryKey("id", "UUID"))
            .addField(DynamicField.of("token", "UUID"))
            .addField(DynamicField.of("version", "BIGINT"))
            .build();

    @TestFactory
    Stream<DynamicTest> reactiveWritesBindNativeUuidNulls() {
        return Stream.of("insert", "update", "batch-insert", "batch-update").map(operation ->
                DynamicTest.dynamicTest(operation, () -> {
                    String database = "uuid_null_" + UUID.randomUUID().toString().replace("-", "");
                    JdbcDataSource dataSource = new JdbcDataSource();
                    dataSource.setURL("jdbc:h2:mem:" + database + ";DATABASE_TO_LOWER=TRUE");
                    dataSource.setUser("sa");
                    dataSource.setPassword("");
                    ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                            .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                            .option(DATABASE, database).option(USER, "sa").option(PASSWORD, "")
                            .option(H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
                    // The keeper owns this in-process database; closing it discards all test rows.
                    try (Connection keeper = dataSource.getConnection();
                         Statement statement = keeper.createStatement();
                         FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(dataSource),
                                 ConnectionAccessTestSupport.reactive(factory)).configuredDialect("h2").build()) {
                        statement.execute("create table uuid_rows (id uuid primary key, token uuid, version bigint)");
                        boolean update = operation.endsWith("update");
                        if (update) {
                            clients.forms().insert(WriteSpec.insert(FORM,
                                    Map.of("id", ID, "token", ID, "version", 1L))).block(TIMEOUT);
                        }
                        Map<String, Object> values = new LinkedHashMap<>();
                        if (!update) {
                            values.put("id", ID);
                        }
                        values.put("token", null);
                        if (!update) {
                            values.put("version", 1L);
                        }
                        ConditionGroup where = ConditionGroup.and().where("id", "=", ID).build();
                        if (operation.startsWith("batch-")) {
                            BatchSpec spec = update
                                    ? BatchSpec.update(FORM, Flux.just(new BatchOptimisticUpdate(values, where,
                                            OptimisticLockOptions.increment("version", 1L))))
                                    : BatchSpec.insert(FORM, Flux.just(values));
                            BatchExecutionEvidence result = ExternalTransactionTestSupport.reactive(factory, transaction ->
                                    Mono.using(() -> FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(dataSource), transaction)
                                                    .configuredDialect("h2").build(),
                                            transactional -> transactional.forms().writeBatch(spec),
                                            FlyingOrmClients::close));
                            assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
                            assertEquals(com.flying.orm.rdb.batch.BatchAffectedRows.known(1L), result.affectedRows());
                        } else {
                            Long affected = update
                                    ? clients.forms().update(WriteSpec.update(FORM, values, where)).block(TIMEOUT)
                                    : clients.forms().insert(WriteSpec.insert(FORM, values)).block(TIMEOUT);
                            assertEquals(1L, affected);
                        }
                        try (ResultSet rows = statement.executeQuery("select id, token, version from uuid_rows")) {
                            assertTrue(rows.next());
                            assertEquals(ID, rows.getObject("id", UUID.class));
                            assertNull(rows.getObject("token"));
                            assertEquals(operation.equals("batch-update") ? 2L : 1L, rows.getLong("version"));
                            assertFalse(rows.next());
                        }
                    }
                }));
    }
}
