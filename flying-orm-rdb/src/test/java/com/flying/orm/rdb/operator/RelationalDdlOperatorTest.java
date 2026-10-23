package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalDdlOperatorTest {

    @TestFactory
    Stream<DynamicTest> bothFacadesReuseTheCompleteSchemaLifecycle() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive,
                () -> verifyLifecycle(reactive)));
    }

    private static void verifyLifecycle(boolean reactive) throws Exception {
        String name = "ddl_facade_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + name);
        source.setUser("sa");
        source.setPassword("");
        ConnectionFactory factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                .option(DATABASE, name).option(USER, "sa").option(PASSWORD, "").build());
        RdbDialect dialect = RdbDialect.h2();
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.4.240", dialect);
        RelationalTableDefinition original = table(false);
        try (Connection keeper = source.getConnection(); Statement statement = keeper.createStatement();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(factory)).dialect(dialect).build()) {
            DdlOperator async = clients.operator().ddl();
            SyncDdlOperator sync = clients.syncOperator().ddl();
            SchemaMigrationResult created;
            if (reactive) {
                Mono<SchemaMigrationResult> pending = async.createOrAlter(database, original);
                try (ResultSet tables = keeper.getMetaData().getTables(null, null, "RECORDS", null)) {
                    assertFalse(tables.next(), "assembly must not perform I/O");
                }
                created = pending.block();
            } else {
                created = sync.createOrAlter(database, original);
            }
            assertNotNull(created);
            assertNotNull(created);
            statement.executeUpdate("insert into records default values");
            try (ResultSet rows = statement.executeQuery("select id from records")) {
                assertTrue(rows.next());
                assertNotNull(rows.getObject(1));
            }
            SchemaMigrationResult unchanged = reactive ? async.createOrAlter(database, original).block()
                    : sync.createOrAlter(database, original);
            assertNotNull(unchanged);
            assertTrue(unchanged.steps().isEmpty());

            RelationalTableDefinition expanded = table(true);
            SchemaMigrationPlan addition = reactive ? async.plan(database, expanded).block()
                    : sync.plan(database, expanded);
            assertFalse(addition.steps().isEmpty());
            SchemaMigrationResult added = reactive ? async.execute(addition).block()
                    : sync.execute(addition);
            assertNotNull(added);

            SchemaMigrationPlan removal = reactive ? async.planReplacement(database, original, SchemaCompatibilityMode.EXACT).block()
                    : sync.planReplacement(database, original, SchemaCompatibilityMode.EXACT);
            SchemaMigrationResult removed = reactive ? async.execute(removal).block()
                    : sync.execute(removal, SqlExecutionOptions.unlimited());
            assertNotNull(removed);
            try (ResultSet rows = statement.executeQuery("select * from records")) {
                assertEquals(1, rows.getMetaData().getColumnCount());
                assertTrue(rows.next());
                assertNotNull(rows.getObject("id"));
            }
            verifyDynamicFields(reactive, async, sync, statement);
            verifyFluentFields(reactive, async, sync, statement);
            verifyConfiguredSchema(reactive, async, sync, database, statement);
            verifyQualifiedSchema(reactive, async, sync, database, statement);
        }
    }

    private static void verifyQualifiedSchema(boolean reactive, DdlOperator async, SyncDdlOperator sync,
                                               DatabaseDescriptor database, Statement statement) throws Exception {
        statement.execute("create schema SALES");
        statement.execute("create table PUBLIC.QUALIFIED_PAYMENTS(amount bigint default 77)");
        RelationIdentity identity = RelationIdentity.of(null, "SALES", "QUALIFIED_PAYMENTS");
        AtomicInteger configurations = new AtomicInteger();
        AtomicReference<ColumnDefinition.Builder> retained = new AtomicReference<>();
        Consumer<RelationalTableDefinition.Builder> definition = table -> {
            configurations.incrementAndGet();
            table.addColumn("AMOUNT", "BIGINT", column -> {
                column.defaultValue(ColumnDefault.literal(11));
                retained.set(column);
            });
        };
        SchemaMigrationResult report;
        if (reactive) {
            Mono<SchemaMigrationResult> pending = async.createOrAlter(database, identity, definition);
            assertEquals(1, configurations.get());
            retained.get().defaultValue(ColumnDefault.literal(99));
            assertThrows(SQLException.class, () -> statement.executeQuery("select * from SALES.QUALIFIED_PAYMENTS"));
            report = pending.block();
            SchemaMigrationResult repeated = pending.block();
            assertNotNull(repeated);
            assertTrue(repeated.steps().isEmpty());
        } else {
            report = sync.createOrAlter(database, identity, definition);
        }
        assertEquals(1, configurations.get());
        assertNotNull(report);
        for (String schema : java.util.List.of("SALES", "PUBLIC")) {
            statement.executeUpdate("insert into " + schema + ".QUALIFIED_PAYMENTS default values");
            try (ResultSet rows = statement.executeQuery("select amount from " + schema + ".QUALIFIED_PAYMENTS")) {
                assertTrue(rows.next());
                assertEquals(schema.equals("SALES") ? 11 : 77, rows.getLong(1));
                assertFalse(rows.next());
            }
        }
    }

    private static void verifyConfiguredSchema(boolean reactive, DdlOperator async, SyncDdlOperator sync,
                                                DatabaseDescriptor database, Statement statement) throws Exception {
        AtomicInteger configurations = new AtomicInteger();
        AtomicReference<RelationalTableDefinition.Builder> retainedTable = new AtomicReference<>();
        AtomicReference<ColumnDefinition.Builder> retainedColumn = new AtomicReference<>();
        Consumer<RelationalTableDefinition.Builder> definition = table -> {
            configurations.incrementAndGet();
            retainedTable.set(table);
            table.addColumn("id", "BIGINT", column -> column.nullable(false)
                            .generation(ValueGeneration.identity()))
                    .addColumn("amount", "DECIMAL", column -> {
                        column.precision(18).scale(2).defaultValue(ColumnDefault.literal(new BigDecimal("5.00")));
                        retainedColumn.set(column);
                    })
                    .addColumn("created_at", "TIMESTAMP", column -> column.defaultValue(ColumnDefault.currentTimestamp()))
                    .primaryKey(PrimaryKeyDefinition.of("pk_configured", "id"))
                    .addCheck(CheckConstraintDefinition.of("ck_configured_amount",
                            CheckPredicate.compare("amount", CheckPredicate.ComparisonOperator.GREATER_THAN_OR_EQUAL, 0)));
        };
        SchemaMigrationResult report;
        if (reactive) {
            Mono<SchemaMigrationResult> pending = async.createOrAlter(database, "configured_payments", definition);
            assertEquals(1, configurations.get(), "configuration is applied once at assembly");
            retainedColumn.get().defaultValue(ColumnDefault.literal(-1));
            retainedTable.get().addColumn(ColumnDefinition.builder("late_column", "INTEGER").build());
            assertThrows(SQLException.class, () -> statement.executeQuery("select * from configured_payments"));
            report = pending.block();
            SchemaMigrationResult repeated = pending.block();
            assertNotNull(repeated);
            assertTrue(repeated.steps().isEmpty(), "resubscription must reuse the frozen definition");
        } else {
            report = sync.createOrAlter(database, "configured_payments", definition);
        }
        assertEquals(1, configurations.get());
        assertNotNull(report);
        statement.executeUpdate("insert into configured_payments default values");
        try (ResultSet rows = statement.executeQuery("select * from configured_payments")) {
            assertEquals(3, rows.getMetaData().getColumnCount());
            assertTrue(rows.next());
            assertTrue(rows.getLong("id") > 0);
            assertEquals(new BigDecimal("5.00"), rows.getBigDecimal("amount"));
            assertNotNull(rows.getTimestamp("created_at"));
            assertFalse(rows.next());
        }
        assertThrows(SQLException.class,
                () -> statement.executeUpdate("insert into configured_payments(amount) values(-1)"));
    }

    private static void verifyFluentFields(boolean reactive, DdlOperator async, SyncDdlOperator sync,
                                           Statement statement) throws Exception {
        if (reactive) {
            async.createOrAlter("fluent_payments")
                    .addColumn("id", column -> column.type("BIGINT").primaryKey().generation(ValueGeneration.identity(10, 2, 0)))
                    .addColumn("code", column -> column.varchar(30).nullable(false).unique(true))
                    .addColumn("amount", column -> column.number(18, 4))
                    .addColumn("created_at", column -> column.type("TIMESTAMP(6)")).commit().block();
        } else {
            sync.createOrAlter("fluent_payments")
                    .addColumn("id", column -> column.type("BIGINT").primaryKey().generation(ValueGeneration.identity(10, 2, 0)))
                    .addColumn("code", column -> column.varchar(30).nullable(false).unique(true))
                    .addColumn("amount", column -> column.number(18, 4))
                    .addColumn("created_at", column -> column.type("TIMESTAMP(6)")).commit();
        }
        statement.executeUpdate("insert into fluent_payments(code,amount,created_at)"
                + " values('a',12.3456,TIMESTAMP '2026-10-05 12:34:56.123456')");
        statement.executeUpdate("insert into fluent_payments(code) values('b')");
        try (ResultSet rows = statement.executeQuery("select id,amount,created_at from fluent_payments order by id")) {
            assertTrue(rows.next());
            assertEquals(10, rows.getLong(1));
            assertEquals(new BigDecimal("12.3456"), rows.getBigDecimal(2));
            assertEquals(123456000, rows.getTimestamp(3).getNanos());
            assertTrue(rows.next());
            assertEquals(12, rows.getLong(1));
            assertFalse(rows.next());
        }
        assertThrows(SQLException.class, () -> statement.executeUpdate("insert into fluent_payments(code) values('a')"));
        assertThrows(SQLException.class, () -> statement.executeUpdate("insert into fluent_payments(code) values(null)"));
    }

    private static void verifyDynamicFields(boolean reactive, DdlOperator async, SyncDdlOperator sync,
                                            Statement statement) throws Exception {
        DynamicField id = DynamicField.primaryKey("id", "BIGINT").generatedByIdentity();
        DynamicField code = DynamicField.of("code", "VARCHAR(30)").withNullable(false).withUnique(true);
        DynamicField amount = DynamicField.of("amount", "DECIMAL").withPrecision(18, 4);
        DynamicField created = DynamicField.of("created_at", "TIMESTAMP").withPrecision(6, null);
        if (reactive) {
            async.createOrAlter("payments").addColumn(id).addColumn(code).addColumn(amount).addColumn(created)
                    .addColumn("note").varchar(80).commit().commit().block();
        } else {
            sync.createOrAlter("payments").addColumn(id).addColumn(code).addColumn(amount).addColumn(created)
                    .addColumn("note").varchar(80).commit().commit();
        }
        statement.executeUpdate("insert into payments(code,amount,created_at) values('a',12.3456,current_timestamp)");
        try (ResultSet rows = statement.executeQuery("select id,amount,created_at from payments where code='a'")) {
            assertTrue(rows.next());
            assertTrue(rows.getLong(1) > 0);
            assertEquals(new BigDecimal("12.3456"), rows.getBigDecimal(2));
            assertNotNull(rows.getTimestamp(3));
        }
        assertThrows(SQLException.class, () -> statement.executeUpdate("insert into payments(code) values('a')"));
        assertThrows(SQLException.class, () -> statement.executeUpdate("insert into payments(code) values(null)"));
        assertTrue((reactive
                ? async.createOrAlter("payments").addColumn(id).addColumn(code).addColumn(amount).addColumn(created)
                        .addColumn("note").varchar(80).commit().plan().block()
                : sync.createOrAlter("payments").addColumn(id).addColumn(code).addColumn(amount).addColumn(created)
                        .addColumn("note").varchar(80).commit().plan()).requests().isEmpty());
    }

    private static RelationalTableDefinition table(boolean expanded) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(RelationIdentity.table("records"))
                .addColumn(ColumnDefinition.builder("id", "UUID").nullable(false)
                        .defaultValue(ColumnDefault.nativeExpression("RANDOM_UUID()")).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_records", "id"));
        if (expanded) {
            builder.addColumn(ColumnDefinition.builder("note", "VARCHAR(40)").build());
        }
        return builder.build();
    }
}
