package com.flying.orm.consumer;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.schema.SchemaCompatibilityMode;
import com.flying.orm.rdb.schema.SchemaDialect;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.schema.SchemaOperation;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class SchemaOperationExecutionContractTest {
    @TestFactory
    Stream<DynamicTest> customDdlUsesExistingReviewExecutionAndReadback() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(invalid ->
                DynamicTest.dynamicTest("reactive=" + reactive + ", invalid=" + invalid,
                        () -> verify(reactive, invalid))));
    }

    private static void verify(boolean reactive, boolean invalid) throws Exception {
        String name = "custom_ddl_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + name);
        source.setUser("sa");
        AtomicInteger renders = new AtomicInteger();
        RdbDialect base = RdbDialect.h2();
        SchemaDialect schema = base.schema().withOperationRenderer((operation, fallback) -> {
            renders.incrementAndGet();
            if (operation.kind() == SchemaOperation.Kind.CREATE_TABLE) {
                return List.of(new SqlRequest("create table items (" + (invalid ? "wrong" : "id") + " bigint)", List.of()));
            }
            if (operation.kind() == SchemaOperation.Kind.ADD_COLUMN) {
                return List.of(new SqlRequest("alter table items add column if not exists name varchar(40)", List.of()));
            }
            return fallback.get();
        });
        RdbDialect dialect = base.withSchema(schema);
        DatabaseDescriptor database = DatabaseDescriptor.of("H2", "2.4.240", dialect);
        RelationalTableDefinition first = table(false);
        try (Connection keeper = source.getConnection();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(ConnectionFactories.get(ConnectionFactoryOptions.builder()
                             .option(ConnectionFactoryOptions.DRIVER, "h2")
                             .option(ConnectionFactoryOptions.PROTOCOL, "mem")
                             .option(ConnectionFactoryOptions.DATABASE, name)
                             .option(ConnectionFactoryOptions.USER, "sa").build())))
                     .dialect(dialect).build()) {
            SchemaMigrationResult created = reactive ? clients.operator().ddl().createOrAlter(database, first).block()
                    : clients.syncOperator().ddl().createOrAlter(database, first);
            assertNotNull(created);
            if (invalid) {
                assertEquals(1, created.steps().size());
                try (java.sql.Statement statement = keeper.createStatement()) {
                    statement.executeQuery("select wrong from items").close();
                }
                return;
            }
            assertNotNull(created);
            assertTrue(renders.get() > 0);
            SchemaMigrationPlan add = reactive ? clients.operator().ddl().plan(database, table(true)).block()
                    : clients.syncOperator().ddl().plan(database, table(true));
            assertNotNull(add);
            assertEquals(List.of("alter table items add column if not exists name varchar(40)"),
                    add.requests().stream().map(SqlRequest::sql).toList());
            int reviewedCalls = renders.get();
            SchemaMigrationResult changed = reactive ? clients.operator().ddl().execute(add).block()
                    : clients.syncOperator().ddl().execute(add);
            assertNotNull(changed);
            assertEquals(reviewedCalls, renders.get(), "execution must use the reviewed SQL without rendering again");
            assertEquals(add.requests().size(), changed.steps().size());
            try (java.sql.Statement statement = keeper.createStatement()) {
                assertEquals(1, statement.executeUpdate("insert into items(id,name) values(1,'Ada')"));
            }
            SchemaMigrationPlan remove = reactive
                    ? clients.operator().ddl().planReplacement(database, first, SchemaCompatibilityMode.EXACT).block()
                    : clients.syncOperator().ddl().planReplacement(database, first, SchemaCompatibilityMode.EXACT);
            assertNotNull(remove);
            SchemaMigrationResult removed = reactive
                    ? clients.operator().ddl().execute(remove).block()
                    : clients.syncOperator().ddl().execute(remove);
            assertNotNull(removed);
            assertEquals(remove.requests(), removed.steps().stream().map(step -> step.request()).toList());
            try (java.sql.Statement statement = keeper.createStatement();
                 java.sql.ResultSet rows = statement.executeQuery("select * from items")) {
                assertEquals(1, rows.getMetaData().getColumnCount());
                assertTrue(rows.next());
                assertEquals(1L, rows.getLong("id"));
            }
        }
    }

    private static RelationalTableDefinition table(boolean expanded) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(RelationIdentity.table("items"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build());
        if (expanded) builder.addColumn(ColumnDefinition.builder("name", "VARCHAR").length(40).build());
        return builder.build();
    }

    private static String column(Object value) {
        if (!(value instanceof ColumnDefinition column)) return String.valueOf(value);
        return column.name() + ":" + column.databaseType() + "/" + column.length() + "/"
                + column.precision() + "/" + column.scale() + "/" + column.nullable() + "/"
                + column.defaultValue() + "/" + column.charset() + "/" + column.collation();
    }
}
