package com.flying.orm.consumer;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.schema.FormSchemaDdlRenderer;
import com.flying.orm.rdb.schema.SchemaMigrationPlan;
import com.flying.orm.rdb.schema.SchemaMigrationOptions;
import com.flying.orm.rdb.schema.SchemaMigrationResult;
import com.flying.orm.rdb.schema.SchemaSnapshot;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class FormSchemaDdlExecutionContractTest {
    @TestFactory
    Stream<DynamicTest> nativeFormPlansExecuteTheExactCustomSqlOnBothPorts() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest("reactive=" + reactive,
                () -> verify(reactive)));
    }

    private static void verify(boolean reactive) throws Exception {
        String name = "form_ddl_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:" + name);
        source.setUser("sa");
        List<SchemaMigrationPlan> renders = new ArrayList<>();
        FormSchemaDdlRenderer extension = new FormSchemaDdlRenderer() {
            @Override
            public List<SqlRequest> render(RelationalTableDefinition current, SchemaSnapshot snapshot, SchemaMigrationPlan plan) {
                assertEquals(plan.tableExists(), current != null);
                renders.add(plan);
                return custom(plan.requests());
            }

        };
        RdbDialect dialect = RdbDialect.h2().withSchema(RdbDialect.h2().schema().withFormRenderer(extension));
        try (Connection keeper = source.getConnection();
             FlyingOrmClients clients = FlyingOrmClients.builder(ConnectionAccessTestSupport.jdbc(source),
                     ConnectionAccessTestSupport.reactive(ConnectionFactories.get(ConnectionFactoryOptions.builder()
                             .option(ConnectionFactoryOptions.DRIVER, "h2")
                             .option(ConnectionFactoryOptions.PROTOCOL, "mem")
                             .option(ConnectionFactoryOptions.DATABASE, name)
                             .option(ConnectionFactoryOptions.USER, "sa").build())))
                     .dialect(dialect).build()) {
            JdbcFormMetadataReader jdbc = JdbcFormMetadataReaders.create(clients.syncExecutor(), dialect);
            for (boolean expanded : List.of(false, true)) {
                DynamicForm target = form(expanded);
                int beforePlan = renders.size();
                SchemaMigrationPlan plan = reactive
                        ? clients.schema().planCreateOrAlter(target, List.of(), clients.metadata()).block()
                        : clients.syncSchema().planCreateOrAlter(target, List.of(), jdbc);
                assertNotNull(plan);
                assertEquals(beforePlan + 1, renders.size());
                assertTrue(plan.requests().stream().allMatch(request -> request.sql().startsWith("/* native */ ")));
                SchemaMigrationPlan reviewed = reactive
                        ? clients.schema().planCreateOrAlter(target, List.of(), List.of(), clients.metadata(),
                                SchemaMigrationOptions.safe()).block()
                        : clients.syncSchema().planCreateOrAlter(target, List.of(), List.of(), jdbc,
                                SchemaMigrationOptions.safe());
                assertNotNull(reviewed);
                assertEquals(beforePlan + 2, renders.size());
                assertEquals(plan.requests(), reviewed.requests());
                SchemaMigrationResult result = reactive
                        ? clients.schema().execute(reviewed, clients.metadata()).block()
                        : clients.syncSchema().execute(reviewed);
                assertNotNull(result);
                assertSame(reviewed, result.plan());
                assertEquals(beforePlan + 2, renders.size(), "execution must not render again");
            }
            try (java.sql.Statement statement = keeper.createStatement()) {
                assertEquals(1, statement.executeUpdate("insert into items(id,title) values(1,'Ada')"));
            }
            int calls = renders.size();
            SchemaMigrationPlan unchanged = reactive
                    ? clients.schema().planCreateOrAlter(form(true), List.of(), clients.metadata()).block()
                    : clients.syncSchema().planCreateOrAlter(form(true), List.of(), jdbc);
            assertNotNull(unchanged);
            assertFalse(unchanged.hasExecutableSql());
            assertEquals(calls, renders.size());
            SchemaMigrationPlan drop = reactive
                    ? clients.schema().planCreateOrAlter(form(false), List.of(), List.of(), clients.metadata(),
                            SchemaMigrationOptions.safe().allowDropColumn()).block()
                    : clients.syncSchema().planCreateOrAlter(form(false), List.of(), List.of(), jdbc,
                            SchemaMigrationOptions.safe().allowDropColumn());
            assertNotNull(drop);
            SchemaMigrationResult safe = reactive
                    ? clients.schema().createOrAlterDetailed(form(false), List.of(), clients.metadata()).block()
                    : clients.syncSchema().createOrAlterDetailed(form(false), List.of(), jdbc);
            assertNotNull(safe);
            assertFalse(safe.plan().hasExecutableSql(), "default options must still retain existing columns");

            SchemaMigrationResult executed = reactive
                    ? clients.schema().execute(drop, clients.metadata()).block()
                    : clients.syncSchema().execute(drop);
            assertNotNull(executed);
            assertTrue(executed.plan().hasExecutableSql(), "explicit options must not require a second approval flow");
            assertTrue(executed.plan().requests().stream().allMatch(request -> request.sql().startsWith("/* native */ ")));
            try (java.sql.Statement statement = keeper.createStatement();
                 java.sql.ResultSet rows = statement.executeQuery("select * from items")) {
                assertEquals(1, rows.getMetaData().getColumnCount());
                assertTrue(rows.next());
                assertEquals(1L, rows.getLong("id"), "dropping the explicitly allowed column retains the row");
            }
        }
    }

    private static DynamicForm form(boolean expanded) {
        DynamicForm.Builder builder = DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"));
        if (expanded) builder.addField(DynamicField.of("title", "VARCHAR").withLength(40));
        return builder.build();
    }

    private static List<SqlRequest> custom(List<SqlRequest> requests) {
        return requests.stream().map(request -> new SqlRequest("/* native */ " + request.sql()
                .replace("create table ", "create table if not exists ")
                .replace("add column ", "add column if not exists "), request.parameters())).toList();
    }
}
