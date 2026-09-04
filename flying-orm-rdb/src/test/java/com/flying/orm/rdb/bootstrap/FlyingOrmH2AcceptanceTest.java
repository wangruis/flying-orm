package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PROTOCOL;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlyingOrmH2AcceptanceTest {

    private static final Duration REACTIVE_TIMEOUT = Duration.ofSeconds(10);

    @Test
    void executesDocumentedSyncAndReactiveFormPathsAgainstOneRealDatabase() throws Exception {
        String database = "flying_orm_" + UUID.randomUUID().toString().replace("-", "");
        JdbcDataSource dataSource = dataSource(database);
        createSchema(dataSource);
        ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(DRIVER, "h2")
                .option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                .option(DATABASE, database)
                .option(USER, "sa")
                .option(PASSWORD, "")
                .option(H2ConnectionFactoryProvider.OPTIONS,
                        "DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE")
                .build());

        DynamicForm departments = departmentForm();
        DynamicForm users = userForm();
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource, connectionFactory)
                                                         .configuredDialect("h2")
                                                         .build()) {
            assertTrue(clients.jdbcAvailable());
            assertTrue(clients.reactiveAvailable());

            assertEquals(1, clients.syncForms().insert(WriteSpec.insert(departments,
                    Map.of("id", 10L, "name", "Engineering"))));
            assertEquals(1, clients.syncForms().insert(WriteSpec.insert(users,
                    user(1L, 10L, "Alice", Instant.parse("2026-08-25T00:00:00Z")))));

            assertEquals(1, clients.forms().insert(WriteSpec.insert(users,
                    user(4L, null, "Carol", Instant.parse("2026-08-25T00:03:00Z"))))
                    .block(REACTIVE_TIMEOUT));

            assertEquals(1, clients.operator().unsafeNativeSql("""
                    insert into users(id, department_id, name, created_at)
                    values (:id, :departmentId, :name, :createdAt)
                    """)
                    .bind("id", 5L)
                    .bind("departmentId", null)
                    .bind("name", "Dave")
                    .bind("createdAt", Instant.parse("2026-08-25T00:04:00Z"))
                    .insert()
                    .block(REACTIVE_TIMEOUT));

            DynamicRow reactiveTypedNull = clients.operator()
                    .unsafeNativeSql("select cast(:value as bigint) as null_value")
                    .bindNull("value", Long.class)
                    .one()
                    .block(REACTIVE_TIMEOUT);
            assertNull(reactiveTypedNull.get("null_value"));

            DynamicRow syncTypedNull = clients.syncOperator()
                    .unsafeNativeSql("select cast(:value as bigint) as null_value")
                    .bindNull("value", Long.class)
                    .one();
            assertNull(syncTypedNull.get("null_value"));

            BatchWriteResult batch = clients.forms().writeBatch(BatchSpec.insert(users, Flux.just(
                    user(2L, 10L, "ALAN", Instant.parse("2026-08-25T00:01:00Z")),
                    user(3L, null, "Bob", Instant.parse("2026-08-25T00:02:00Z")))))
                    .block(REACTIVE_TIMEOUT);
            assertEquals(2, batch.inputCount());
            assertEquals(2, batch.affectedRows());

            ConditionGroup namesStartingWithA = ConditionGroup.and()
                    .where("name", "like-ignore-case", "a%")
                    .build();
            List<DynamicRow> matching = clients.forms()
                    .select(QuerySpec.of(users, namesStartingWithA))
                    .collectList()
                    .block(REACTIVE_TIMEOUT);
            assertEquals(List.of("ALAN", "Alice"), matching.stream()
                    .map(row -> row.get("name", String.class))
                    .sorted()
                    .toList());

            PageResult<DynamicRow> page = clients.syncForms().page(
                    QuerySpec.of(users, ConditionGroup.and().build()),
                    PageQuery.of(1, 2, PageSort.asc("id")));
            assertEquals(5, page.total());
            assertEquals(2, page.rows().size());
            assertTrue(page.hasNext());

            JoinQuerySpec.Builder join = JoinQuerySpec.builder(users);
            JoinSource user = join.root();
            JoinSource department = join.join(
                    JoinType.LEFT, departments, user, "department_id", "id");
            JoinQuerySpec joinQuery = join.select(user, "id")
                                          .select(user, "name")
                                          .select(department, "name")
                                          .orderBy(user, "id", PageSort.Direction.ASC)
                                          .build();
            List<DynamicRow> joined = clients.syncForms().selectJoin(joinQuery);
            assertEquals(5, joined.size());
            assertEquals("Engineering", joined.getFirst().get("s1_name", String.class));
            assertNull(joined.getLast().get("s1_name"));

            assertEquals(1, clients.syncForms().delete(WriteSpec.delete(users,
                    ConditionGroup.and().where("id", "=", 1L).build())));
            List<DynamicRow> remaining = clients.forms()
                    .select(QuerySpec.of(users, ConditionGroup.and().build()))
                    .collectList()
                    .block(REACTIVE_TIMEOUT);
            assertEquals(4, remaining.size());
        }
    }

    private static JdbcDataSource dataSource(String database) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + database
                + ";DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    private static void createSchema(JdbcDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE departments (
                        id BIGINT PRIMARY KEY,
                        name VARCHAR(100) NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE users (
                        id BIGINT PRIMARY KEY,
                        department_id BIGINT,
                        name VARCHAR(100) NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        deleted BOOLEAN NOT NULL DEFAULT FALSE,
                        CONSTRAINT fk_users_department FOREIGN KEY (department_id) REFERENCES departments(id)
                    )
                    """);
        }
    }

    private static DynamicForm departmentForm() {
        return DynamicForm.builder("departments", "departments")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR(100)").withNullable(false))
                          .build();
    }

    private static DynamicForm userForm() {
        return DynamicForm.builder("users", "users")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("department_id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR(100)").withNullable(false))
                          .addField(DynamicField.of("created_at", "TIMESTAMPTZ").withNullable(false))
                          .addField(DynamicField.of("deleted", "BOOLEAN").withNullable(false))
                          .logicDelete("deleted", false, true)
                          .build();
    }

    private static Map<String, Object> user(long id, Long departmentId, String name, Instant createdAt) {
        Map<String, Object> values = new java.util.LinkedHashMap<>();
        values.put("id", id);
        values.put("department_id", departmentId);
        values.put("name", name);
        values.put("created_at", createdAt);
        return values;
    }
}
