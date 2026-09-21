package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.execution.ExternalTransactionTestSupport;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.r2dbc.spi.ConnectionFactoryOptions.*;
import static org.junit.jupiter.api.Assertions.*;

class T7T8RepositoryRoundTripTest {
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final SqlRenderer SQL = SqlRenderer.builder().addDefaultTerms().build();

    @TestFactory
    Stream<DynamicTest> t701EnumWriteConditionRead() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).flatMap(bean ->
                Stream.of("eq", "in", "notIn", "between", "nested", "repository").map(predicate ->
                        DynamicTest.dynamicTest("T7-01 reactive=" + reactive + " bean=" + bean + " " + predicate,
                                () -> {
                                    try (Database db = new Database()) {
                                        if (bean) {
                                            enumRoundTrip(db, reactive, EnumBean.class,
                                                    new EnumBean(1L, Status.ACTIVE), EnumBean::getStatus, predicate);
                                        } else {
                                            enumRoundTrip(db, reactive, EnumRecord.class,
                                                    new EnumRecord(1L, Status.ACTIVE), EnumRecord::status, predicate);
                                        }
                                    }
                                }))));
    }

    private static <T> void enumRoundTrip(Database db, boolean reactive, Class<T> type, T entity,
                                           EntityProperty<T, ?> property, String predicate) throws Exception {
        db.insert(reactive, type, entity);
        assertEquals("A", db.scalar("select status from enum_rows where id = 1"));
        List<T> rows;
        if (predicate.equals("repository")) {
            ConditionGroup where = ConditionGroup.and().where("status", "=", Status.ACTIVE).build();
            rows = db.select(reactive, type, where);
        } else if (reactive) {
            var query = EntityDmlOperator.create(db.clients.forms(), SQL, type).query();
            switch (predicate) {
                case "eq" -> query.where(property, Status.ACTIVE);
                case "in" -> query.in(property, List.of(Status.ACTIVE));
                case "notIn" -> query.notIn(property, List.of(Status.INACTIVE));
                case "between" -> query.between(property, Status.ACTIVE, Status.ACTIVE);
                case "nested" -> query.or(group -> group.where(property, Status.ACTIVE));
                default -> throw new AssertionError(predicate);
            }
            rows = query.execute().collectList().block(WAIT);
        } else {
            var query = SyncEntityDmlOperator.create(db.clients.syncForms(), SQL, type).query();
            switch (predicate) {
                case "eq" -> query.where(property, Status.ACTIVE);
                case "in" -> query.in(property, List.of(Status.ACTIVE));
                case "notIn" -> query.notIn(property, List.of(Status.INACTIVE));
                case "between" -> query.between(property, Status.ACTIVE, Status.ACTIVE);
                case "nested" -> query.or(group -> group.where(property, Status.ACTIVE));
                default -> throw new AssertionError(predicate);
            }
            rows = query.execute();
        }
        assertNotNull(rows);
        assertEquals(1, rows.size(), "condition must match the stored enum code");
        assertEquals(Status.ACTIVE, property.apply(rows.getFirst()));
    }

    @TestFactory
    Stream<DynamicTest> enumLambdaUpdateUsesDeclaredDatabaseValue() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                "enum lambda update [reactive=" + reactive + "]", () -> {
                    try (Database db = new Database()) {
                        db.insert(reactive, EnumRecord.class, new EnumRecord(1L, Status.INACTIVE));
                        long affected = reactive
                                ? EntityDmlOperator.create(db.clients.forms(), SQL, EnumRecord.class)
                                        .update().set(EnumRecord::status, Status.ACTIVE)
                                        .where(EnumRecord::id, 1L).execute().block(WAIT)
                                : SyncEntityDmlOperator.create(db.clients.syncForms(), SQL, EnumRecord.class)
                                        .update().set(EnumRecord::status, Status.ACTIVE)
                                        .where(EnumRecord::id, 1L).execute();

                        assertEquals(1L, affected);
                        assertEquals("A", db.scalar("select status from enum_rows where id = 1"));
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> entityJoinFilterUsesDeclaredDatabaseValue() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                "entity join enum filter [reactive=" + reactive + "]", () -> {
                    try (Database db = new Database()) {
                        db.insert(reactive, EnumRecord.class, new EnumRecord(1L, Status.ACTIVE));
                        db.insert(reactive, EnumLink.class, new EnumLink(10L, 1L));

                        int rows = reactive
                                ? db.clients.operator().dml().joinQuery(EnumLink.class)
                                        .join(EnumRecord.class, EnumLink::enumId, EnumRecord::id)
                                        .select(EnumLink.class, EnumLink::id)
                                        .where(EnumRecord.class, EnumRecord::status, "=", Status.ACTIVE)
                                        .executeRows().collectList().block(WAIT).size()
                                : db.clients.syncOperator().dml().joinQuery(EnumLink.class)
                                        .join(EnumRecord.class, EnumLink::enumId, EnumRecord::id)
                                        .select(EnumLink.class, EnumLink::id)
                                        .where(EnumRecord.class, EnumRecord::status, "=", Status.ACTIVE)
                                        .executeRows().size();

                        assertEquals(1, rows);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> entityJoinMultiValueFilterUsesDeclaredDatabaseValue() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                "entity join enum multi-value filter [reactive=" + reactive + "]", () -> {
                    try (Database db = new Database()) {
                        db.insert(reactive, EnumRecord.class, new EnumRecord(1L, Status.ACTIVE));
                        db.insert(reactive, EnumLink.class, new EnumLink(10L, 1L));

                        Status[] statuses = {Status.ACTIVE};
                        int rows = reactive
                                ? db.clients.operator().dml().joinQuery(EnumLink.class)
                                        .join(EnumRecord.class, EnumLink::enumId, EnumRecord::id)
                                        .select(EnumLink.class, EnumLink::id)
                                        .where(EnumRecord.class, EnumRecord::status, "in", statuses)
                                        .executeRows().collectList().block(WAIT).size()
                                : db.clients.syncOperator().dml().joinQuery(EnumLink.class)
                                        .join(EnumRecord.class, EnumLink::enumId, EnumRecord::id)
                                        .select(EnumLink.class, EnumLink::id)
                                        .where(EnumRecord.class, EnumRecord::status, "in", statuses)
                                        .executeRows().size();

                        assertEquals(1, rows);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> t702JsonWriteRead() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).flatMap(bean ->
                Stream.of("{\"k\":1}", "[1,2]", "42", "1.25", "true", "false",
                        "\"plain text\"", "\"{\\\"looks\\\":\\\"like json\\\"}\"", "\"42\"", "set")
                        .map(shape -> DynamicTest.dynamicTest(
                        "T7-02 reactive=" + reactive + " bean=" + bean + " " + shape, () -> {
                            try (Database db = new Database()) {
                                if (shape.equals("set")) {
                                    Set<Integer> expected = Set.of(1, 2);
                                    if (bean) {
                                        db.insert(reactive, SetBean.class, new SetBean(1L, expected));
                                        assertEquals(expected, db.select(reactive, SetBean.class, id(1)).getFirst().payload);
                                    } else {
                                        db.insert(reactive, SetRecord.class, new SetRecord(1L, expected));
                                        assertEquals(expected, db.select(reactive, SetRecord.class, id(1)).getFirst().payload());
                                    }
                                } else {
                                    JsonNode expected = JsonMapper.builder().build().readTree(shape);
                                    if (bean) {
                                        db.insert(reactive, JsonBean.class, new JsonBean(1L, expected));
                                        assertEquals(expected, db.select(reactive, JsonBean.class, id(1)).getFirst().payload);
                                    } else {
                                        db.insert(reactive, JsonRecord.class, new JsonRecord(1L, expected));
                                        assertEquals(expected, db.select(reactive, JsonRecord.class, id(1)).getFirst().payload());
                                    }
                                }
                            }
                        }))));
    }

    @TestFactory
    Stream<DynamicTest> t703TextCharactersWriteRead() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).map(bean ->
                DynamicTest.dynamicTest("T7-03 reactive=" + reactive + " bean=" + bean, () -> {
                    try (Database db = new Database()) {
                        char[] expected = "a b".toCharArray();
                        if (bean) {
                            db.insert(reactive, CharBean.class, new CharBean(1L, expected));
                            assertEquals("a b", db.scalar("select text from char_rows where id = 1"));
                            assertArrayEquals(expected, db.select(reactive, CharBean.class, id(1)).getFirst().text);
                        } else {
                            db.insert(reactive, CharRecord.class, new CharRecord(1L, expected));
                            assertEquals("a b", db.scalar("select text from char_rows where id = 1"));
                            assertArrayEquals(expected, db.select(reactive, CharRecord.class, id(1)).getFirst().text());
                        }
                    }
                })));
    }

    @TestFactory
    Stream<DynamicTest> t801ListenerAppendAndFailure() {
        return Stream.of(false, true).flatMap(reactive -> Stream.of(false, true).flatMap(bean ->
                Stream.of(false, true).flatMap(batch -> Stream.of(false, true).map(reject ->
                        DynamicTest.dynamicTest("T8-01 reactive=" + reactive + " bean=" + bean
                                + " batch=" + batch + " reject=" + reject, () -> {
                                    try (Database db = new Database()) {
                                        if (bean) {
                                            listeners(db, reactive, batch, reject, EnumBean.class,
                                                    new EnumBean(1L, Status.ACTIVE));
                                        } else {
                                            listeners(db, reactive, batch, reject, EnumRecord.class,
                                                    new EnumRecord(1L, Status.ACTIVE));
                                        }
                                    }
                                })))));
    }

    private static <T> void listeners(Database db, boolean reactive, boolean batch, boolean reject,
                                       Class<T> type, T entity) throws Throwable {
        List<String> events = new ArrayList<>();
        ReactiveEntityListener<T> first = event -> Mono.defer(() -> {
            events.add("a:" + event.phase());
            return reject ? Mono.error(new IllegalStateException("first listener rejected")) : Mono.empty();
        });
        ReactiveEntityListener<T> second = event -> Mono.fromRunnable(() -> events.add("b:" + event.phase()));
        Executable write;
        Runnable read;
        Runnable readOriginal;
        Runnable readFirst;
        if (reactive) {
            var original = db.clients.repository(type);
            var withFirst = original.withListener(first);
            var both = withFirst.withListener(second);
            write = () -> {
                if (batch) {
                    db.writeBatch(true, () -> {
                        BatchExecutionEvidence result = both.insertBatch(
                                Flux.just(entity), BatchWriteOptions.of(1)).block(WAIT);
                        assertEquals(List.of("a:PRE_PERSIST", "b:PRE_PERSIST", "a:POST_PERSIST", "b:POST_PERSIST"), events,
                                "POST_PERSIST runs after the row has completed ORM work");
                        return result;
                    });
                } else {
                    both.insert(entity).block(WAIT);
                }
            };
            read = () -> both.select(id(1)).collectList().block(WAIT);
            readOriginal = () -> original.select(id(1)).collectList().block(WAIT);
            readFirst = () -> withFirst.select(id(1)).collectList().block(WAIT);
        } else {
            var original = db.clients.syncRepository(type);
            var withFirst = original.withListener(first);
            var both = withFirst.withListener(second);
            write = () -> {
                if (batch) {
                    db.writeBatch(false, () -> {
                        BatchExecutionEvidence result = both.insertBatch(Flux.just(entity), BatchWriteOptions.of(1));
                        assertEquals(List.of("a:PRE_PERSIST", "b:PRE_PERSIST", "a:POST_PERSIST", "b:POST_PERSIST"), events,
                                "POST_PERSIST runs after the row has completed ORM work");
                        return result;
                    });
                } else {
                    both.insert(entity);
                }
            };
            read = () -> both.select(id(1));
            readOriginal = () -> original.select(id(1));
            readFirst = () -> withFirst.select(id(1));
        }
        if (reject) {
            assertThrows(RuntimeException.class, write);
            assertEquals(List.of("a:PRE_PERSIST"), events);
            assertEquals(0L, ((Number) db.scalar("select count(*) from enum_rows")).longValue());
        } else {
            write.execute();
            read.run();
            assertEquals(List.of("a:PRE_PERSIST", "b:PRE_PERSIST", "a:POST_PERSIST", "b:POST_PERSIST",
                    "a:POST_LOAD", "b:POST_LOAD"), events);
            events.clear();
            readOriginal.run();
            assertTrue(events.isEmpty(), "derivation must not mutate the original repository");
            readFirst.run();
            assertEquals(List.of("a:POST_LOAD"), events);
        }
    }

    private static ConditionGroup id(long id) {
        return ConditionGroup.and().where("id", "=", id).build();
    }

    private enum Status {
        ACTIVE("A"), INACTIVE("Z");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }

    @TableName("enum_rows")
    private record EnumRecord(@TableId(type = IdType.INPUT) Long id, Status status) { }
    @TableName("enum_links")
    private record EnumLink(@TableId(type = IdType.INPUT) Long id, Long enumId) { }
    @TableName("enum_rows")
    private static final class EnumBean {
        @TableId(type = IdType.INPUT) private Long id;
        private Status status;
        private EnumBean() { }
        private EnumBean(Long id, Status status) { this.id = id; this.status = status; }
        public Status getStatus() { return status; }
    }
    @TableName("json_rows")
    private record JsonRecord(@TableId(type = IdType.INPUT) Long id, JsonNode payload) { }
    @TableName("json_rows")
    private static final class JsonBean {
        @TableId(type = IdType.INPUT) private Long id;
        private JsonNode payload;
        private JsonBean() { }
        private JsonBean(Long id, JsonNode payload) { this.id = id; this.payload = payload; }
    }
    @TableName("json_rows")
    private record SetRecord(@TableId(type = IdType.INPUT) Long id, Set<Integer> payload) { }
    @TableName("json_rows")
    private static final class SetBean {
        @TableId(type = IdType.INPUT) private Long id;
        private Set<Integer> payload;
        private SetBean() { }
        private SetBean(Long id, Set<Integer> payload) { this.id = id; this.payload = payload; }
    }
    @TableName("char_rows")
    private record CharRecord(@TableId(type = IdType.INPUT) Long id, char[] text) { }
    @TableName("char_rows")
    private static final class CharBean {
        @TableId(type = IdType.INPUT) private Long id;
        private char[] text;
        private CharBean() { }
        private CharBean(Long id, char[] text) { this.id = id; this.text = text; }
    }

    private static final class Database implements AutoCloseable {
        private final Connection keeper;
        private final FlyingOrmClients clients;
        private final JdbcDataSource source;
        private final ConnectionFactory factory;

        private Database() throws Exception {
            String name = "t7t8_" + UUID.randomUUID().toString().replace("-", "");
            source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:" + name + ";DATABASE_TO_LOWER=TRUE");
            source.setUser("sa");
            source.setPassword("");
            keeper = source.getConnection();
            try (Statement statement = keeper.createStatement()) {
                statement.execute("create table enum_rows (id bigint primary key, status varchar(32))");
                statement.execute("create table enum_links (id bigint primary key, enum_id bigint not null)");
                statement.execute("create table json_rows (id bigint primary key, payload json)");
                statement.execute("create table char_rows (id bigint primary key, text varchar(64))");
            }
            factory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                    .option(DRIVER, "h2").option(PROTOCOL, H2ConnectionFactoryProvider.PROTOCOL_MEM)
                    .option(DATABASE, name).option(USER, "sa").option(PASSWORD, "")
                    .option(H2ConnectionFactoryProvider.OPTIONS, "DATABASE_TO_LOWER=TRUE").build());
            clients = FlyingOrmClients.builder(
                    com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(source),
                    com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(factory))
                    .configuredDialect("h2").build();
        }

        private void writeBatch(boolean reactive, Supplier<BatchExecutionEvidence> operation) {
            assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, operation.get().state());
        }

        private <T> void insert(boolean reactive, Class<T> type, T entity) {
            if (reactive) {
                clients.repository(type).insert(entity).block(WAIT);
            } else {
                clients.syncRepository(type).insert(entity);
            }
        }

        private <T> List<T> select(boolean reactive, Class<T> type, ConditionGroup where) {
            return reactive ? clients.repository(type).select(where).collectList().block(WAIT)
                    : clients.syncRepository(type).select(where);
        }

        private Object scalar(String sql) throws Exception {
            try (Statement statement = keeper.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
                assertTrue(rows.next());
                return rows.getObject(1);
            }
        }

        @Override
        public void close() throws Exception {
            clients.close();
            keeper.close();
        }
    }
}
