package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.rdb.mapping.MappingException;
import io.r2dbc.h2.H2ConnectionConfiguration;
import io.r2dbc.h2.H2ConnectionFactory;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证同一套实体注解在 JDBC 与 R2DBC Repository 中都能回填数据库自增主键。 */
class RepositoryGeneratedIdIntegrationTest {

    @Test
    void jdbcRepositoryWritesIdentityBackToTheEntity() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_repository_id;DB_CLOSE_DELAY=-1");
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).configuredDialect("h2").build()) {
            clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(createTableSql(), List.of()));
            Device entity = new Device("sensor");

            long rows = clients.syncRepository(Device.class).insert(entity);

            assertEquals(1L, rows);
            assertNotNull(entity.getDeviceId());
        }
    }

    @Test
    void reactiveRepositoryWritesIdentityBackToTheEntity() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("reactive_repository_id")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        try (FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
                                                        .configuredDialect("h2")
                                                        .build()) {
            Device entity = new Device("gateway");
            StepVerifier.create(clients.executor().rowsUpdated(SqlRequest.nativeSql(createTableSql(), List.of()))
                                       .then(clients.repository(Device.class).insert(entity)))
                        .expectNext(1L)
                        .verifyComplete();
            assertNotNull(entity.getDeviceId());
        }
    }

    @Test
    void jdbcBatchRepositoryWritesEveryIdentityBackToItsEntity() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_repository_batch_auto_id;DB_CLOSE_DELAY=-1");
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).configuredDialect("h2").build()) {
            clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(createTableSql(), List.of()));
            Device first = new Device("sensor-1");
            Device second = new Device("sensor-2");

            assertEquals(2L, clients.syncRepository(Device.class)
                                     .insertBatch(List.of(first, second)).affectedRows());
            assertNotNull(first.getDeviceId());
            assertNotNull(second.getDeviceId());
        }
    }

    @Test
    void reactiveBatchRepositoryWritesEveryIdentityBackToItsEntity() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("reactive_repository_batch_auto_id")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        try (FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
                                                        .configuredDialect("h2")
                                                        .build()) {
            Device first = new Device("gateway-1");
            Device second = new Device("gateway-2");

            StepVerifier.create(clients.executor().rowsUpdated(SqlRequest.nativeSql(createTableSql(), List.of()))
                                       .then(clients.repository(Device.class).insertBatch(List.of(first, second))))
                        .expectNextMatches(result -> result.affectedRows() == 2L)
                        .verifyComplete();
            assertNotNull(first.getDeviceId());
            assertNotNull(second.getDeviceId());
        }
    }

    @Test
    void jdbcRepositoryUsesTheExplicitAssignIdGenerator() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_assigned_id;DB_CLOSE_DELAY=-1");
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                        .configuredDialect("h2")
                                                        .idGenerator((entity, property, type) -> 9_001L)
                                                        .build()) {
            clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(
                    "create table assigned_device (device_id bigint primary key, device_name varchar(64))",
                    List.of()));
            AssignedDevice entity = new AssignedDevice("meter");

            assertEquals(1L, clients.syncRepository(AssignedDevice.class).insert(entity));
            assertEquals(9_001L, entity.getDeviceId());
        }
    }

    @Test
    void jdbcBatchInsertAndUpsertPrepareAssignedKeysBeforeSql() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:jdbc_repository_batch_id;DB_CLOSE_DELAY=-1");
        AtomicLong sequence = new AtomicLong(10_000L);
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                        .configuredDialect("h2")
                                                        .idGenerator((entity, property, type) -> sequence.getAndIncrement())
                                                        .build()) {
            createBatchTables(clients);

            AssignedDevice first = new AssignedDevice("meter-1");
            AssignedDevice second = new AssignedDevice("meter-2");
            assertEquals(2L, clients.syncRepository(AssignedDevice.class)
                                     .insertBatch(List.of(first, second)).affectedRows());
            assertNotNull(first.getDeviceId());
            assertNotNull(second.getDeviceId());

            UuidDevice uuid = new UuidDevice("gateway");
            assertEquals(1L, clients.syncRepository(UuidDevice.class)
                                     .upsertBatch(List.of(uuid)).affectedRows());
            assertNotNull(uuid.getDeviceId());

            assertThrows(MappingException.class,
                         () -> clients.syncRepository(InputDevice.class)
                                      .insertBatch(List.of(new InputDevice(null, "missing"))));
        }
    }

    @Test
    void reactiveBatchInsertAndUpsertPrepareAssignedKeysBeforeSql() {
        H2ConnectionFactory connectionFactory = new H2ConnectionFactory(
                H2ConnectionConfiguration.builder()
                                         .inMemory("reactive_repository_batch_id")
                                         .property("DB_CLOSE_DELAY", "-1")
                                         .build());
        AtomicLong sequence = new AtomicLong(20_000L);
        try (FlyingOrmClients clients = FlyingOrmClients.builder(connectionFactory)
                                                        .configuredDialect("h2")
                                                        .idGenerator((entity, property, type) -> sequence.getAndIncrement())
                                                        .build()) {
            StepVerifier.create(
                            clients.executor().rowsUpdated(SqlRequest.nativeSql(createAssignedTableSql(), List.of()))
                                    .then(clients.executor().rowsUpdated(SqlRequest.nativeSql(
                                            createUuidTableSql(), List.of())))
                                    .then(clients.executor().rowsUpdated(SqlRequest.nativeSql(
                                            createInputTableSql(), List.of())))
                                    .then())
                        .verifyComplete();

            AssignedDevice first = new AssignedDevice("meter-1");
            AssignedDevice second = new AssignedDevice("meter-2");
            UuidDevice uuid = new UuidDevice("gateway");
            StepVerifier.create(clients.repository(AssignedDevice.class)
                                       .insertBatch(List.of(first, second))
                                       .then(clients.repository(UuidDevice.class).upsertBatch(List.of(uuid))))
                        .expectNextMatches(result -> result.affectedRows() == 1L)
                        .verifyComplete();
            assertNotNull(first.getDeviceId());
            assertNotNull(second.getDeviceId());
            assertNotNull(uuid.getDeviceId());

            StepVerifier.create(clients.repository(InputDevice.class)
                                       .insertBatch(List.of(new InputDevice(null, "missing"))))
                        .expectError(MappingException.class)
                        .verify();
        }
    }

    private static void createBatchTables(FlyingOrmClients clients) {
        clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(createAssignedTableSql(), List.of()));
        clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(createUuidTableSql(), List.of()));
        clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(createInputTableSql(), List.of()));
    }

    private static String createAssignedTableSql() {
        return "create table assigned_device (device_id bigint primary key, device_name varchar(64))";
    }

    private static String createUuidTableSql() {
        return "create table assigned_uuid_device (device_id varchar(64) primary key, device_name varchar(64))";
    }

    private static String createInputTableSql() {
        return "create table input_device (device_id bigint primary key, device_name varchar(64))";
    }

    private static String createTableSql() {
        return "create table generated_device ("
                + "device_id bigint generated by default as identity primary key, "
                + "device_name varchar(64) not null)";
    }

    @TableName("generated_device")
    private static final class Device {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        private Device(String name) {
            this.name = name;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            this.deviceId = deviceId;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("assigned_device")
    private static final class AssignedDevice {

        @TableId(value = "device_id", type = IdType.ASSIGN_ID)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        private AssignedDevice(String name) {
            this.name = name;
        }

        public Long getDeviceId() {
            return deviceId;
        }

        public void setDeviceId(Long deviceId) {
            this.deviceId = deviceId;
        }

        public String getName() {
            return name;
        }
    }

    @TableName("assigned_uuid_device")
    private static final class UuidDevice {

        @TableId(value = "device_id", type = IdType.ASSIGN_UUID)
        private String deviceId;

        @TableField("device_name")
        private String name;

        private UuidDevice(String name) {
            this.name = name;
        }

        public String getDeviceId() {
            return deviceId;
        }
    }

    @TableName("input_device")
    private static final class InputDevice {

        @TableId(value = "device_id", type = IdType.INPUT)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        private InputDevice(Long deviceId, String name) {
            this.deviceId = deviceId;
            this.name = name;
        }
    }
}
