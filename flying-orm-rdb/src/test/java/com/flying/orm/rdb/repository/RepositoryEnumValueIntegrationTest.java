package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 用真实 JDBC 参数绑定和行映射验证 @EnumValue，不只检查元数据字符串。 */
class RepositoryEnumValueIntegrationTest {

    @Test
    void storesAndReadsTheAnnotatedEnumMember() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:repository_enum_value;DB_CLOSE_DELAY=-1");
        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource).configuredDialect("h2").build()) {
            clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(
                    "create table enum_device (id bigint primary key, status integer not null)", List.of()));
            SyncFormRepository<Device> repository = clients.syncRepository(Device.class);

            assertEquals(1L, repository.insert(new Device(7L, Status.ONLINE)));
            Device loaded = repository.select(ConditionGroup.and().build()).getFirst();

            assertEquals(7L, loaded.id);
            assertEquals(Status.ONLINE, loaded.status);
        }
    }

    @TableName("enum_device")
    private static final class Device {

        @TableId
        private Long id;
        private Status status;

        private Device() {
        }

        private Device(Long id, Status status) {
            this.id = id;
            this.status = status;
        }
    }

    private enum Status {
        OFFLINE(0),
        ONLINE(10);

        @EnumValue
        private final int code;

        Status(int code) {
            this.code = code;
        }
    }
}
