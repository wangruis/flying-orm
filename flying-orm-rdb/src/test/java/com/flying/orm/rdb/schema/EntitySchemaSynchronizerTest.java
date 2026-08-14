package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 只覆盖启动同步最容易出错的模式边界，不重复测试底层 DDL 方言。 */
class EntitySchemaSynchronizerTest {

    @Test
    void safelyCreatesThenValidatesOneEntityTable() {
        FlyingOrmClients clients = clients("schema_sync_safe");

        EntitySchemaSyncReport updated = clients.entitySchemas()
                                                      .synchronize(EntitySchemaSyncMode.SAFE_UPDATE, Account.class);
        EntitySchemaSyncReport validated = clients.entitySchemas()
                                                        .synchronize(EntitySchemaSyncMode.VALIDATE, Account.class);

        assertEquals(1, updated.results().size());
        assertFalse(validated.hasDifferences());
    }

    @Test
    void validationAndDuplicateTableMappingFailBeforeDdl() {
        FlyingOrmClients clients = clients("schema_sync_reject");

        EntitySchemaSyncException validation = assertThrows(
                EntitySchemaSyncException.class,
                () -> clients.entitySchemas().synchronize(EntitySchemaSyncMode.VALIDATE, Account.class));
        assertEquals(1, validation.report().plans().size());

        assertThrows(IllegalArgumentException.class,
                     () -> clients.entitySchemas().synchronize(
                             EntitySchemaSyncMode.SAFE_UPDATE, Account.class, DuplicateAccount.class));
        assertThrows(EntitySchemaSyncException.class,
                     () -> clients.entitySchemas().synchronize(EntitySchemaSyncMode.VALIDATE, Account.class));
    }

    private static FlyingOrmClients clients(String database) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + database + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return FlyingOrmClients.builder(dataSource).configuredDialect("h2").build();
    }

    @TableName("sync_account")
    private static final class Account {
        @TableId(type = IdType.INPUT)
        private Long id;
    }

    @TableName("sync_account")
    private static final class DuplicateAccount {
        @TableId(type = IdType.INPUT)
        private Long id;
    }
}
