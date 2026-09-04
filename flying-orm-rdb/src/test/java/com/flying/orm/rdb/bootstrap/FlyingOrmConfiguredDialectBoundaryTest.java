package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRequest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlyingOrmConfiguredDialectBoundaryTest {

    @Test
    void jdbcBuilderWithConfiguredDialectBuildsWithoutBorrowingAConnection() {
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                FlyingOrmConfiguredDialectBoundaryTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        throw new AssertionError("configured dialect must not borrow a JDBC connection");
                    }
                    return defaultValue(method.getReturnType());
                });

        assertDoesNotThrow(() -> {
            try (FlyingOrmClients ignored = FlyingOrmClients.builder(dataSource)
                                                         .configuredDialect("h2")
                                                         .build()) {
                // Client construction is the assertion boundary.
            }
        });
    }

    @Test
    void jdbcBuilderUsesTheConfiguredDialectForNativeSqlValidation() {
        AtomicBoolean prepared = new AtomicBoolean();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                FlyingOrmConfiguredDialectBoundaryTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> "executeLargeUpdate".equals(method.getName())
                        ? 1L : defaultValue(method.getReturnType()));
        java.sql.Connection connection = (java.sql.Connection) Proxy.newProxyInstance(
                FlyingOrmConfiguredDialectBoundaryTest.class.getClassLoader(),
                new Class<?>[]{java.sql.Connection.class},
                (proxy, method, arguments) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        prepared.set(true);
                        return statement;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                FlyingOrmConfiguredDialectBoundaryTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));

        try (FlyingOrmClients clients = FlyingOrmClients.builder(dataSource)
                                                     .configuredDialect("mysql")
                                                     .build()) {
            long rows = clients.syncExecutor().rowsUpdated(SqlRequest.nativeSql(
                    "update account set active=false # ; comment\nwhere id=1", List.of()));

            assertEquals(1L, rows);
            assertTrue(prepared.get());
        }
    }

    @Test
    void r2dbcBuilderWithConfiguredDialectBuildsWithoutReadingFactoryMetadata() {
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new AssertionError("configured dialect must not create an R2DBC connection");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                throw new AssertionError("configured dialect must not read R2DBC factory metadata");
            }
        };

        assertDoesNotThrow(() -> {
            try (FlyingOrmClients ignored = FlyingOrmClients.builder(connectionFactory)
                                                         .configuredDialect("h2")
                                                         .build()) {
                // Client construction is the assertion boundary.
            }
        });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }
}
