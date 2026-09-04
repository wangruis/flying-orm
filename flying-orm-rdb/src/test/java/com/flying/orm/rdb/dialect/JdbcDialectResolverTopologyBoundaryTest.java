package com.flying.orm.rdb.dialect;

import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcDialectResolverTopologyBoundaryTest {

    @Test
    void configuredDialectDoesNotOpenRuntimeOrPhysicalDataSource() {
        AtomicInteger runtimeConnections = new AtomicInteger();
        AtomicInteger physicalConnections = new AtomicInteger();
        DataSource runtime = dataSource("H2", runtimeConnections);
        DataSource physical = dataSource("PostgreSQL", physicalConnections);

        RdbDialect dialect = JdbcDialectResolver.resolveAndValidate(
                "h2", runtime, Map.of("unavailable-shard", physical));

        assertEquals("h2", dialect.name());
        assertEquals(0, runtimeConnections.get());
        assertEquals(0, physicalConnections.get());
    }

    @Test
    void missingConfiguredDialectReadsOnlyTheUnifiedRuntimeDataSourceOnce() {
        AtomicInteger runtimeConnections = new AtomicInteger();

        RdbDialect dialect = JdbcDialectResolver.resolveAndValidate(
                null, dataSource("H2", runtimeConnections), Map.of());

        assertEquals("h2", dialect.name());
        assertEquals(1, runtimeConnections.get());
    }

    private static DataSource dataSource(String productName, AtomicInteger connections) {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                JdbcDialectResolverTopologyBoundaryTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getDatabaseProductName" -> productName;
                    case "getURL" -> "jdbc:" + productName.toLowerCase();
                    default -> defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcDialectResolverTopologyBoundaryTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> "getMetaData".equals(method.getName())
                        ? metadata : defaultValue(method.getReturnType()));
        return (DataSource) Proxy.newProxyInstance(
                JdbcDialectResolverTopologyBoundaryTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        connections.incrementAndGet();
                        return connection;
                    }
                    return defaultValue(method.getReturnType());
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
