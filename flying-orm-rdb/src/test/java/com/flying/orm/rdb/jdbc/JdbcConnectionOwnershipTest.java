package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class JdbcConnectionOwnershipTest {

    @Test
    void closesOwnedConnectionThroughJdbcBoundary() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes);
        JdbcConnectionProvider provider = new JdbcConnectionProvider(
                dataSource(connection), JdbcTransactionParticipant.none());

        try (JdbcConnectionProvider.JdbcConnectionLease lease = provider.acquire()) {
            assertSame(connection, lease.connection());
        }

        assertEquals(1, closes.get());
    }

    @Test
    void neverClosesConnectionOwnedByExternalTransaction() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes);
        JdbcConnectionProvider provider = new JdbcConnectionProvider(
                dataSource(connection), () -> Optional.of(JdbcTransactionContext.external(connection)));

        try (JdbcConnectionProvider.JdbcConnectionLease lease = provider.acquire()) {
            assertSame(connection, lease.connection());
        }

        assertEquals(0, closes.get());
    }

    private static Connection connection(AtomicInteger closes) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        closes.incrementAndGet();
                    }
                    return null;
                });
    }

    private static DataSource dataSource(Connection connection) {
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName()) ? connection : null);
    }
}
