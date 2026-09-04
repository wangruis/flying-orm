package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcSqlExecutorParameterSnapshotTest {

    @Test
    void ownsMutableBinaryValuesDuringSynchronousExecution() throws Exception {
        byte[] source = {1, 2, 3};
        CountDownLatch bound = new CountDownLatch(1);
        CountDownLatch execute = new CountDownLatch(1);
        AtomicReference<byte[]> observed = new AtomicReference<>();
        JdbcSqlExecutor sql = JdbcSqlExecutor.create(dataSource(bound, execute, observed));

        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            Future<Long> result = worker.submit(() -> sql.rowsUpdated(
                    new SqlRequest("update sample set payload = ?", List.of(source))));
            assertTrue(bound.await(2, TimeUnit.SECONDS));
            source[0] = 9;
            execute.countDown();

            result.get(2, TimeUnit.SECONDS);
        } finally {
            execute.countDown();
        }

        assertArrayEquals(new byte[]{1, 2, 3}, observed.get());
    }

    @Test
    void ownsMutableBinaryValuesDuringAdvancedScrollableExecution() throws Exception {
        byte[] source = {1, 2, 3};
        CountDownLatch bound = new CountDownLatch(1);
        CountDownLatch execute = new CountDownLatch(1);
        AtomicReference<byte[]> observed = new AtomicReference<>();
        JdbcSqlExecutor sql = JdbcSqlExecutor.create(dataSource(bound, execute, observed));

        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            Future<Long> result = worker.submit(() -> sql.advanced().scroll(
                    new SqlRequest("select ?", List.of(source)), ignored -> 1L));
            assertTrue(bound.await(2, TimeUnit.SECONDS));
            source[0] = 9;
            execute.countDown();

            result.get(2, TimeUnit.SECONDS);
        } finally {
            execute.countDown();
        }

        assertArrayEquals(new byte[]{1, 2, 3}, observed.get());
    }

    private static DataSource dataSource(CountDownLatch bound,
                                         CountDownLatch execute,
                                         AtomicReference<byte[]> observed) {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                JdbcSqlExecutorParameterSnapshotTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (proxy, method, args) -> "getDatabaseProductName".equals(method.getName())
                        ? "H2" : defaultValue(method.getReturnType()));
        Connection connection = connection(metadata, bound, execute, observed);
        return (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorParameterSnapshotTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, args) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));
    }

    private static Connection connection(DatabaseMetaData metadata,
                                         CountDownLatch bound,
                                         CountDownLatch execute,
                                         AtomicReference<byte[]> observed) {
        return (Connection) Proxy.newProxyInstance(
                JdbcSqlExecutorParameterSnapshotTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "prepareStatement" -> statement(bound, execute, observed);
                    case "close" -> null;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static PreparedStatement statement(CountDownLatch bound,
                                               CountDownLatch execute,
                                               AtomicReference<byte[]> observed) {
        AtomicReference<byte[]> parameter = new AtomicReference<>();
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcSqlExecutorParameterSnapshotTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setObject" -> {
                        parameter.set((byte[]) args[1]);
                        bound.countDown();
                        yield null;
                    }
                    case "executeLargeUpdate" -> {
                        if (!execute.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release JDBC execution");
                        }
                        observed.set(parameter.get().clone());
                        yield 1L;
                    }
                    case "executeQuery" -> {
                        if (!execute.await(2, TimeUnit.SECONDS)) {
                            throw new AssertionError("test did not release JDBC execution");
                        }
                        observed.set(parameter.get().clone());
                        yield resultSet();
                    }
                    case "close" -> null;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ResultSet resultSet() {
        return (ResultSet) Proxy.newProxyInstance(
                JdbcSqlExecutorParameterSnapshotTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> defaultValue(method.getReturnType()));
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
