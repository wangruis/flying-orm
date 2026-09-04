package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class JdbcMixedParameterOwnershipTest {

    enum Path { QUERY, UPDATE, GENERATED_KEYS, SCROLL }

    @Test
    void freezesOnlyOpaquePayloadAndSharesBoundValuesWithObservation() {
        assertAll(java.util.Arrays.stream(Path.values()).map(path -> () -> verifyPath(path)));
    }

    private static void verifyPath(Path path) {
        byte[] ordinary = new byte[4096];
        byte[] wrapped = {7, 8};
        SqlRequest request = new SqlRequest("select ?, ?, ?", List.of(
                ordinary, new Date(1000), new SqlTypedValue(SqlTypedValue.Kind.BLOB, wrapped)));
        Object[] bound = new Object[3];
        AtomicReference<List<Object>> observed = new AtomicReference<>();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override public boolean requiresParameterValues() { return true; }
            @Override public void onExecution(SqlExecutionObservation observation) { }
            @Override public void onExecution(SqlExecutionObservation observation, List<Object> values) {
                observed.set(values);
            }
        };
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource(bound, () -> {
            ordinary[0] = 99;
            wrapped[0] = 99;
        })).withObserver(observer);

        switch (path) {
            case QUERY -> executor.query(request);
            case UPDATE -> executor.rowsUpdated(request);
            case GENERATED_KEYS -> executor.rowsUpdatedReturningKeys(request, SqlExecutionOptions.safeDefaults());
            case SCROLL -> executor.advanced().scroll(request, ignored -> 0);
        }

        assertArrayEquals(new byte[]{7, 8}, (byte[]) bound[2], "wrapper is frozen before acquiring a connection");
        assertEquals(0, ((byte[]) bound[0])[0], "public request owns ordinary caller bytes");
        assertSame(request.parameters().get(0), bound[0], "owned ordinary array must not be copied again");
        assertSame(request.parameters().get(1), bound[1], "owned Date must not be copied again");
        assertSame(bound[0], observed.get().get(0));
        assertSame(bound[1], observed.get().get(1));
        assertArrayEquals(new byte[]{7, 8}, (byte[]) ((SqlTypedValue) observed.get().get(2)).value());
    }

    private static DataSource dataSource(Object[] bound, Runnable acquired) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (p, m, a) ->
                "getColumnCount".equals(m.getName()) ? 0 : defaultValue(m.getReturnType()));
        ResultSet rows = proxy(ResultSet.class, (p, m, a) ->
                "getMetaData".equals(m.getName()) ? metadata : defaultValue(m.getReturnType()));
        PreparedStatement statement = proxy(PreparedStatement.class, (p, m, a) -> switch (m.getName()) {
            case "setObject" -> { bound[(int) a[0] - 1] = a[1]; yield null; }
            case "setBinaryStream" -> {
                bound[(int) a[0] - 1] = ((InputStream) a[1]).readAllBytes();
                yield null;
            }
            case "executeLargeUpdate" -> 1L;
            case "executeQuery", "getGeneratedKeys" -> rows;
            default -> defaultValue(m.getReturnType());
        });
        Connection connection = proxy(Connection.class, (p, m, a) ->
                "prepareStatement".equals(m.getName()) ? statement : defaultValue(m.getReturnType()));
        return proxy(DataSource.class, (p, m, a) -> {
            if ("getConnection".equals(m.getName())) {
                acquired.run();
                return connection;
            }
            return defaultValue(m.getReturnType());
        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) { return null; }
        if (type == boolean.class) { return false; }
        if (type == long.class) { return 0L; }
        return 0;
    }
}
