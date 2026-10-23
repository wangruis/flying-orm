package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcStatementOptionsTest {

    @Test
    void appliesCapacityAndFetchHintsWithoutChangingDriverTimePolicy() throws Exception {
        List<String> calls = new ArrayList<>();
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> {
                    calls.add(method.getName() + "=" + arguments[0]);
                    return null;
                });
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(7).withFetchSize(32);

        JdbcStatementOptions.apply(statement, options);
        assertEquals(List.of("setFetchSize=32", "setMaxRows=8"), calls);
        calls.clear();
        JdbcStatementOptions.applyForScrollableCursor(statement, options);
        assertEquals(List.of("setFetchSize=32", "setMaxRows=7"), calls);
        calls.clear();
        JdbcStatementOptions.apply(statement, SqlExecutionOptions.unlimited());
        assertEquals(List.of(), calls);
    }

    @Test
    void doesNotSilentlyReduceScrollableRowsWhenLargeMaxRowsIsUnsupported() {
        assertLargeMaxRowsFailure(new SQLFeatureNotSupportedException("unsupported"));
    }

    @Test
    void doesNotSilentlyReduceScrollableRowsWhenDriverUsesTheJdbcDefaultMethod() {
        assertLargeMaxRowsFailure(new UnsupportedOperationException("not implemented"));
    }

    private static void assertLargeMaxRowsFailure(Throwable largeMaxRowsFailure) {
        AtomicInteger fallbackMaxRows = new AtomicInteger();
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "setLargeMaxRows" -> throw largeMaxRowsFailure;
                    case "setMaxRows" -> {
                        fallbackMaxRows.set((Integer) arguments[0]);
                        yield null;
                    }
                    default -> defaultValue(method.getReturnType());
                });

        Throwable actual = assertThrows(largeMaxRowsFailure.getClass(), () -> JdbcStatementOptions.applyForScrollableCursor(
                statement,
                SqlExecutionOptions.unlimited().withMaxRows((long) Integer.MAX_VALUE + 1L)));

        assertSame(largeMaxRowsFailure, actual);
        assertEquals(0, fallbackMaxRows.get());
    }

    private static Object defaultValue(Class<?> type) {
        if (type == void.class) {
            return null;
        }
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        throw new IllegalArgumentException("unsupported primitive type");
    }
}
