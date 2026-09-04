package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcStatementOptionsTest {

    @Test
    void keepsScrollableResultBoundedWhenLargeMaxRowsIsUnsupported() throws Exception {
        assertLargeMaxRowsFallback(new SQLFeatureNotSupportedException("unsupported"));
    }

    @Test
    void keepsScrollableResultBoundedWhenDriverUsesTheJdbcDefaultMethod() throws Exception {
        assertLargeMaxRowsFallback(new UnsupportedOperationException("not implemented"));
    }

    private static void assertLargeMaxRowsFallback(Throwable largeMaxRowsFailure) throws Exception {
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

        JdbcStatementOptions.applyForScrollableCursor(
                statement,
                SqlExecutionOptions.safeDefaults().withMaxRows((long) Integer.MAX_VALUE + 1L));

        assertEquals(Integer.MAX_VALUE, fallbackMaxRows.get());
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
