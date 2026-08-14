package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证自有 JDBC 租约归还失败时会物理淘汰连接。 */
class JdbcConnectionProviderTest {

    @Test
    void abortsOwnedConnectionWhenPoolReturnFails() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger aborts = new AtomicInteger();
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class}, (ignored, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        closes.incrementAndGet();
                        throw new SQLException("simulated pool return failure", "08006");
                    }
                    if (method.getName().equals("abort")) {
                        aborts.incrementAndGet();
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class}, (ignored, method, arguments) ->
                        method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        JdbcConnectionProvider.JdbcConnectionLease lease = new JdbcConnectionProvider(
                dataSource, JdbcTransactionParticipant.none()).acquire();

        assertThrows(SQLException.class, lease::close);

        assertEquals(1, closes.get());
        assertEquals(1, aborts.get());
    }

    /** abort Runtime 已携带 discard cause 时，lease 不能把它再反向 suppress 到 discard cause。 */
    @Test
    void keepsDiscardGraphAcyclicWhenAbortFailureAlreadyCausesDiscardCause() throws Exception {
        IllegalStateException discardCause = new IllegalStateException("transaction outcome is unknown");
        IllegalStateException abortFailure = new IllegalStateException("abort failed", discardCause);
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class}, (ignored, method, arguments) -> {
                    if (method.getName().equals("abort")) {
                        throw abortFailure;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class}, (ignored, method, arguments) ->
                        method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        JdbcConnectionProvider.JdbcConnectionLease lease = new JdbcConnectionProvider(
                dataSource, JdbcTransactionParticipant.none()).acquire();
        lease.discardAfterUncertainTransaction(discardCause);

        lease.close();

        assertSame(discardCause, abortFailure.getCause());
        assertFalse(reaches(discardCause, abortFailure));
    }

    /** abort 的驱动包装异常携带 JVM 致命错误时，必须传播同一 fatal 且不能形成反向异常环。 */
    @Test
    void promotesVirtualMachineErrorNestedInAbortFailure() throws Exception {
        IllegalStateException discardCause = new IllegalStateException("transaction outcome is unknown");
        OutOfMemoryError fatal = new OutOfMemoryError("abort fatal");
        IllegalStateException abortFailure = new IllegalStateException("driver wrapper", fatal);
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class}, (ignored, method, arguments) -> {
                    if (method.getName().equals("abort")) {
                        throw abortFailure;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class}, (ignored, method, arguments) ->
                        method.getName().equals("getConnection") ? connection : defaultValue(method.getReturnType()));
        JdbcConnectionProvider.JdbcConnectionLease lease = new JdbcConnectionProvider(
                dataSource, JdbcTransactionParticipant.none()).acquire();
        lease.discardAfterUncertainTransaction(discardCause);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, lease::close);

        assertSame(fatal, observed);
        assertFalse(reaches(discardCause, fatal));
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

    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }
}
