package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 JDBC 资源收尾在 Error 参与时仍完整执行且保持正确主异常。 */
class JdbcResourcesTest {

    @Test
    void keepsOperationVirtualMachineErrorWhenCleanupIsOrdinaryFailure() {
        OutOfMemoryError primary = new OutOfMemoryError("operation");
        IllegalStateException cleanup = new IllegalStateException("cleanup");
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicInteger abortCalls = new AtomicInteger();

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcResources.close(
                SqlExecutionOperation.QUERY,
                false,
                primary,
                observations(),
                failing(cleanup),
                lease(closeCalls, abortCalls)));

        assertSame(primary, error);
        assertSame(cleanup, error.getSuppressed()[0]);
        assertEquals(0, closeCalls.get());
        assertEquals(1, abortCalls.get());
    }

    @Test
    void promotesCleanupVirtualMachineErrorOverOrdinaryOperationFailure() {
        IllegalStateException operation = new IllegalStateException("operation");
        OutOfMemoryError cleanup = new OutOfMemoryError("cleanup");

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcResources.close(
                SqlExecutionOperation.QUERY, false, operation, observations(), failing(cleanup)));

        assertSame(cleanup, error);
        assertSame(operation, error.getSuppressed()[0]);
    }

    @Test
    void continuesClosingLaterResourcesAndLeaseAfterCleanupVirtualMachineError() {
        OutOfMemoryError cleanup = new OutOfMemoryError("cleanup");
        AtomicInteger laterClosed = new AtomicInteger();
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicInteger abortCalls = new AtomicInteger();

        assertThrows(OutOfMemoryError.class, () -> JdbcResources.close(SqlExecutionOperation.QUERY, true, null,
                observations(), failing(cleanup), laterClosed::incrementAndGet, lease(closeCalls, abortCalls)));

        assertEquals(1, laterClosed.get());
        assertEquals(0, closeCalls.get());
        assertEquals(1, abortCalls.get());
    }

    /** Statement/ResultSet 清理异常会使会话状态未知，自有连接不能再被正常归还连接池。 */
    @Test
    void discardsOwnedLeaseWhenResourceCloseThrowsRuntimeException() {
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicInteger abortCalls = new AtomicInteger();

        JdbcResources.close(SqlExecutionOperation.QUERY, true, null, observations(),
                failing(new IllegalStateException("statement close failed")), lease(closeCalls, abortCalls));

        assertEquals(0, closeCalls.get());
        assertEquals(1, abortCalls.get());
    }

    /**
     * abort 的 VM Error 已按连接隔离语义关联资源关闭的 VM Error；资源聚合不能再反向关联而形成 Throwable 环。
     */
    @Test
    void keepsAbortVirtualMachineErrorAsPrimaryWithoutCreatingSuppressedCycle() {
        OutOfMemoryError resourceFailure = new OutOfMemoryError("result set close failed");
        OutOfMemoryError abortFailure = new OutOfMemoryError("connection abort failed");
        AtomicInteger abortCalls = new AtomicInteger();

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcResources.close(
                SqlExecutionOperation.QUERY,
                false,
                null,
                observations(),
                failing(resourceFailure),
                lease(new AtomicInteger(), abortCalls, abortFailure)));

        assertSame(abortFailure, error);
        assertEquals(1, abortCalls.get());
        assertEquals(1, abortFailure.getSuppressed().length);
        assertSame(resourceFailure, abortFailure.getSuppressed()[0]);
        assertEquals(0, resourceFailure.getSuppressed().length);
    }

    /**
     * 驱动清理可能把 VME 包装在普通异常的 cause 中；资源收尾仍应提升同一 VME、继续收尾，
     * 且不能将包装异常反向追加到该 VME 形成异常图环。
     */
    @Test
    void promotesNestedCleanupVirtualMachineErrorAndCompletesOwnedResourceCleanup() {
        OutOfMemoryError expected = new OutOfMemoryError("result set close failed");
        IllegalStateException wrapped = new IllegalStateException("driver wrapper", expected);
        AtomicInteger laterClosed = new AtomicInteger();
        AtomicInteger closeCalls = new AtomicInteger();
        AtomicInteger abortCalls = new AtomicInteger();

        OutOfMemoryError error = assertThrows(OutOfMemoryError.class, () -> JdbcResources.close(
                SqlExecutionOperation.QUERY,
                true,
                null,
                observations(),
                failing(wrapped),
                laterClosed::incrementAndGet,
                lease(closeCalls, abortCalls)));

        assertSame(expected, error);
        assertEquals(1, laterClosed.get());
        assertEquals(0, closeCalls.get());
        assertEquals(1, abortCalls.get());
        assertFalse(reaches(expected, wrapped));
    }

    private static JdbcExecutionObservationSupport observations() {
        return JdbcExecutionObservationSupport.create(SqlExecutionObserver.noop());
    }

    private static boolean reaches(Throwable root, Throwable target) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == target) {
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

    private static AutoCloseable failing(Throwable error) {
        return () -> {
            if (error instanceof Error fatal) {
                throw fatal;
            }
            if (error instanceof Exception failure) {
                throw failure;
            }
            throw new AssertionError(error);
        };
    }

    private static JdbcConnectionProvider.JdbcConnectionLease lease(AtomicInteger closes) {
        return lease(closes, new AtomicInteger());
    }

    private static JdbcConnectionProvider.JdbcConnectionLease lease(AtomicInteger closeCalls,
                                                                      AtomicInteger abortCalls) {
        return lease(closeCalls, abortCalls, null);
    }

    private static JdbcConnectionProvider.JdbcConnectionLease lease(AtomicInteger closeCalls,
                                                                      AtomicInteger abortCalls,
                                                                      Throwable abortFailure) {
        Connection connection = (Connection) Proxy.newProxyInstance(JdbcResourcesTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        closeCalls.incrementAndGet();
                    }
                    if (method.getName().equals("abort")) {
                        abortCalls.incrementAndGet();
                        if (abortFailure instanceof Error fatal) {
                            throw fatal;
                        }
                        if (abortFailure instanceof Exception failure) {
                            throw failure;
                        }
                    }
                    return null;
                });
        return JdbcConnectionProvider.JdbcConnectionLease.owned(connection);
    }
}
