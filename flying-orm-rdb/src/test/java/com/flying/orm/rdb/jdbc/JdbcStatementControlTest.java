package com.flying.orm.rdb.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 取消的保守契约：线程已经中断时，先尽力取消语句，再返回稳定的取消错误。 */
class JdbcStatementControlTest {

    @Test
    void cancelsTheStatementWhenTheCallingThreadWasInterrupted() {
        AtomicBoolean cancelled = new AtomicBoolean();
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> {
                    if ("cancel".equals(method.getName())) {
                        cancelled.set(true);
                        return null;
                    }
                    return null;
                });

        Thread.currentThread().interrupt();
        try {
            SQLException error = assertThrows(SQLException.class,
                                               () -> JdbcStatementControl.requireNotInterrupted(statement));
            assertEquals("HY008", error.getSQLState());
            assertTrue(cancelled.get());
        } finally {
            // JUnit 复用工作线程，必须清掉本测试设置的中断位，避免污染后续测试。
            Thread.interrupted();
        }
    }

    /** 驱动包装的 VM 错误仍须提升原对象，不能被稳定的 HY008 取消异常降级。 */
    @Test
    void promotesVirtualMachineErrorNestedInStatementCancellationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("statement cancellation fatal");
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> {
                    if ("cancel".equals(method.getName())) {
                        throw new IllegalStateException("driver wrapper", fatal);
                    }
                    return null;
                });

        Thread.currentThread().interrupt();
        try {
            OutOfMemoryError observed = assertThrows(
                    OutOfMemoryError.class,
                    () -> JdbcStatementControl.requireNotInterrupted(statement));

            assertSame(fatal, observed);
            SQLException cancelled = Arrays.stream(observed.getSuppressed())
                    .filter(SQLException.class::isInstance)
                    .map(SQLException.class::cast)
                    .findFirst()
                    .orElseThrow();
            assertEquals("HY008", cancelled.getSQLState());
            assertEquals(0, cancelled.getSuppressed().length);
        } finally {
            Thread.interrupted();
        }
    }
}
