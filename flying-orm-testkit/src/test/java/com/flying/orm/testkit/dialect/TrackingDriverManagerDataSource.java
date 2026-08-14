package com.flying.orm.testkit.dialect;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * 给外部 JDBC 认证场景使用的最小 DataSource。
 *
 * <p>它不模拟连接池，也不替代上层项目的 DataSource：只是在 DriverManager 创建的每条真连接外面加一个极薄的
 * close 追踪层。这样测试除了验证 SQL 能执行，还能确认方言识别、DDL、DML、批量和元数据读取后没有遗漏连接。</p>
 */
final class TrackingDriverManagerDataSource implements DataSource {

    private final String url;
    private final String username;
    private final String password;
    private final AtomicInteger opened = new AtomicInteger();
    private final AtomicInteger closed = new AtomicInteger();

    TrackingDriverManagerDataSource(String url, String username, String password) {
        this.url = Objects.requireNonNull(url, "JDBC URL must not be null");
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return track(open(username, password));
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return track(open(username, password));
    }

    /** 测试结束时每一条由该 DataSource 交出的连接都必须已经正常关闭。 */
    void assertAllConnectionsClosed() {
        if (opened.get() != closed.get()) {
            throw new AssertionError("JDBC connections were not fully closed: opened="
                    + opened.get() + ", closed=" + closed.get());
        }
    }

    private Connection open(String requestedUsername, String requestedPassword) throws SQLException {
        if (requestedUsername == null || requestedUsername.isBlank()) {
            return DriverManager.getConnection(url);
        }
        return DriverManager.getConnection(url, requestedUsername, requestedPassword);
    }

    private Connection track(Connection connection) {
        opened.incrementAndGet();
        AtomicBoolean released = new AtomicBoolean();
        return (Connection) Proxy.newProxyInstance(
                connection.getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    try {
                        Object result = method.invoke(connection, arguments);
                        if (method.getName().equals("close") && released.compareAndSet(false, true)) {
                            closed.incrementAndGet();
                        }
                        return result;
                    } catch (InvocationTargetException exception) {
                        throw exception.getTargetException();
                    }
                });
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return DriverManager.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        DriverManager.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        // DriverManager 本身没有统一的驱动父 logger；返回 JDBC 标准 logger，避免测试辅助类伪造某个具体驱动的实现细节。
        return Logger.getLogger("java.sql");
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (type.isInstance(this)) {
            return type.cast(this);
        }
        throw new SQLException("DataSource does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) {
        return type.isInstance(this);
    }
}
