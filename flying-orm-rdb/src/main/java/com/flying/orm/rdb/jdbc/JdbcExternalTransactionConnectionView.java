package com.flying.orm.rdb.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 给外部事务回调提供受保护的 JDBC 视图。
 *
 * <p>只拦 {@link Connection#commit()} 还不够：回调可以从 Statement 或 ResultSet 重新拿到 Connection。
 * 因此连接、语句、结果集和元数据的回链与 unwrap 都在这里统一保护。事务回调只开放保存点等连接级能力；
 * SQL 必须走执行器，滚动或元数据回调拿到的 Statement 也不能被重新用于执行。</p>
 */
final class JdbcExternalTransactionConnectionView {

    private JdbcExternalTransactionConnectionView() {
    }

    static Connection connection(Connection delegate) {
        AtomicReference<Connection> view = new AtomicReference<>();
        Connection protectedConnection = proxy(Connection.class, delegate, (method, arguments) -> {
            rejectConnectionOwnershipMethod(method, arguments);
            return wrapConnectionResult(delegate, method, arguments, view.get());
        });
        view.set(protectedConnection);
        return protectedConnection;
    }

    static DatabaseMetaData metadata(DatabaseMetaData delegate, Connection protectedConnection) {
        return proxy(DatabaseMetaData.class, delegate, (method, arguments) -> {
            rejectUnwrap(method, arguments);
            if (method.getName().equals("getConnection") && method.getParameterCount() == 0) {
                return protectedConnection;
            }
            Object result = invoke(delegate, method, arguments);
            return result instanceof ResultSet resultSet ? resultSet(resultSet, protectedConnection) : result;
        });
    }

    static ResultSet resultSet(ResultSet delegate, Connection protectedConnection) {
        return proxy(ResultSet.class, delegate, (method, arguments) -> {
            rejectUnwrap(method, arguments);
            if (method.getName().equals("getStatement") && method.getParameterCount() == 0) {
                Statement statement = (Statement) invoke(delegate, method, arguments);
                return statement == null ? null : statement(statement, protectedConnection);
            }
            return invoke(delegate, method, arguments);
        });
    }

    private static Statement statement(Statement delegate, Connection protectedConnection) {
        if (delegate instanceof CallableStatement callable) {
            return proxy(CallableStatement.class, callable,
                         (method, arguments) -> invokeStatement(callable, protectedConnection, method, arguments));
        }
        if (delegate instanceof PreparedStatement prepared) {
            return proxy(PreparedStatement.class, prepared,
                         (method, arguments) -> invokeStatement(prepared, protectedConnection, method, arguments));
        }
        return proxy(Statement.class, delegate,
                     (method, arguments) -> invokeStatement(delegate, protectedConnection, method, arguments));
    }

    private static Object invokeStatement(Statement delegate,
                                          Connection protectedConnection,
                                          Method method,
                                          Object[] arguments) throws SQLException {
        rejectUnwrap(method, arguments);
        rejectStatementExecution(method);
        if (method.getName().equals("getConnection") && method.getParameterCount() == 0) {
            return protectedConnection;
        }
        Object result = invoke(delegate, method, arguments);
        return result instanceof ResultSet resultSet ? resultSet(resultSet, protectedConnection) : result;
    }

    private static Object wrapConnectionResult(Connection delegate,
                                               Method method,
                                               Object[] arguments,
                                               Connection protectedConnection) throws SQLException {
        rejectUnwrap(method, arguments);
        Object result = invoke(delegate, method, arguments);
        if (result instanceof Statement statement) {
            return statement(statement, protectedConnection);
        }
        return result instanceof DatabaseMetaData metadata ? metadata(metadata, protectedConnection) : result;
    }

    private static void rejectConnectionOwnershipMethod(Method method, Object[] arguments) throws SQLException {
        String name = method.getName();
        if (name.equals("commit") || name.equals("close") || name.equals("abort")
                || name.equals("setAutoCommit") || name.equals("setReadOnly") || name.equals("setCatalog")
                || name.equals("setTransactionIsolation") || name.equals("setTypeMap")
                || name.equals("setHoldability") || name.equals("setClientInfo") || name.equals("setSchema")
                || name.equals("setNetworkTimeout") || name.equals("beginRequest") || name.equals("endRequest")
                || name.startsWith("setShardingKey")
                || name.equals("createStatement") || name.equals("prepareStatement") || name.equals("prepareCall")
                || (name.equals("rollback") && (arguments == null || arguments.length == 0))) {
            throw new SQLException("external transaction connection cannot " + name
                    + "; let the transaction manager finish the transaction");
        }
    }

    private static void rejectStatementExecution(Method method) throws SQLException {
        String name = method.getName();
        if (name.startsWith("execute") || name.equals("addBatch") || name.equals("clearBatch")) {
            throw new SQLException("statement obtained from a protected callback cannot execute SQL; "
                    + "use JdbcSqlExecutor so transaction ownership and execution protection stay active");
        }
    }

    private static void rejectUnwrap(Method method, Object[] arguments) throws SQLException {
        String name = method.getName();
        if (name.equals("unwrap") || (name.equals("isWrapperFor") && arguments != null && arguments.length == 1)) {
            throw new SQLException("external transaction connection view cannot be unwrapped");
        }
    }

    private static Object invoke(Object delegate, Method method, Object[] arguments) throws SQLException {
        try {
            return method.invoke(delegate, arguments);
        } catch (IllegalAccessException error) {
            throw new SQLException("cannot access protected JDBC callback method", error);
        } catch (InvocationTargetException error) {
            Throwable cause = error.getCause();
            if (cause instanceof SQLException sqlError) throw sqlError;
            if (cause instanceof RuntimeException runtimeError) throw runtimeError;
            if (cause instanceof Error fatalError) throw fatalError;
            throw new SQLException("protected JDBC callback method failed", cause);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, T delegate, JdbcInvocation invocation) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (ignored, method, arguments) -> {
            if (method.getDeclaringClass() == Object.class) {
                return invoke(delegate, method, arguments);
            }
            return invocation.invoke(method, arguments);
        });
    }

    @FunctionalInterface
    private interface JdbcInvocation {
        Object invoke(Method method, Object[] arguments) throws SQLException;
    }
}
