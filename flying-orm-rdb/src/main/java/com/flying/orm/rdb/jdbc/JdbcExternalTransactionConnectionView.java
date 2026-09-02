package com.flying.orm.rdb.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 给外部事务回调提供受保护的 JDBC 视图。
 *
 * <p>连接视图禁止事务所有权方法、SQL 创建和 unwrap；元数据与结果集只在返回给回调的当前层做浅保护，
 * 直接拒绝可回到 Statement 或 Connection 的链接。这样保留安全读取能力，而不递归代理整个 JDBC 对象图。</p>
 */
final class JdbcExternalTransactionConnectionView {

    private JdbcExternalTransactionConnectionView() {
    }

    static Connection connection(Connection delegate) {
        return proxy(Connection.class, delegate, (method, arguments) -> {
            rejectConnectionOwnershipMethod(method, arguments);
            rejectUnwrap(method, arguments);
            Object result = invoke(delegate, method, arguments);
            return result instanceof DatabaseMetaData metadata ? metadata(metadata) : result;
        });
    }

    static DatabaseMetaData metadata(DatabaseMetaData delegate) {
        return proxy(DatabaseMetaData.class, delegate, (method, arguments) -> {
            rejectUnwrap(method, arguments);
            if (method.getName().equals("getConnection") && method.getParameterCount() == 0) {
                throw new SQLException("external transaction metadata cannot expose its connection");
            }
            Object result = invoke(delegate, method, arguments);
            return result instanceof ResultSet resultSet ? resultSet(resultSet) : result;
        });
    }

    static ResultSet resultSet(ResultSet delegate) {
        return proxy(ResultSet.class, delegate, (method, arguments) -> {
            rejectUnwrap(method, arguments);
            if (method.getName().equals("getStatement") && method.getParameterCount() == 0) {
                throw new SQLException("external transaction result set cannot expose its statement");
            }
            return invoke(delegate, method, arguments);
        });
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
