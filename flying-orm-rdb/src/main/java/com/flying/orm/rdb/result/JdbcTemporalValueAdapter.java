package com.flying.orm.rdb.result;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** JDBC 厂商绝对时间对象到稳定 java.time 类型的包内适配器。 */
final class JdbcTemporalValueAdapter {

    private static final String FAILURE_MESSAGE = "failed to materialize JDBC temporal value";

    private static final TemporalAdapter FAILURE = (value, resultSet) -> {
        throw failure(null);
    };

    /** 驱动类型只解析一次，避免逐行反射查找，也不强引用驱动 ClassLoader。 */
    private static final ClassValue<TemporalAdapter> ADAPTERS = new ClassValue<>() {
        @Override
        protected TemporalAdapter computeValue(Class<?> type) {
            return switch (type.getName()) {
                case "microsoft.sql.DateTimeOffset" -> sqlServerDateTimeOffset(type);
                case "oracle.sql.TIMESTAMPTZ", "oracle.sql.TIMESTAMPLTZ" -> oracleOffsetTimestamp(type);
                default -> TemporalAdapter.IDENTITY;
            };
        }
    };

    private JdbcTemporalValueAdapter() {
    }

    static Object materialize(Object value, ResultSet resultSet) throws SQLException {
        return value == null ? null : ADAPTERS.get(value.getClass()).adapt(value, resultSet);
    }

    /** 驱动提供 getOffsetDateTime 时直接使用；否则用同一 instant 与显式分钟偏移重建。 */
    private static TemporalAdapter sqlServerDateTimeOffset(Class<?> type) {
        Method direct = method(type, "getOffsetDateTime");
        if (direct != null) {
            return (value, resultSet) -> requireOffsetDateTime(invoke(direct, value));
        }
        Method timestamp = method(type, "getTimestamp");
        Method minutesOffset = method(type, "getMinutesOffset");
        if (timestamp == null || minutesOffset == null) {
            return FAILURE;
        }
        return (value, resultSet) -> {
            Object timestampValue = invoke(timestamp, value);
            Object minutesValue = invoke(minutesOffset, value);
            if (!(timestampValue instanceof Timestamp safeTimestamp) || !(minutesValue instanceof Number minutes)) {
                throw failure(null);
            }
            int seconds = Math.multiplyExact(minutes.intValue(), 60);
            return safeTimestamp.toInstant().atOffset(ZoneOffset.ofTotalSeconds(seconds));
        };
    }

    /** Oracle LOCAL TIME ZONE 必须使用当前 ResultSet 连接，才能按真实会话时区解析。 */
    private static TemporalAdapter oracleOffsetTimestamp(Class<?> type) {
        Method converter = method(type, "offsetDateTimeValue", Connection.class);
        return converter == null ? FAILURE
                : (value, resultSet) -> requireOffsetDateTime(
                        invoke(converter, value, resultSetConnection(resultSet)));
    }

    private static Connection resultSetConnection(ResultSet resultSet) throws SQLException {
        try {
            Statement statement = resultSet.getStatement();
            Connection connection = statement == null ? null : statement.getConnection();
            if (connection == null) {
                throw failure(null);
            }
            return connection;
        } catch (SQLException | RuntimeException error) {
            throw failure(error);
        }
    }

    private static Method method(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        } catch (RuntimeException error) {
            VirtualMachineError fatal = findVirtualMachineError(error);
            if (fatal != null) {
                throw fatal;
            }
            return null;
        }
    }

    private static Object invoke(Method method, Object target, Object... arguments) throws SQLException {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException error) {
            throw failure(error.getCause());
        } catch (ReflectiveOperationException | RuntimeException error) {
            throw failure(error);
        }
    }

    private static OffsetDateTime requireOffsetDateTime(Object value) throws SQLException {
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime;
        }
        throw failure(null);
    }

    private static SQLException failure(Throwable error) {
        VirtualMachineError fatal = findVirtualMachineError(error);
        if (fatal != null) {
            throw fatal;
        }
        if (error instanceof Error fatalError) {
            throw fatalError;
        }
        // 继续使用固定消息，避免把驱动细节带到错误报告；但必须保留 JDBC 标准连接异常类别，
        // 否则 SQLState 缺失时异常翻译器会把连接故障误判为 UNKNOWN，并可能复用已损坏连接。
        if (error instanceof SQLNonTransientConnectionException connectionError) {
            return new SQLNonTransientConnectionException(
                    FAILURE_MESSAGE, connectionError.getSQLState(), connectionError.getErrorCode());
        }
        if (error instanceof SQLTransientConnectionException connectionError) {
            return new SQLTransientConnectionException(
                    FAILURE_MESSAGE, connectionError.getSQLState(), connectionError.getErrorCode());
        }
        if (error instanceof SQLRecoverableException connectionError) {
            return new SQLRecoverableException(
                    FAILURE_MESSAGE, connectionError.getSQLState(), connectionError.getErrorCode());
        }
        if (error instanceof SQLTimeoutException timeoutError) {
            return new SQLTimeoutException(
                    FAILURE_MESSAGE, timeoutError.getSQLState(), timeoutError.getErrorCode());
        }
        if (error instanceof SQLException sqlError) {
            return new SQLException(FAILURE_MESSAGE, sqlError.getSQLState(), sqlError.getErrorCode());
        }
        return new SQLException(FAILURE_MESSAGE);
    }

    @FunctionalInterface
    private interface TemporalAdapter {

        TemporalAdapter IDENTITY = (value, resultSet) -> value;

        Object adapt(Object value, ResultSet resultSet) throws SQLException;
    }
}
