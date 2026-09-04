package com.flying.orm.rdb.result;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** JDBC 厂商时间对象必须在 ResultSet 资源域内转换为稳定 java.time 值。 */
class JdbcDynamicRowFactoryTest {

    @Test
    void materializesSqlServerDateTimeOffsetWithoutACompileTimeDriverDependency() throws SQLException {
        Instant instant = Instant.parse("2026-08-21T03:04:05.123456Z");
        Timestamp timestamp = Timestamp.from(instant);
        microsoft.sql.DateTimeOffset driverValue = new microsoft.sql.DateTimeOffset(timestamp, 8 * 60);

        DynamicRow row = read(driverValue, connection());

        assertEquals(instant.atOffset(ZoneOffset.ofHours(8)), row.value(0));
    }

    @Test
    void materializesOracleOffsetTimestampsWithTheActualResultSetConnection() throws SQLException {
        Connection connection = connection();
        OffsetDateTime zoned = OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00");
        oracle.sql.TIMESTAMPTZ timestampWithZone = new oracle.sql.TIMESTAMPTZ(zoned);
        oracle.sql.TIMESTAMPLTZ timestampWithLocalZone = new oracle.sql.TIMESTAMPLTZ(zoned);

        assertEquals(zoned, read(timestampWithZone, connection).value(0));
        assertSame(connection, timestampWithZone.connection());
        assertEquals(zoned, read(timestampWithLocalZone, connection).value(0));
        assertSame(connection, timestampWithLocalZone.connection());
    }

    @Test
    void sanitizesArbitrarilyWrappedAdapterFailuresWithoutMiningTheirCauseGraph() {
        OutOfMemoryError fatal = new OutOfMemoryError("secret fatal");
        IllegalStateException wrapper = new IllegalStateException("secret wrapper", fatal);
        oracle.sql.TIMESTAMPTZ fatalValue = new oracle.sql.TIMESTAMPTZ(null, wrapper);

        SQLException wrappedFailure = assertThrows(SQLException.class, () -> read(fatalValue, connection()));
        assertEquals("failed to materialize JDBC temporal value", wrappedFailure.getMessage());
        assertNull(wrappedFailure.getCause());

        oracle.sql.TIMESTAMPTZ ordinaryValue = new oracle.sql.TIMESTAMPTZ(
                null, new IllegalStateException("secret driver detail"));
        SQLException failure = assertThrows(SQLException.class, () -> read(ordinaryValue, connection()));
        assertEquals("failed to materialize JDBC temporal value", failure.getMessage());
        assertNull(failure.getCause());
    }

    @Test
    void keepsSanitizedConnectionFailuresClassifiableWithoutExposingDriverDetails() {
        oracle.sql.TIMESTAMPTZ value = new oracle.sql.TIMESTAMPTZ(
                OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00"));
        SQLTransientConnectionException transientFailure = assertThrows(
                SQLTransientConnectionException.class,
                () -> JdbcDynamicRowFactory.from(
                        resultSetWithStatementFailure(
                                value, new SQLTransientConnectionException("secret transient detail", null, 701)),
                        SqlExecutionOptions.safeDefaults()).readCurrentRow());
        SQLNonTransientConnectionException permanentFailure = assertThrows(
                SQLNonTransientConnectionException.class,
                () -> JdbcDynamicRowFactory.from(
                        resultSetWithStatementFailure(
                                value, new SQLNonTransientConnectionException("secret permanent detail", " ", 702)),
                        SqlExecutionOptions.safeDefaults()).readCurrentRow());

        assertSanitizedConnectionFailure(transientFailure, null, 701);
        assertSanitizedConnectionFailure(permanentFailure, " ", 702);
    }

    @Test
    void keepsSanitizedTimeoutFailureClassifiableWithoutSqlState() {
        oracle.sql.TIMESTAMPTZ value = new oracle.sql.TIMESTAMPTZ(
                OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00"));

        SQLTimeoutException failure = assertThrows(
                SQLTimeoutException.class,
                () -> JdbcDynamicRowFactory.from(
                        resultSetWithStatementFailure(
                                value, new SQLTimeoutException("secret timeout detail", null, 703)),
                        SqlExecutionOptions.safeDefaults()).readCurrentRow());

        assertEquals("failed to materialize JDBC temporal value", failure.getMessage());
        assertNull(failure.getCause());
        assertNull(failure.getSQLState());
        assertEquals(703, failure.getErrorCode());
        RdbException translated = (RdbException) RdbExceptionTranslator.translate(failure);
        assertEquals(RdbErrorKind.TIMEOUT, translated.kind());
    }

    @Test
    void keepsSanitizedRecoverableFailureClassifiableWithoutSqlState() {
        oracle.sql.TIMESTAMPTZ value = new oracle.sql.TIMESTAMPTZ(
                OffsetDateTime.parse("2026-08-21T11:04:05.123456+08:00"));

        SQLRecoverableException failure = assertThrows(
                SQLRecoverableException.class,
                () -> JdbcDynamicRowFactory.from(
                        resultSetWithStatementFailure(
                                value, new SQLRecoverableException("secret recoverable detail", null, 704)),
                        SqlExecutionOptions.safeDefaults()).readCurrentRow());

        assertEquals("failed to materialize JDBC temporal value", failure.getMessage());
        assertNull(failure.getCause());
        assertNull(failure.getSQLState());
        assertEquals(704, failure.getErrorCode());
        RdbException translated = (RdbException) RdbExceptionTranslator.translate(failure);
        assertEquals(RdbErrorKind.CONNECTION, translated.kind());
    }

    private static DynamicRow read(Object value, Connection connection) throws SQLException {
        return JdbcDynamicRowFactory.from(resultSet(value, connection), SqlExecutionOptions.safeDefaults())
                                    .readCurrentRow();
    }

    private static ResultSet resultSet(Object value, Connection connection) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getColumnCount" -> 1;
            case "getColumnLabel", "getColumnName" -> "value";
            default -> defaultValue(method.getReturnType());
        });
        Statement statement = proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getConnection" -> connection;
            default -> defaultValue(method.getReturnType());
        });
        return proxy(ResultSet.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "getObject" -> value;
            case "getStatement" -> statement;
            default -> defaultValue(method.getReturnType());
        });
    }

    private static ResultSet resultSetWithStatementFailure(Object value, SQLException statementFailure) {
        ResultSetMetaData metadata = proxy(ResultSetMetaData.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getColumnCount" -> 1;
            case "getColumnLabel", "getColumnName" -> "value";
            default -> defaultValue(method.getReturnType());
        });
        return proxy(ResultSet.class, (proxy, method, arguments) -> switch (method.getName()) {
            case "getMetaData" -> metadata;
            case "getObject" -> value;
            case "getStatement" -> throw statementFailure;
            default -> defaultValue(method.getReturnType());
        });
    }

    private static void assertSanitizedConnectionFailure(SQLException failure, String sqlState, int errorCode) {
        assertEquals("failed to materialize JDBC temporal value", failure.getMessage());
        assertNull(failure.getCause());
        assertEquals(sqlState, failure.getSQLState());
        assertEquals(errorCode, failure.getErrorCode());
        RdbException translated = (RdbException) RdbExceptionTranslator.translate(failure);
        assertEquals(RdbErrorKind.CONNECTION, translated.kind());
    }

    private static Connection connection() {
        return proxy(Connection.class, (proxy, method, arguments) -> defaultValue(method.getReturnType()));
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
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
}
