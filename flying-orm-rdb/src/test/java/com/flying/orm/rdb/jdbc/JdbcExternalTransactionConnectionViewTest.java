package com.flying.orm.rdb.jdbc;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcExternalTransactionConnectionViewTest {

    @Test
    void connectionViewKeepsSafeMetadataAccessWithoutExposingItsConnectionBacklink() throws SQLException {
        Connection[] rawConnection = new Connection[1];
        DatabaseMetaData metadata = metadata(rawConnection);
        rawConnection[0] = connection(metadata);

        Connection view = JdbcExternalTransactionConnectionView.connection(rawConnection[0]);

        assertThrows(SQLException.class, view::commit);
        assertThrows(SQLException.class, view::rollback);
        assertThrows(SQLException.class, view::close);
        assertThrows(SQLException.class, view::createStatement);
        assertThrows(SQLException.class, () -> view.unwrap(Connection.class));
        Savepoint savepoint = view.setSavepoint("task-1");
        view.rollback(savepoint);
        assertEquals("task-1", savepoint.getSavepointName());

        DatabaseMetaData protectedMetadata = view.getMetaData();
        assertEquals("test-database", protectedMetadata.getDatabaseProductName());
        assertThrows(SQLException.class, protectedMetadata::getConnection);
    }

    @Test
    void resultSetViewKeepsRowAccessWithoutExposingItsStatementBacklink() throws SQLException {
        ResultSet view = JdbcExternalTransactionConnectionView.resultSet(resultSet());

        assertTrue(view.next());
        assertEquals("value", view.getString(1));
        assertFalse(view.next());
        assertThrows(SQLException.class, view::getStatement);
        assertThrows(SQLException.class, () -> view.unwrap(ResultSet.class));
    }

    private static Connection connection(DatabaseMetaData metadata) {
        Savepoint savepoint = new Savepoint() {
            @Override
            public int getSavepointId() {
                return 1;
            }

            @Override
            public String getSavepointName() {
                return "task-1";
            }
        };
        return (Connection) Proxy.newProxyInstance(
                JdbcExternalTransactionConnectionViewTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "setSavepoint" -> savepoint;
                    case "rollback" -> null;
                    case "toString" -> "raw-connection";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static DatabaseMetaData metadata(Connection[] connection) {
        return (DatabaseMetaData) Proxy.newProxyInstance(
                JdbcExternalTransactionConnectionViewTest.class.getClassLoader(),
                new Class<?>[]{DatabaseMetaData.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getConnection" -> connection[0];
                    case "getDatabaseProductName" -> "test-database";
                    case "toString" -> "raw-metadata";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ResultSet resultSet() {
        int[] row = {0};
        return (ResultSet) Proxy.newProxyInstance(
                JdbcExternalTransactionConnectionViewTest.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "next" -> ++row[0] == 1;
                    case "getString" -> "value";
                    case "getStatement" -> Proxy.newProxyInstance(
                            JdbcExternalTransactionConnectionViewTest.class.getClassLoader(),
                            new Class<?>[]{java.sql.Statement.class},
                            (statement, statementMethod, statementArguments) ->
                                    defaultValue(statementMethod.getReturnType()));
                    case "toString" -> "raw-result-set";
                    default -> defaultValue(method.getReturnType());
                });
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
        return 0D;
    }
}
