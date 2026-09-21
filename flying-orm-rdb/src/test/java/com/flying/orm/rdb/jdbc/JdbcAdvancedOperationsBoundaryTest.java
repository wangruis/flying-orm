package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcAdvancedOperationsBoundaryTest {

    @Test
    void publicAdvancedApiHasNoTransactionCallback() {
        assertFalse(Arrays.stream(JdbcAdvancedOperations.class.getMethods())
                .anyMatch(method -> method.getName().equals("transaction")));
    }

    @Test
    void metadataCallbackReceivesDriverMetadataDirectly() {
        Fixture fixture = new Fixture();

        String product = fixture.advanced(true).metadata(metadata -> {
            assertSame(fixture.metadata, metadata);
            return metadata.getDatabaseProductName();
        });

        assertEquals("test-database", product);
        assertEquals(List.of(), fixture.closed);
    }

    @Test
    void scrollCallbackReceivesDriverResultSetAndRetainsBindingOptionsAndReleaseOrder() {
        Fixture fixture = new Fixture();

        String value = fixture.advanced(true).scroll(request(),
                SqlExecutionOptions.safeDefaults().withMaxRows(7).withFetchSize(3), resultSet -> {
                    assertSame(fixture.resultSet, resultSet);
                    assertTrue(resultSet.next());
                    assertEquals("value", resultSet.getString(1));
                    assertFalse(resultSet.next());
                    return "read";
                });

        assertEquals("read", value);
        assertEquals(List.of("select value_col from sample where id = ?",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY), fixture.prepared);
        assertEquals(List.of(1, 42), fixture.bound);
        assertEquals(7L, fixture.maxRows);
        assertEquals(3, fixture.fetchSize);
        assertEquals(List.of("resultSet", "statement"), fixture.closed);
    }

    @Test
    void scrollFailurePreservesCallbackAndSuppressedCleanupErrors() {
        Fixture fixture = new Fixture();
        IllegalArgumentException callbackFailure = new IllegalArgumentException("callback failed");
        fixture.resultSetCloseFailure = new SQLException("result set close failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> fixture.advanced(false).scroll(request(), resultSet -> {
                    throw callbackFailure;
                }));

        assertSame(callbackFailure, thrown);
        assertEquals(List.of(fixture.resultSetCloseFailure), List.of(thrown.getSuppressed()));
        assertEquals(List.of("resultSet", "statement", "connection"), fixture.closed);
    }

    @Test
    void metadataFailurePreservesCallbackAndSuppressedReleaseErrors() {
        Fixture fixture = new Fixture();
        IllegalArgumentException callbackFailure = new IllegalArgumentException("metadata failed");
        fixture.connectionCloseFailure = new SQLException("release failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> fixture.advanced(false).metadata(metadata -> {
                    throw callbackFailure;
                }));

        assertSame(callbackFailure, thrown);
        assertEquals(List.of(fixture.connectionCloseFailure), List.of(thrown.getSuppressed()));
        assertEquals(List.of("connection"), fixture.closed);
    }

    @Test
    void ownedMetadataConnectionIsReleasedAfterSuccessfulRead() {
        Fixture fixture = new Fixture();

        assertEquals("test-database", fixture.advanced(false).metadata(DatabaseMetaData::getDatabaseProductName));

        assertEquals(List.of("connection"), fixture.closed);
    }

    @Test
    void multipleStatementsAreRejectedBeforeConnectionAcquisition() {
        Fixture fixture = new Fixture();

        assertThrows(IllegalArgumentException.class, () -> fixture.advanced(false).scroll(
                SqlRequest.nativeSql("select 1; delete from sample", List.of()), resultSet -> null));

        assertEquals(0, fixture.acquired);
    }

    private static SqlRequest request() {
        return SqlRequest.nativeSql("select value_col from sample where id = ?", List.of(42));
    }

    private static final class Fixture {
        private final List<String> closed = new ArrayList<>();
        private int row;
        private int acquired;
        private int fetchSize;
        private long maxRows;
        private List<Object> prepared;
        private List<Object> bound;
        private SQLException resultSetCloseFailure;
        private SQLException connectionCloseFailure;
        private final DatabaseMetaData metadata = proxy(DatabaseMetaData.class, (view, method, arguments) -> {
            if (method.getName().equals("getDatabaseProductName")) {
                return "test-database";
            }
            throw new AssertionError("unexpected metadata call: " + method.getName());
        });
        private final ResultSet resultSet = proxy(ResultSet.class, (view, method, arguments) -> {
            switch (method.getName()) {
                case "next": return ++row == 1;
                case "getString": return "value";
                case "close":
                    closed.add("resultSet");
                    if (resultSetCloseFailure != null) {
                        throw resultSetCloseFailure;
                    }
                    return null;
                default: throw new AssertionError("unexpected result set call: " + method.getName());
            }
        });
        private final PreparedStatement statement = proxy(PreparedStatement.class, (view, method, arguments) -> {
            switch (method.getName()) {
                case "executeQuery": return resultSet;
                case "setObject": bound = List.of(arguments); return null;
                case "setFetchSize": fetchSize = (int) arguments[0]; return null;
                case "setMaxRows": maxRows = (int) arguments[0]; return null;
                case "setLargeMaxRows": maxRows = (long) arguments[0]; return null;
                case "close": closed.add("statement"); return null;
                default: throw new AssertionError("unexpected statement call: " + method.getName());
            }
        });
        private final Connection connection = proxy(Connection.class, (view, method, arguments) -> {
            switch (method.getName()) {
                case "getMetaData": return metadata;
                case "prepareStatement": prepared = List.of(arguments); return statement;
                case "close":
                    closed.add("connection");
                    if (connectionCloseFailure != null) {
                        throw connectionCloseFailure;
                    }
                    return null;
                default: throw new AssertionError("unexpected connection call: " + method.getName());
            }
        });
        private JdbcAdvancedOperations advanced(boolean borrowed) {
            DataSource dataSource = proxy(DataSource.class, (view, method, arguments) -> {
                if (method.getName().equals("getConnection")) {
                    acquired++;
                    return connection;
                }
                throw new AssertionError("unexpected data source call: " + method.getName());
            });
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(borrowed
                    ? com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection)
                    : com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(dataSource), RdbDialect.h2());
            return (borrowed ? executor : executor).advanced();
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (view, method, arguments) -> method.getName().equals("toString")
                        ? "test " + type.getSimpleName() : handler.invoke(view, method, arguments)));
    }
}
