package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcMappedQueryTerminalTest {

    @Test
    void stopsAfterTheTerminalRowLimitAndMapsAfterClosingJdbcResources() throws Exception {
        JdbcDataSource delegate = new JdbcDataSource();
        delegate.setURL("jdbc:h2:mem:mapped-terminal;DB_CLOSE_DELAY=-1");
        try (Connection connection = delegate.getConnection()) {
            connection.createStatement().execute("create table item(id int primary key)");
            connection.createStatement().execute("insert into item values (1), (2), (3)");
        }
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger firstMappedAfterNextCalls = new AtomicInteger();
        AtomicBoolean resultSetClosed = new AtomicBoolean();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                countingDataSource(delegate, nextCalls, resultSetClosed));

        List<Integer> rows = executor.queryMapped(
                new SqlRequest("select id from item order by id", List.of()),
                null,
                row -> {
                    firstMappedAfterNextCalls.compareAndSet(0, nextCalls.get());
                    assertTrue(resultSetClosed.get());
                    return ((Number) row.value(0)).intValue();
                },
                2);

        assertEquals(List.of(1, 2), rows);
        assertEquals(2, firstMappedAfterNextCalls.get());
        assertEquals(2, nextCalls.get());
        assertTrue(resultSetClosed.get());
    }

    @Test
    void mapperFailureDoesNotTurnSuccessfulSqlIntoAnErrorObservation() throws Exception {
        JdbcDataSource delegate = new JdbcDataSource();
        delegate.setURL("jdbc:h2:mem:mapped-observation;DB_CLOSE_DELAY=-1");
        try (Connection connection = delegate.getConnection()) {
            connection.createStatement().execute("create table item(id int primary key)");
            connection.createStatement().execute("insert into item values (1)");
        }
        AtomicBoolean resultSetClosed = new AtomicBoolean();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                countingDataSource(delegate, new AtomicInteger(), resultSetClosed))
                .withObserver(observations::add);
        IllegalStateException expected = new IllegalStateException("mapping failed");

        IllegalStateException actual = assertThrows(IllegalStateException.class, () -> executor.queryMapped(
                new SqlRequest("select id from item", List.of()),
                null,
                row -> {
                    assertTrue(resultSetClosed.get());
                    throw expected;
                },
                0));

        assertSame(expected, actual);
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
        assertEquals(1L, observations.getFirst().rows());
    }

    private static DataSource countingDataSource(DataSource delegate,
                                                 AtomicInteger nextCalls,
                                                 AtomicBoolean resultSetClosed) {
        return (DataSource) Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(delegate, method, arguments);
                    return result instanceof Connection connection
                            ? wrapConnection(connection, nextCalls, resultSetClosed)
                            : result;
                });
    }

    private static Connection wrapConnection(Connection connection,
                                             AtomicInteger nextCalls,
                                             AtomicBoolean resultSetClosed) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(connection, method, arguments);
                    return result instanceof PreparedStatement statement
                            ? wrapStatement(statement, nextCalls, resultSetClosed)
                            : result;
                });
    }

    private static PreparedStatement wrapStatement(PreparedStatement statement,
                                                   AtomicInteger nextCalls,
                                                   AtomicBoolean resultSetClosed) {
        return (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> {
                    Object result = invoke(statement, method, arguments);
                    return result instanceof ResultSet resultSet
                            ? wrapResultSet(resultSet, nextCalls, resultSetClosed)
                            : result;
                });
    }

    private static ResultSet wrapResultSet(ResultSet resultSet,
                                           AtomicInteger nextCalls,
                                           AtomicBoolean resultSetClosed) {
        return (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("next")) {
                        nextCalls.incrementAndGet();
                    } else if (method.getName().equals("close")) {
                        resultSetClosed.set(true);
                    }
                    return invoke(resultSet, method, arguments);
                });
    }

    private static Object invoke(Object target,
                                 java.lang.reflect.Method method,
                                 Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }
}
