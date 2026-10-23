package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.bootstrap.FlyingOrmClients;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import java.util.function.Function;
import java.util.function.IntFunction;
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
    void formStreamingUsesBoundMappingWithoutBufferingOrReadingPastTheStop() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:form-stream-bound;DB_CLOSE_DELAY=-1");
        try (Connection connection = source.getConnection(); java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table stream_item as select X as ID from system_range(1, 3)");
        }
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger bindings = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        RowMapper<Integer> mapper = new RowMapper<>() {
            @Override
            public Integer map(DynamicRow row) {
                throw new AssertionError("streaming must use the same bound mapping as list queries");
            }

            @Override
            public Function<IntFunction<Object>, Integer> bindColumns(DynamicRow columns) {
                bindings.incrementAndGet();
                return values -> {
                    org.junit.jupiter.api.Assertions.assertFalse(closed.get());
                    return ((Number) values.apply(0)).intValue();
                };
            }
        };
        try (FlyingOrmClients clients = FlyingOrmClients.builder(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(source, nextCalls, closed))).configuredDialect("h2").build()) {
            DynamicForm form = DynamicForm.builder("stream_item", "stream_item")
                    .addField(DynamicField.of("ID", "BIGINT")).build();
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build());
            List<Integer> visited = new ArrayList<>();
            assertEquals(2L, clients.syncForms().forEachWhile(spec, mapper, value -> {
                visited.add(value);
                assertEquals(value.intValue(), nextCalls.get());
                return value < 2;
            }));
            assertEquals(List.of(1, 2), visited);
            assertEquals(2, nextCalls.get());
            assertTrue(closed.get());
            assertEquals(1, bindings.get());

            nextCalls.set(0);
            closed.set(false);
            visited.clear();
            assertEquals(3L, clients.syncForms().forEach(spec, mapper, visited::add));
            assertEquals(List.of(1, 2, 3), visited);
            assertEquals(4, nextCalls.get());
            assertEquals(2, bindings.get());
            assertTrue(closed.get());

            nextCalls.set(0);
            closed.set(false);
            IllegalStateException failure = new IllegalStateException("stream consumer failed");
            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> clients.syncForms().forEach(spec, mapper, value -> { throw failure; })));
            assertEquals(1, nextCalls.get());
            assertTrue(closed.get());

            nextCalls.set(0);
            closed.set(false);
            List<Id> budgeted = new ArrayList<>();
            assertEquals(3L, clients.syncForms().forEach(
                    spec.withExecutionOptions(SqlExecutionOptions.unlimited().withMaxResultBytes(4096)),
                    Id.class, budgeted::add));
            assertEquals(List.of(new Id(1L), new Id(2L), new Id(3L)), budgeted);
            assertTrue(closed.get());
        }
    }

    @Test
    void bindsColumnsOnceAndMapsInsideTheSameCursorAndCleanupBoundary() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:bound-mapping;DB_CLOSE_DELAY=-1");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger bindings = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2()).withObserver(observations::add);
        RowMapper<Integer> mapper = new RowMapper<>() {
            public Integer map(DynamicRow row) { throw new AssertionError("bound mapping must not materialize each row"); }
            public Function<IntFunction<Object>, Integer> bindColumns(DynamicRow columns) {
                bindings.incrementAndGet();
                assertEquals("ID", columns.columnName(0));
                return values -> {
                    org.junit.jupiter.api.Assertions.assertFalse(closed.get());
                    assertEquals(nextCalls.get(), ((Number) values.apply(0)).intValue());
                    return ((Number) values.apply(0)).intValue();
                };
            }
        };
        SqlRequest request = new SqlRequest("select X as id from system_range(1, 3)", List.of());
        assertThrows(IllegalArgumentException.class, () -> executor.queryMapped(request, null, mapper, -1));
        assertEquals(0, nextCalls.get());
        assertEquals(List.of(1, 2), executor.queryMapped(request, null, mapper, 2));
        assertEquals(1, bindings.get());
        assertEquals(2, nextCalls.get());
        assertTrue(closed.get());
        assertEquals(2L, observations.getFirst().rows());

        nextCalls.set(0);
        closed.set(false);
        try (Connection connection = dataSource.getConnection(); java.sql.Statement statement = connection.createStatement()) {
            statement.execute("create table series as select X as ID from system_range(1, 3)");
        }
        try (FlyingOrmClients clients = FlyingOrmClients.builder(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed))).configuredDialect("h2").build()) {
            DynamicForm form = DynamicForm.builder("series", "series")
                    .addField(DynamicField.of("ID", "BIGINT")).build();
            assertEquals(List.of(1, 2), clients.syncForms().selectMapped(
                    QuerySpec.of(form, ConditionGroup.and().build()), mapper, 2));
            assertEquals(2, bindings.get());
            assertTrue(closed.get());
        }
    }

    @Test
    void boundFailureStopsReadingAndReleasesResourcesWithoutChangingObservedCount() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:bound-failure");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2()).withObserver(observations::add);
        IllegalStateException expected = new IllegalStateException("bound failure");
        RowMapper<Long> mapper = new RowMapper<>() {
            public Long map(DynamicRow row) { throw new AssertionError("bound mapper expected"); }
            public Function<IntFunction<Object>, Long> bindColumns(DynamicRow columns) {
                return values -> {
                    long value = ((Number) values.apply(0)).longValue();
                    if (value == 2) throw expected;
                    return value;
                };
            }
        };
        assertSame(expected, assertThrows(IllegalStateException.class, () -> executor.queryMapped(
                new SqlRequest("select X as id from system_range(1, 3)", List.of()), null, mapper, 0)));
        assertEquals(2, nextCalls.get());
        assertTrue(closed.get());
        assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
        assertEquals(1L, observations.getFirst().rows());
    }

    @Test
    void configuredResultBudgetStillPrecedesEntityMappingAndDoesNotChangeResults() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:bound-budget");
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(dataSource), RdbDialect.h2());
        SqlRequest request = new SqlRequest("select X as id from system_range(1, 3)", List.of());
        RowMapper<Id> mapper = RowMapper.of(Id.class);
        List<Id> expected = List.of(new Id(1L), new Id(2L), new Id(3L));
        assertEquals(expected, executor.queryMapped(request, null, mapper, 0));
        assertEquals(expected, executor.queryMapped(request, SqlExecutionOptions.unlimited().withMaxResultBytes(4096), mapper, 0));
        assertThrows(com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException.class,
                () -> executor.queryMapped(request, SqlExecutionOptions.unlimited().withMaxResultBytes(1), mapper, 0));
    }

    private record Id(Long id) { }
    @Test
    void normalStopIncludesCurrentRowWithoutReadingAheadAndReportsSuccess() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:while-terminal");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2())
                .withObserver(observations::add);
        List<Integer> visited = new ArrayList<>();
        assertEquals(2L, executor.queryWhile(new SqlRequest("select X from system_range(1, 3)", List.of()),
                com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited().withMaxRows(2), row -> {
                    int value = ((Number) row.value(0)).intValue();
                    visited.add(value);
                    org.junit.jupiter.api.Assertions.assertFalse(closed.get());
                    return value < 2;
                }));
        assertEquals(List.of(1, 2), visited);
        assertEquals(2, nextCalls.get());
        assertTrue(closed.get());
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
        assertEquals(2L, observations.getFirst().rows());
    }

    @Test
    void stoppableConsumptionKeepsEmptyErrorAndCapacityBehavior() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:while-options");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2())
                .withObserver(observations::add);
        assertEquals(0L, executor.queryWhile(new SqlRequest("select 1 where 1 = 0", List.of()),
                null, row -> { throw new AssertionError("empty query invoked callback"); }));
        assertTrue(closed.get());
        closed.set(false);
        nextCalls.set(0);
        SqlRequest request = new SqlRequest("select X from system_range(1, 3)", List.of());
        IllegalStateException expected = new IllegalStateException("visitor failed");
        assertSame(expected, assertThrows(IllegalStateException.class, () -> executor.queryWhile(
                request, null, row -> { throw expected; })));
        assertEquals(1, nextCalls.get());
        assertTrue(closed.get());
        assertEquals(0L, observations.getLast().rows());
        assertEquals(SqlExecutionStatus.ERROR, observations.getLast().status());
        closed.set(false);
        AtomicInteger visited = new AtomicInteger();
        assertThrows(com.flying.orm.rdb.execution.SqlRowLimitExceededException.class, () -> executor.queryWhile(
                request, com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited().withMaxRows(1),
                row -> { visited.incrementAndGet(); return true; }));
        assertEquals(1, visited.get());
        assertTrue(closed.get());
    }

    @Test
    void consumesRowsBeforeReadingTheNextWithoutCollectingAndClosesBeforeReturning() throws Exception {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:each-terminal");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2());
        AtomicInteger consumed = new AtomicInteger();
        long count = executor.queryEach(new SqlRequest("select X as id from system_range(1, 3)", List.of()),
                null, row -> {
                    assertEquals(consumed.incrementAndGet(), nextCalls.get());
                    assertEquals(consumed.get(), ((Number) row.value(0)).intValue());
                    org.junit.jupiter.api.Assertions.assertFalse(closed.get());
                });
        assertEquals(3L, count);
        assertEquals(4, nextCalls.get());
        assertTrue(closed.get());
    }

    @Test
    void rowConsumerFailureStopsReadingAndClosesResources() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:each-failure");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2());
        IllegalStateException expected = new IllegalStateException("consumer failed");
        assertSame(expected, assertThrows(IllegalStateException.class, () -> executor.queryEach(
                new SqlRequest("select X as id from system_range(1, 3)", List.of()), null,
                row -> { throw expected; })));
        assertEquals(1, nextCalls.get());
        assertTrue(closed.get());
    }

    @Test
    void rowConsumptionRetainsConfiguredLimitsAndEmptyResultBehavior() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:each-options");
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(
                        countingDataSource(dataSource, nextCalls, closed)), RdbDialect.h2());
        AtomicInteger consumed = new AtomicInteger();
        assertEquals(0L, executor.queryEach(new SqlRequest("select 1 where 1 = 0", List.of()),
                null, row -> consumed.incrementAndGet()));
        assertEquals(0, consumed.get());
        assertTrue(closed.get());
        closed.set(false);
        assertThrows(com.flying.orm.rdb.execution.SqlRowLimitExceededException.class, () -> executor.queryEach(
                new SqlRequest("select X as id from system_range(1, 3)", List.of()),
                com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited().withMaxRows(1),
                row -> consumed.incrementAndGet()));
        assertEquals(1, consumed.get());
        assertTrue(closed.get());
    }


    @Test
    void mapsEachRowBeforeAdvancingAndClosesBeforePublishingTheLimitedResult() throws Exception {
        JdbcDataSource delegate = new JdbcDataSource();
        delegate.setURL("jdbc:h2:mem:mapped-terminal;DB_CLOSE_DELAY=-1");
        try (Connection connection = delegate.getConnection()) {
            connection.createStatement().execute("create table item(id int primary key)");
            connection.createStatement().execute("insert into item values (1), (2), (3)");
        }
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicInteger firstMappedAfterNextCalls = new AtomicInteger();
        AtomicBoolean resultSetClosed = new AtomicBoolean();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(countingDataSource(delegate, nextCalls, resultSetClosed)), RdbDialect.h2());

        List<Integer> rows = executor.queryMapped(
                new SqlRequest("select id from item order by id", List.of()),
                null,
                row -> {
                    firstMappedAfterNextCalls.compareAndSet(0, nextCalls.get());
                    org.junit.jupiter.api.Assertions.assertFalse(resultSetClosed.get());
                    return ((Number) row.value(0)).intValue();
                },
                2);

        assertEquals(List.of(1, 2), rows);
        assertEquals(1, firstMappedAfterNextCalls.get());
        assertEquals(2, nextCalls.get());
        assertTrue(resultSetClosed.get());
    }

    @Test
    void mapperFailureStopsReadingAndReportsFailedConsumptionAfterCleanup() throws Exception {
        JdbcDataSource delegate = new JdbcDataSource();
        delegate.setURL("jdbc:h2:mem:mapped-observation;DB_CLOSE_DELAY=-1");
        try (Connection connection = delegate.getConnection()) {
            connection.createStatement().execute("create table item(id int primary key)");
            connection.createStatement().execute("insert into item values (1), (2), (3)");
        }
        AtomicBoolean resultSetClosed = new AtomicBoolean();
        AtomicInteger nextCalls = new AtomicInteger();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.jdbc(countingDataSource(delegate, nextCalls, resultSetClosed)), RdbDialect.h2())
                .withObserver(observations::add);
        IllegalStateException expected = new IllegalStateException("mapping failed");

        IllegalStateException actual = assertThrows(IllegalStateException.class, () -> executor.queryMapped(
                new SqlRequest("select id from item", List.of()),
                null,
                row -> {
                    org.junit.jupiter.api.Assertions.assertFalse(resultSetClosed.get());
                    throw expected;
                },
                0));

        assertSame(expected, actual);
        assertEquals(1, nextCalls.get());
        assertTrue(resultSetClosed.get());
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
        assertSame(expected, observations.getFirst().error());
        assertEquals(0L, observations.getFirst().rows());
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
