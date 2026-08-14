package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 高级入口只扩展专有能力，不改变普通执行器的连接和事务规则。 */
class JdbcAdvancedOperationsTest {

    /** 受保护连接的 Object 方法也必须解开反射包装，不能把驱动致命错误降级。 */
    @Test
    void protectedConnectionObjectMethodsPreserveVirtualMachineErrors() {
        OutOfMemoryError failure = new OutOfMemoryError("connection object method fatal");
        Connection delegate = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> {
                    if (method.getName().equals("toString")) {
                        throw failure;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection view = JdbcExternalTransactionConnectionView.connection(delegate);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, view::toString);

        assertSame(failure, observed);
    }

    @Test
    void scrollsAReadOnlyResultSetAndAppliesTheStatementLimits() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_scroll");
        createTable(dataSource);
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource).advanced();

        int visibleRows = advanced.scroll(new SqlRequest("select id from device order by id", List.of()),
                                          SqlExecutionOptions.unlimited()
                                                             .withTimeout(java.time.Duration.ofSeconds(2))
                                                             .withFetchSize(7)
                                                             .withMaxRows(2),
                                          resultSet -> {
                                              assertEquals(java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                           resultSet.getType());
                                              assertTrue(resultSet.last());
                                              assertEquals(2, resultSet.getRow());
                                              assertEquals(2, resultSet.getStatement().getQueryTimeout());
                                              assertEquals(7, resultSet.getStatement().getFetchSize());
                                              return resultSet.getRow();
                                          });

        assertEquals(2, visibleRows);
    }

    @Test
    void exposesMetadataWithoutInventingASqlObservation() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_metadata");
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource).advanced();

        String productName = advanced.metadata(metadata -> metadata.getDatabaseProductName());

        assertTrue(productName.toLowerCase().contains("h2"));
    }

    @Test
    void reusesTheExternalTransactionConnectionAndLetsTheCallerControlSavepoints() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_transaction");
        createTable(dataSource);
        try (Connection external = dataSource.getConnection()) {
            external.setAutoCommit(false);
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(external, "primary")));
            JdbcAdvancedOperations advanced = executor.advanced();

            executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("kept")));
            Savepoint savepoint = advanced.transaction(connection -> {
                assertNotSame(external, connection);
                assertThrows(SQLException.class, connection::commit);
                assertThrows(SQLException.class, connection::rollback);
                assertThrows(SQLException.class, connection::close);
                assertThrows(SQLException.class, () -> connection.setAutoCommit(true));
                assertThrows(SQLException.class, () -> connection.setReadOnly(true));
                assertThrows(SQLException.class, () -> connection.setTransactionIsolation(
                        Connection.TRANSACTION_SERIALIZABLE));
                assertThrows(SQLException.class, () -> connection.setSchema("PUBLIC"));
                assertThrows(SQLException.class, () -> connection.unwrap(Connection.class));
                assertSame(connection, connection.getMetaData().getConnection());
                assertThrows(SQLException.class, connection.getMetaData().getConnection()::commit);
                assertThrows(SQLException.class, connection::createStatement);
                return connection.setSavepoint("before_local_change");
            });
            executor.rowsUpdated(new SqlRequest("insert into device(name) values (?)", List.of("rolled_back")));
            advanced.transaction(connection -> {
                connection.rollback(savepoint);
                connection.releaseSavepoint(savepoint);
                return null;
            });

            // 建表时已有三行；保存点之后插入的 rolled_back 行应消失，只留下保存点前的 kept 行。
            assertFalse(external.isClosed());
            assertEquals(4, rowCount(external));
            try (Connection independent = dataSource.getConnection()) {
                // 若高级入口偷偷提交，独立连接会看到 kept；若偷偷整体回滚，外部连接也不会再看到 kept。
                assertEquals(3, rowCount(independent));
            }
            external.rollback();
        }
    }

    @Test
    void doesNotExposeTheExternalTransactionThroughScrollableResultSet() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_scroll_protected_connection");
        createTable(dataSource);
        try (Connection external = dataSource.getConnection()) {
            external.setAutoCommit(false);
            JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(external, "primary")))
                    .advanced();

            int rows = advanced.scroll(new SqlRequest("select id from device order by id", List.of()), resultSet -> {
                Connection callbackConnection = resultSet.getStatement().getConnection();
                assertNotSame(external, callbackConnection);
                assertThrows(SQLException.class, callbackConnection::close);
                assertThrows(SQLException.class, callbackConnection::rollback);
                assertThrows(SQLException.class, () -> resultSet.getStatement().execute("commit"));
                return resultSet.last() ? resultSet.getRow() : 0;
            });

            assertEquals(3, rows);
            assertFalse(external.isClosed());
            external.rollback();
        }
    }

    @Test
    void protectsConnectionsReturnedThroughExternalTransactionMetadata() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_metadata_protected_connection");
        try (Connection external = dataSource.getConnection()) {
            external.setAutoCommit(false);
            JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource)
                    .withTransactionParticipant(() -> Optional.of(
                            JdbcTransactionContext.external(external, "primary")))
                    .advanced();

            advanced.metadata(metadata -> {
                Connection callbackConnection = metadata.getConnection();
                assertNotSame(external, callbackConnection);
                assertThrows(SQLException.class, callbackConnection::commit);
                try (var tables = metadata.getTables(null, null, "%", null)) {
                    if (tables.getStatement() != null) {
                        assertSame(callbackConnection, tables.getStatement().getConnection());
                        assertThrows(SQLException.class, () -> tables.getStatement().execute("commit"));
                    }
                }
                return null;
            });

            assertFalse(external.isClosed());
            external.rollback();
        }
    }

    @Test
    void rejectsTransactionOperationsWithoutAnExternalTransaction() {
        JdbcDataSource dataSource = dataSource("advanced_transaction_required");
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource).advanced();

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> advanced.transaction(Connection::getAutoCommit));

        assertTrue(error.getMessage().contains("external transaction"));
    }

    @Test
    void publishesTheExistingSqlObserverForScrollableSqlAndNeverReportsFailureAsSuccess() throws Exception {
        JdbcDataSource dataSource = dataSource("advanced_observation");
        createTable(dataSource);
        List<SqlExecutionObservation> observations = new ArrayList<>();
        List<SqlTransactionSource> sources = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource).withObserver(new com.flying.orm.rdb.observation.SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onExecution(SqlExecutionObservation observation, SqlTransactionSource source) {
                observations.add(observation);
                sources.add(source);
            }

            @Override
            public boolean requiresTransactionSource() {
                return true;
            }
        });

        RdbException error = assertThrows(RdbException.class,
                () -> executor.advanced().scroll(new SqlRequest("select id from device", List.of()),
                                                 ignored -> { throw new java.sql.SQLException("bad callback", "42000"); }));

        assertEquals(RdbErrorKind.BAD_SQL, error.kind());
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
        assertEquals(List.of(SqlTransactionSource.AUTO_COMMIT), sources);
    }

    /** 高级 metadata 回调遇到连接级故障时必须淘汰自有连接，不能重新归还连接池。 */
    @Test
    void discardsOwnedConnectionAfterAdvancedConnectionFailure() {
        AtomicInteger aborts = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                (ignored, method, arguments) -> defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "abort" -> {
                        aborts.incrementAndGet();
                        yield null;
                    }
                    case "close" -> {
                        closes.incrementAndGet();
                        yield null;
                    }
                    default -> defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (ignored, method, arguments) -> method.getName().equals("getConnection")
                        ? connection
                        : defaultValue(method.getReturnType()));

        RdbException error = assertThrows(RdbException.class,
                () -> JdbcSqlExecutor.create(dataSource).advanced().metadata(ignored -> {
                    throw new SQLException("connection failed", "08006");
                }));

        assertEquals(RdbErrorKind.CONNECTION, error.kind());
        assertEquals(1, aborts.get());
        assertEquals(0, closes.get());
    }

    /** 回调 VME 与包装它的 close Runtime 不能在 finally 聚合时被互相连成环。 */
    @Test
    void keepsFatalCallbackGraphAcyclicWhenCloseFailureAlreadyCausesIt() {
        OutOfMemoryError callbackFatal = new OutOfMemoryError("callback fatal");
        IllegalStateException closeFailure = new IllegalStateException("close failed", callbackFatal);
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                (ignored, method, arguments) -> defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "close" -> throw closeFailure;
                    case "abort" -> null;
                    default -> defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (ignored, method, arguments) -> method.getName().equals("getConnection")
                        ? connection
                        : defaultValue(method.getReturnType()));

        OutOfMemoryError thrown = assertThrows(OutOfMemoryError.class,
                () -> JdbcSqlExecutor.create(dataSource).advanced().metadata(ignored -> {
                    throw callbackFatal;
                }));

        assertSame(callbackFatal, thrown);
        assertSame(callbackFatal, closeFailure.getCause());
        assertFalse(reaches(callbackFatal, closeFailure));
    }

    /** 公开高级回调用普通异常包装 VME 时，必须恢复原致命错误而不是翻译为普通 RDB 异常。 */
    @Test
    void promotesVirtualMachineErrorNestedInAdvancedCallbackFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("callback nested fatal");
        IllegalStateException wrapper = new IllegalStateException("callback wrapper", fatal);
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource("advanced_nested_callback")).advanced();

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> advanced.metadata(ignored -> { throw wrapper; }));

        assertSame(fatal, observed);
    }

    /** 自有连接关闭用普通异常包装 VME 时，必须先尝试物理淘汰再传播原致命错误。 */
    @Test
    void promotesVirtualMachineErrorNestedInAdvancedCloseFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("close nested fatal");
        IllegalStateException closeFailure = new IllegalStateException("close wrapper", fatal);
        AtomicInteger aborts = new AtomicInteger();
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                (ignored, method, arguments) -> defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "close" -> throw closeFailure;
                    case "abort" -> {
                        aborts.incrementAndGet();
                        yield null;
                    }
                    default -> defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (ignored, method, arguments) -> method.getName().equals("getConnection")
                        ? connection : defaultValue(method.getReturnType()));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> JdbcSqlExecutor.create(dataSource).advanced().metadata(ignored -> null));

        assertSame(fatal, observed);
        assertEquals(1, aborts.get());
    }

    /** 普通回调失败与嵌套关闭 VME 同时发生时，关闭 VME 优先且诊断图不能反向成环。 */
    @Test
    void promotesNestedCloseFatalOverAdvancedCallbackFailure() {
        IllegalStateException primary = new IllegalStateException("callback failed");
        OutOfMemoryError fatal = new OutOfMemoryError("close nested fatal");
        IllegalStateException closeFailure = new IllegalStateException("close wrapper", fatal);
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DatabaseMetaData.class},
                (ignored, method, arguments) -> defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "close" -> throw closeFailure;
                    case "abort" -> null;
                    default -> defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{DataSource.class},
                (ignored, method, arguments) -> method.getName().equals("getConnection")
                        ? connection : defaultValue(method.getReturnType()));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> JdbcSqlExecutor.create(dataSource).advanced().metadata(ignored -> { throw primary; }));

        assertSame(fatal, observed);
        assertTrue(reaches(fatal, primary));
        assertFalse(reaches(primary, fatal));
    }

    private static JdbcDataSource dataSource(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        return dataSource;
    }

    private static void createTable(JdbcDataSource dataSource) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("create table device (id bigint generated by default as identity primary key, "
                    + "name varchar(128) not null)");
            statement.executeUpdate("insert into device(name) values ('a'), ('b'), ('c')");
        }
    }

    private static int rowCount(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement(); var rows = statement.executeQuery("select count(*) from device")) {
            rows.next();
            return rows.getInt(1);
        }
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

    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }
}
