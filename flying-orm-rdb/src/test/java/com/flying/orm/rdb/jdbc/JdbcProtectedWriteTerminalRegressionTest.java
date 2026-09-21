package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcProtectedWriteTerminalRegressionTest {

    @Test
    void acquisitionFatalKeepsIdentityBeforeALeaseExists() {
        Driver driver = new Driver();
        driver.acquisitionFailure = driver.fatal;
        Throwable failure = assertThrows(Throwable.class,
                () -> driver.executor(false).rowsUpdated(work(false).writeRequest()));
        assertAll(() -> assertSame(driver.fatal, failure), () -> driver.assertOwnership(0, 0, 0, 0));
    }

    @Test
    void externalGeneratedKeyFatalKeepsIdentityWithoutTakingTransactionOwnership() {
        Driver driver = new Driver();
        driver.keyFailure = driver.fatal;
        Throwable failure = assertThrows(Throwable.class,
                () -> driver.executor(true).protectedWrite(work(true), SqlExecutionOptions.safeDefaults()));
        assertAll(() -> assertSame(driver.fatal, failure),
                () -> driver.assertOwnership(0, 0, 0, 0),
                () -> assertEquals(1, driver.acquisitions),
                () -> assertEquals(1, driver.keyReads),
                () -> assertEquals(1, driver.statementsClosed));
    }

    @Test
    void externalProtectedSuccessRetainsExternalSource() {
        Driver driver = new Driver();
        driver.executor(true).protectedWrite(work(false), SqlExecutionOptions.safeDefaults());
        driver.assertOwnership(0, 0, 0, 0);
        assertEquals(List.of(com.flying.orm.rdb.observation.SqlExecutionStatus.SUCCESS), driver.sources);
    }

    @Test
    void externalProtectedFailureRetainsExternalSource() {
        Driver driver = new Driver();
        driver.workFailure = new SQLException("write failed", "23000");
        assertThrows(RuntimeException.class,
                () -> driver.executor(true).protectedWrite(work(false), SqlExecutionOptions.safeDefaults()));
        driver.assertOwnership(0, 0, 0, 0);
        assertEquals(List.of(com.flying.orm.rdb.observation.SqlExecutionStatus.ERROR), driver.sources);
    }

    @Test
    void ordinarySqlRetainsAutoCommitSource() {
        Driver driver = new Driver();
        driver.executor(false).rowsUpdated(work(false).writeRequest());
        driver.assertOwnership(0, 0, 0, 0);
        assertEquals(List.of(com.flying.orm.rdb.observation.SqlExecutionStatus.SUCCESS), driver.sources);
    }

    private static ProtectedWriteWork work(boolean generatedOwner) {
        return new ProtectedWriteWork(ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(value) values (?)", List.of(7L)),
                null, List.of("id"), generatedOwner ? Map.of() : Map.of("id", 7L), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
    }

    private static final class Driver implements SqlExecutionObserver {
        private final VirtualMachineError fatal = new VirtualMachineError("driver fatal") { };
        private final List<com.flying.orm.rdb.observation.SqlExecutionStatus> sources = new ArrayList<>();
        private Throwable acquisitionFailure;
        private Throwable keyFailure;
        private SQLException workFailure;
        private int acquisitions;
        private int commits;
        private int rollbacks;
        private int closes;
        private int autoCommitChanges;
        private int keyReads;
        private int statementsClosed;

        private JdbcSqlExecutor executor(boolean external) {
            DataSource dataSource = proxy(DataSource.class, (self, method, args) -> {
                if (method.getName().equals("getConnection")) {
                    acquisitions++;
                    if (acquisitionFailure != null) throw acquisitionFailure;
                    return connection();
                }
                throw new AssertionError(method.getName());
            });
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(new JdbcConnectionAccess() {
                public Connection getConnection(SqlRequest request) throws SQLException { return dataSource.getConnection(); }
                public void releaseConnection(Connection connection, SqlRequest request) { }
            }, RdbDialect.h2()).withObserver(this);
            return external ? executor : executor;
        }

        private Connection connection() {
            return proxy(Connection.class, (self, method, args) -> switch (method.getName()) {
                case "getAutoCommit" -> true;
                case "setAutoCommit" -> {
                    autoCommitChanges++;
                    yield null;
                }
                case "prepareStatement" -> statement((String) args[0]);
                case "commit" -> { commits++; yield null; }
                case "rollback" -> { rollbacks++; yield null; }
                case "close" -> { closes++; yield null; }
                default -> throw new AssertionError(method.getName());
            });
        }

        private PreparedStatement statement(String sql) {
            return proxy(PreparedStatement.class, (self, method, args) -> switch (method.getName()) {
                case "setObject", "setBytes", "setBinaryStream", "setQueryTimeout", "addBatch" -> null;
                case "executeLargeUpdate" -> {
                    if (sql.contains("business_row") && workFailure != null) throw workFailure;
                    yield 1L;
                }
                case "executeBatch" -> new int[]{1};
                case "getGeneratedKeys" -> { keyReads++; throw keyFailure; }
                case "close" -> { statementsClosed++; yield null; }
                default -> throw new AssertionError(method.getName());
            });
        }

        private void assertOwnership(int expectedCommits, int expectedRollbacks, int expectedCloses, int changes) {
            assertAll(() -> assertEquals(expectedCommits, commits, "commits"),
                    () -> assertEquals(expectedRollbacks, rollbacks, "rollbacks"),
                    () -> assertEquals(expectedCloses, closes, "connection closes"),
                    () -> assertEquals(changes, autoCommitChanges, "autoCommit changes"));
        }



        @Override
        public void onExecution(SqlExecutionObservation observation) {
            sources.add(observation.status());
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
