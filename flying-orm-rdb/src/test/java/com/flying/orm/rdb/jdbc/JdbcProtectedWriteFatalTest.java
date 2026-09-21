package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcProtectedWriteFatalTest {

    @TestFactory
    Stream<DynamicTest> cleanupFatalSurvivesEveryProtectedWriteResourceBoundary() {
        return Stream.of(FailureStage.values()).map(stage -> DynamicTest.dynamicTest(stage.name(), () -> {
            Driver driver = new Driver();
            driver.failureStage = stage;
            ProtectedWriteWork original = work();
            ProtectedWriteWork request = new ProtectedWriteWork(
                    stage == FailureStage.OWNER ? ProtectedWriteWork.Kind.UPDATE
                            : stage == FailureStage.DELETE ? ProtectedWriteWork.Kind.UPSERT : original.kind(),
                    original.writeRequest(), new SqlRequest("select id from business_row", List.of()),
                    original.ownerFields(), stage == FailureStage.KEYS ? Map.of() : original.knownOwner(),
                    original.ownerPredicateSql(), original.deleteSql(), original.insertSql(), original.fields());

            Throwable failure = assertThrows(Throwable.class, () -> driver.executor()
                    .protectedWrite(request, SqlExecutionOptions.safeDefaults()));

            assertSame(driver.fatal, failure);
            assertEquals(1, driver.releases);
            assertEquals(stage == FailureStage.INSERT || stage == FailureStage.DELETE ? 2 : 1,
                    driver.statementCloses);
            assertEquals(stage == FailureStage.KEYS ? 1 : 0, driver.resultCloses);
            assertEquals(0, driver.closes);
            assertEquals(0, driver.commits);
            assertEquals(0, driver.rollbacks);
        }));
    }

    private enum FailureStage { OWNER, KEYS, INSERT, DELETE }

    @Test
    void statementFatalRemainsPrimaryWhenReleaseAlsoFailsFatally() {
        Driver driver = new Driver();
        driver.businessFailure = new java.sql.SQLException("business failed", "42000");
        driver.statementCloseFailure = driver.fatal;
        driver.releaseFailure = new VirtualMachineError("release fatal") { };

        Throwable failure = assertThrows(Throwable.class, () -> driver.executor()
                .protectedWrite(work(), SqlExecutionOptions.safeDefaults()));

        assertSame(driver.fatal, failure);
        assertEquals(List.of(driver.releaseFailure), List.of(failure.getSuppressed()));
        assertEquals(1, driver.releases);
        assertEquals(1, driver.statementCloses);
        assertEquals(0, driver.closes);
    }

    @Test
    void statementCloseFatalTakesPrecedenceOverOrdinaryBusinessFailure() {
        Driver driver = new Driver();
        driver.businessFailure = new java.sql.SQLException("business failed", "42000");
        driver.statementCloseFailure = driver.fatal;

        Throwable failure = assertThrows(Throwable.class, () -> driver.executor()
                .protectedWrite(work(), SqlExecutionOptions.safeDefaults()));

        assertEquals(1, driver.statementCloses);
        assertEquals(0, driver.closes);
        assertSame(driver.fatal, failure);
    }

    @Test
    void businessFatalPreservesIdentityWithoutCompletingExternalTransaction() {
        Driver driver = new Driver();
        driver.businessFailure = driver.fatal;

        Throwable failure = assertThrows(VirtualMachineError.class, () -> driver.executor()

                .protectedWrite(work(), SqlExecutionOptions.safeDefaults()));

        assertEquals(0, driver.commits);
        assertEquals(0, driver.rollbacks);
        assertEquals(0, driver.closes);
        assertSame(driver.fatal, failure);
    }

    @Test
    void externalTransactionRemainsCallerOwned() {
        Driver driver = new Driver();
        driver.commitFailure = driver.fatal;
        JdbcSqlExecutor executor = driver.executor();

        assertEquals(1L, executor.protectedWrite(work(), SqlExecutionOptions.safeDefaults()).affectedRows());
        assertEquals(0, driver.commits);
        assertEquals(0, driver.rollbacks);
        assertEquals(0, driver.closes);
    }

    private static ProtectedWriteWork work() {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(id) values (?)", List.of(7L)),
                null, List.of("id"), Map.of("id", 7L), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
    }

    /** Only the driver is replaced; transaction ownership and cleanup use the real executor. */
    private static final class Driver {
        private final VirtualMachineError fatal = new VirtualMachineError("driver fatal") { };
        private Throwable commitFailure;
        private Throwable businessFailure;
        private Throwable statementCloseFailure;
        private Error releaseFailure;
        private int statementCloses;
        private int commits;
        private int rollbacks;
        private int closes;
        private int releases;
        private int resultCloses;
        private FailureStage failureStage;

        private JdbcSqlExecutor executor() {
            return JdbcSqlExecutor.create(new JdbcConnectionAccess() {
                @Override public Connection getConnection(SqlRequest request) { return connection(); }
                @Override public void releaseConnection(Connection connection, SqlRequest request) {
                    releases++;
                    if (releaseFailure != null) throw releaseFailure;
                }
            }, RdbDialect.h2());
        }

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "getAutoCommit" -> true;
                        case "setAutoCommit" -> null;
                        case "prepareStatement" -> statement((String) arguments[0]);
                        case "commit" -> {
                            commits++;
                            if (commitFailure != null) throw commitFailure;
                            yield null;
                        }
                        case "rollback" -> { rollbacks++; yield null; }
                        case "close" -> { closes++; yield null; }
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private PreparedStatement statement(String sql) {
            boolean fail = failureStage == FailureStage.OWNER && sql.startsWith("select")
                    || failureStage == FailureStage.INSERT && sql.startsWith("insert into token_index")
                    || failureStage == FailureStage.DELETE && sql.startsWith("delete from token_index");
            return (PreparedStatement) Proxy.newProxyInstance(
                    PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "setObject", "setBytes", "setBinaryStream", "setMaxRows", "addBatch" -> null;
                        case "close" -> {
                            statementCloses++;
                            if (fail) throw fatal;
                            if (statementCloseFailure != null) throw statementCloseFailure;
                            yield null;
                        }
                        case "executeLargeUpdate" -> {
                            if (sql.contains("business_row") && businessFailure != null) throw businessFailure;
                            yield 1L;
                        }
                        case "executeQuery" -> throw new java.sql.SQLException("owner read failed", "42000");
                        case "executeBatch" -> {
                            if (fail) throw new java.sql.SQLException("execution failed", "42000");
                            yield new int[]{1};
                        }
                        case "getGeneratedKeys" -> Proxy.newProxyInstance(
                                java.sql.ResultSet.class.getClassLoader(), new Class<?>[]{java.sql.ResultSet.class},
                                (resultProxy, resultMethod, resultArgs) -> {
                                    if (resultMethod.getName().equals("close")) {
                                        resultCloses++;
                                        throw fatal;
                                    }
                                    throw new java.sql.SQLException("key read failed", "42000");
                                });
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
