package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcGeneratedBatchCompletionBoundaryTest {

    @TestFactory
    List<DynamicTest> priorCompletedRowsDependOnStatementClosureNotSuppressedDiagnostics() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean keyReadFails : List.of(true, false)) {
                for (boolean statementCloseFails : List.of(false, true)) {
                    tests.add(DynamicTest.dynamicTest(dialect.name() + "/keyReadFails=" + keyReadFails
                            + "/statementCloseFails=" + statementCloseFails, () -> {
                        Fixture fixture = new Fixture(keyReadFails, statementCloseFails);
                        List<Long> completed = new ArrayList<>();
                        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                                () -> JdbcBatchWriter.create(fixture.access(), dialect)
                                        .writeBatch(fixture.request(), offset -> {
                                            assertEquals(1, fixture.statementCloses,
                                                    "shared statement must close before completed-row notification");
                                            assertEquals(0, fixture.releases,
                                                    "row completion precedes the upper connection release");
                                            completed.add(offset);
                                        }));

                        assertSame(fixture.primary, failure.getCause());
                        assertTrue(Arrays.asList(fixture.primary.getSuppressed()).contains(fixture.diagnostic));
                        if (statementCloseFails) {
                            assertTrue(Arrays.asList(fixture.primary.getSuppressed())
                                    .contains(fixture.statementCloseFailure));
                        }
                        assertEquals(2, failure.evidence().successfulCount(),
                                "both INSERT counts were delivered, independently of key handoff");
                        assertEquals(2, failure.evidence().affectedRows().value());
                        assertEquals(List.of(0L), fixture.appliedKeys,
                                "only the first row completed key handoff");
                        assertEquals(2, fixture.keyResultCloses);
                        assertEquals(1, fixture.statementCloses);
                        assertEquals(1, fixture.releases);
                        assertEquals(statementCloseFails ? List.of() : List.of(0L), completed,
                                "unrelated suppressed failures must not hide successful shared-statement cleanup");
                    }));
                }
            }
        }
        return tests;
    }

    private static final class Fixture {
        private final boolean keyReadFails;
        private final boolean statementCloseFails;
        private final Throwable primary;
        private final SQLException diagnostic = new SQLException("key result cleanup or callback diagnostic");
        private final SQLException statementCloseFailure = new SQLException("shared statement close failed");
        private final List<Long> appliedKeys = new ArrayList<>();
        private int executedRows;
        private int keyResultCloses;
        private int statementCloses;
        private int releases;

        private Fixture(boolean keyReadFails, boolean statementCloseFails) {
            this.keyReadFails = keyReadFails;
            this.statementCloseFails = statementCloseFails;
            primary = keyReadFails ? new SQLException("second key read failed")
                    : new IllegalStateException("second key assignment failed");
            if (!keyReadFails) primary.addSuppressed(diagnostic);
        }

        private BatchWriteRequest request() {
            return new BatchWriteRequest(SqlStatementPlan.canonical(
                    "insert into samples(label) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                    List.of(String.class), Flux.just(new Object[]{"first"}, new Object[]{"second"}),
                    BatchWriteOptions.of(2), BatchRowCountPolicy.ANY,
                    BatchGeneratedKeys.required("id", (offset, row) -> {
                        if (offset == 1L && !keyReadFails) throw (RuntimeException) primary;
                        assertEquals(offset + 1L, row.value(0));
                        appliedKeys.add(offset);
                    }));
        }

        private JdbcConnectionAccess access() {
            Connection connection = proxy(Connection.class, (self, method, arguments) -> {
                if (method.getName().equals("prepareStatement")) return statement();
                throw new AssertionError("unexpected connection ownership call: " + method.getName());
            });
            return new JdbcConnectionAccess() {
                @Override
                public Connection getConnection(SqlRequest request) {
                    return connection;
                }

                @Override
                public void releaseConnection(Connection actual, SqlRequest request) {
                    assertSame(connection, actual);
                    releases++;
                }
            };
        }

        private PreparedStatement statement() {
            return proxy(PreparedStatement.class, (self, method, arguments) -> switch (method.getName()) {
                case "setObject" -> null;
                case "executeLargeUpdate" -> {
                    executedRows++;
                    yield 1L;
                }
                case "getGeneratedKeys" -> keys(executedRows);
                case "close" -> {
                    statementCloses++;
                    if (statementCloseFails) throw statementCloseFailure;
                    yield null;
                }
                default -> throw new AssertionError("unexpected statement call: " + method.getName());
            });
        }

        private ResultSet keys(int rowNumber) {
            ResultSetMetaData metadata = proxy(ResultSetMetaData.class,
                    (self, method, arguments) -> switch (method.getName()) {
                        case "getColumnCount" -> 1;
                        case "getColumnLabel", "getColumnName" -> "id";
                        case "getColumnType" -> Types.BIGINT;
                        default -> throw new AssertionError("unexpected key metadata call: " + method.getName());
                    });
            int[] nextCalls = {0};
            return proxy(ResultSet.class, (self, method, arguments) -> switch (method.getName()) {
                case "next" -> {
                    if (rowNumber == 2 && keyReadFails) throw primary;
                    yield nextCalls[0]++ == 0;
                }
                case "getMetaData" -> metadata;
                case "getObject" -> (long) rowNumber;
                case "close" -> {
                    keyResultCloses++;
                    if (rowNumber == 2 && keyReadFails) throw diagnostic;
                    yield null;
                }
                default -> throw new AssertionError("unexpected key result call: " + method.getName());
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
