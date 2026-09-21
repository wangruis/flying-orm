package com.flying.orm.rdb.jdbc;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class JdbcConnectionOwnershipTest {
    @Test void sameConnectionScopeAcquiresOnceAndReleasesAfterInnerStatements() {
        var events = new ArrayList<String>();
        var gets = new AtomicInteger();
        var releases = new AtomicInteger();
        SqlRequest first = new SqlRequest("update sample set value_col=?", List.of(1));
        Connection connection = connection(events);
        JdbcConnectionAccess access = new JdbcConnectionAccess() {
            public Connection getConnection(SqlRequest request) {
                assertSame(first, request); gets.incrementAndGet(); return connection;
            }
            public void releaseConnection(Connection actual, SqlRequest request) {
                assertSame(connection, actual); assertSame(first, request);
                releases.incrementAndGet(); events.add("release");
            }
        };
        var executor = JdbcSqlExecutor.create(access, RdbDialect.h2());
        long rows = executor.withConnection(first, bound -> {
            assertEquals(1, bound.rowsUpdated(first));
            return bound.rowsUpdated(new SqlRequest("update sample set value_col=?", List.of(2)));
        });
        assertEquals(1, rows);
        assertEquals(1, gets.get());
        assertEquals(1, releases.get());
        assertEquals(List.of("execute","statement-close","execute","statement-close","release"), events);
    }

    @Test void failureInScopeStillReleasesOnce() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        var executor = JdbcSqlExecutor.create(state.access(), RdbDialect.h2());
        var error = new IllegalArgumentException("work failed");
        assertSame(error, assertThrows(IllegalArgumentException.class, () -> executor.withConnection(
                new SqlRequest("update sample set value_col=?", List.of(1)), bound -> { throw error; })));
        assertEquals(1, state.acquired.get());
        assertEquals(1, state.released.get());
        assertEquals(0, state.closes.get());
    }

    @Test void defaultCustomScopeFailsBeforeWork() {
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            public List<com.flying.orm.rdb.result.DynamicRow> query(SqlRequest r) { return List.of(); }
            public long rowsUpdated(SqlRequest r) { return 1; }
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(SqlRequest r, SqlExecutionOptions o) {
                return new com.flying.orm.rdb.execution.SqlWriteResult(1,List.of());
            }
        };
        assertThrows(UnsupportedOperationException.class, () -> executor.withConnection(
                new SqlRequest("update sample set value_col=?",List.of(1)), ignored -> fail("must not invoke work")));
    }

    @Test void acquisitionFailureNeverReleases() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.acquisitionFailure = new SQLException("get failed");
        var executor = JdbcSqlExecutor.create(state.access(), RdbDialect.h2());
        assertThrows(RuntimeException.class, () -> executor.rowsUpdated(
                new SqlRequest("update sample set value_col=?", List.of(1))));
        assertEquals(1, state.acquired.get());
        assertEquals(0, state.released.get());
    }

    private static Connection connection(List<String> events) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                PreparedStatement.class.getClassLoader(), new Class<?>[]{PreparedStatement.class},
                (self, method, args) -> switch(method.getName()) {
                    case "setObject" -> null;
                    case "executeLargeUpdate" -> { events.add("execute"); yield 1L; }
                    case "close" -> { events.add("statement-close"); yield null; }
                    default -> throw new AssertionError(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class<?>[]{Connection.class},
                (self, method, args) -> switch(method.getName()) {
                    case "prepareStatement" -> statement;
                    default -> throw new AssertionError("external connection must not be changed: "+method.getName());
                });
    }
}
