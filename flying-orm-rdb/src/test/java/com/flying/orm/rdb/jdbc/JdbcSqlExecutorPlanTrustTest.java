package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcSqlExecutorPlanTrustTest {

    @Test
    void synchronousExecutionReusesTheRequestBoundaryBinarySnapshot() {
        byte[] source = {1, 2, 3};
        SqlRequest request = new SqlRequest("select ?", List.of(source));

        List<Object> executionParameters = JdbcSqlExecutor.snapshotExecutionParameters(request);
        source[0] = 9;

        assertSame(request.parameters(), executionParameters);
        assertEquals(1, ((byte[]) executionParameters.getFirst())[0]);
    }

    @Test
    void hasNoJdbcPlanIdentityCache() {
        assertThrows(NoSuchFieldException.class,
                () -> JdbcSqlExecutor.class.getDeclaredField("planValidations"));
    }

    @Test
    void validatesSqlFromAnExternallyPreparedPlan() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:forged_plan;DB_CLOSE_DELAY=-1");
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select 1; select 2", SqlBindMarkerStyle.CANONICAL, 0,
                        "H2", "select 1"),
                List.of());

        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);

        assertThrows(IllegalArgumentException.class, () -> executor.query(request));
    }

    @Test
    void rejectsForgedMultiStatementPlanBeforeBorrowingConnection() {
        AtomicBoolean borrowed = new AtomicBoolean();
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        borrowed.set(true);
                        throw new AssertionError("untrusted SQL must be rejected before borrowing a connection");
                    }
                    return defaultValue(method.getReturnType());
                });
        SqlRequest request = new SqlRequest(
                SqlStatementPlan.prepared(
                        "select 1; select 2", SqlBindMarkerStyle.CANONICAL, 0,
                        "H2", "select 1"),
                List.of());

        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);

        assertThrows(IllegalArgumentException.class, () -> executor.query(request));
        assertFalse(borrowed.get());
    }

    @Test
    void rejectsNativeMultiStatementBeforeBorrowingConnection() {
        AtomicBoolean borrowed = new AtomicBoolean();
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        borrowed.set(true);
                        throw new AssertionError("native SQL must be validated before borrowing a connection");
                    }
                    return defaultValue(method.getReturnType());
                });

        SqlRequest request = SqlRequest.nativeSql("select 1; delete from accounts", List.of());

        assertThrows(IllegalArgumentException.class, () -> JdbcSqlExecutor.create(dataSource).query(request));
        assertFalse(borrowed.get());
    }

    @Test
    void executesPortableSqlWithoutReadingConnectionMetadata() {
        AtomicBoolean prepared = new AtomicBoolean();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> "executeLargeUpdate".equals(method.getName())
                        ? 1L : defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("getMetaData".equals(method.getName())) {
                        throw new AssertionError("JDBC execution must not guess the dialect from a physical connection");
                    }
                    if ("prepareStatement".equals(method.getName())) {
                        prepared.set(true);
                        return statement;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));

        long rows = JdbcSqlExecutor.create(dataSource).rowsUpdated(
                new SqlRequest("update accounts set active = false", List.of()));

        assertEquals(1L, rows);
        assertTrue(prepared.get());
    }

    @Test
    void keepsCanonicalQuestionMarkersForPostgresqlJdbc() {
        AtomicReference<String> preparedSql = new AtomicReference<>();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> "executeLargeUpdate".equals(method.getName())
                        ? 1L : defaultValue(method.getReturnType()));
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        preparedSql.set((String) arguments[0]);
                        return statement;
                    }
                    return defaultValue(method.getReturnType());
                });
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));

        long rows = JdbcSqlExecutor.create(dataSource, RdbDialect.postgresql()).rowsUpdated(
                new SqlRequest("update accounts set active = false where id = ?", List.of(7L)));

        assertEquals(1L, rows);
        assertEquals("update accounts set active = false where id = ?", preparedSql.get());
    }

    @Test
    void validatesSqlBeforeOpeningAnAdvancedScrollableQuery() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:advanced_scroll_boundary;DB_CLOSE_DELAY=-1");
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);

        SqlRequest request = new SqlRequest("select 1; select 2", List.of());

        assertThrows(IllegalArgumentException.class,
                () -> executor.advanced().scroll(request, resultSet -> null));
    }

    @Test
    void validatesAdvancedScrollableSqlBeforeBorrowingAConnection() {
        AtomicBoolean borrowed = new AtomicBoolean();
        DataSource dataSource = (DataSource) Proxy.newProxyInstance(
                JdbcSqlExecutorPlanTrustTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        borrowed.set(true);
                        throw new AssertionError("advanced SQL must be rejected before borrowing a connection");
                    }
                    return defaultValue(method.getReturnType());
                });
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        SqlRequest request = new SqlRequest("select 1; update account set active=0", List.of());

        assertThrows(IllegalArgumentException.class,
                () -> executor.advanced().scroll(request, resultSet -> null));
        assertFalse(borrowed.get());
    }

    @Test
    void rejectsMysqlConditionalCommentAtThePortableBoundary() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:mysql_lexical_boundary;DB_CLOSE_DELAY=-1");
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource);
        SqlRequest request = new SqlRequest("select 1 /*! hidden */", List.of());

        assertThrows(IllegalArgumentException.class,
                () -> executor.requireSingle(request));
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
