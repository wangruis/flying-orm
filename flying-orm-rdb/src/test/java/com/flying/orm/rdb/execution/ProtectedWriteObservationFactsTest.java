package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedWriteObservationFactsTest {

    @Test
    void jdbcSideIndexFailureKeepsConfirmedBusinessRowsInOneTerminalEvent() {
        JdbcFixture fixture = new JdbcFixture(true, false);
        List<SqlExecutionObservation> events = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        assertThrows(RuntimeException.class, () -> executor.protectedWrite(
                work(), SqlExecutionOptions.safeDefaults()));

        fixture.assertStages(1, 1);
        assertErrorFacts(events);
    }

    @Test
    void r2dbcSideIndexFailureKeepsConfirmedBusinessRowsInOneTerminalEvent() {
        R2dbcFixture fixture = new R2dbcFixture();
        List<SqlExecutionObservation> events = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        assertThrows(RuntimeException.class, () -> executor.protectedWrite(
                work(), SqlExecutionOptions.safeDefaults()).block(Duration.ofSeconds(2)));

        fixture.assertStages();
        assertErrorFacts(events);
    }

    @Test
    void jdbcReleaseFailureKeepsConfirmedBusinessRowsInOneTerminalEvent() {
        JdbcFixture fixture = new JdbcFixture(false, true);
        List<SqlExecutionObservation> events = new ArrayList<>();
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(fixture, RdbDialect.h2())
                .withObserver(events::add);

        assertThrows(RuntimeException.class, () -> executor.protectedWrite(
                work(), SqlExecutionOptions.safeDefaults()));

        fixture.assertStages(1, 1);
        assertErrorFacts(events);
    }

    private static void assertErrorFacts(List<SqlExecutionObservation> events) {
        assertEquals(1, events.size(), "one protected write must publish one SQL terminal event");
        assertEquals(SqlExecutionStatus.ERROR, events.getFirst().status());
        assertEquals(7L, events.getFirst().rows());
    }

    private static ProtectedWriteWork work() {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(id, value_col) values (?, ?)",
                        List.of(1L, "value")),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("payload", List.of(new byte[]{1}))));
    }

    private static final class JdbcFixture implements JdbcConnectionAccess {
        private final boolean failSideIndex;
        private final boolean failRelease;
        private final AtomicInteger businessExecutions = new AtomicInteger();
        private final AtomicInteger sideIndexExecutions = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private final java.sql.Connection connection = proxy(
                java.sql.Connection.class, this::invokeConnection);

        private JdbcFixture(boolean failSideIndex, boolean failRelease) {
            this.failSideIndex = failSideIndex;
            this.failRelease = failRelease;
        }

        @Override
        public java.sql.Connection getConnection(SqlRequest request) {
            return connection;
        }

        @Override
        public void releaseConnection(java.sql.Connection actual, SqlRequest request) throws SQLException {
            releases.incrementAndGet();
            if (failRelease) {
                throw new SQLException("release failed", "08006");
            }
        }

        private Object invokeConnection(Object self, java.lang.reflect.Method method, Object[] arguments) {
            if ("prepareStatement".equals(method.getName())) {
                return statement((String) arguments[0]);
            }
            throw new AssertionError(method.getName());
        }

        private PreparedStatement statement(String sql) {
            boolean sideIndex = sql.contains("token_index");
            return proxy(PreparedStatement.class, (self, method, arguments) -> switch (method.getName()) {
                case "setObject", "setBytes", "setBinaryStream", "addBatch", "close" -> null;
                case "executeLargeUpdate" -> {
                    if (sideIndex) throw new AssertionError("side-index insert must use the batch boundary");
                    businessExecutions.incrementAndGet();
                    yield 7L;
                }
                case "executeBatch" -> {
                    if (!sideIndex) throw new AssertionError("business write must not use the batch boundary");
                    sideIndexExecutions.incrementAndGet();
                    if (failSideIndex) throw new SQLException("side-index failed", "23000");
                    yield new int[]{1};
                }
                default -> throw new AssertionError(method.getName());
            });
        }

        private void assertStages(int expectedBusinessExecutions, int expectedSideIndexExecutions) {
            assertEquals(expectedBusinessExecutions, businessExecutions.get(),
                    "business DML must complete before the later failure");
            assertEquals(expectedSideIndexExecutions, sideIndexExecutions.get(),
                    "the side-index stage must be reached before release");
            assertEquals(1, releases.get(), "the connection must be released exactly once");
        }
    }

    private static final class R2dbcFixture implements R2dbcConnectionAccess {
        private final AtomicInteger businessExecutions = new AtomicInteger();
        private final AtomicInteger sideIndexExecutions = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private final R2dbcNonTransientResourceException sideIndexFailure =
                new R2dbcNonTransientResourceException("side-index failed", "23000", 0);
        private final Connection connection = proxy(Connection.class, (self, method, arguments) -> {
            if ("createStatement".equals(method.getName())) {
                return statement((String) arguments[0]);
            }
            throw new AssertionError(method.getName());
        });

        @Override
        public Publisher<? extends Connection> getConnection(SqlRequest request) {
            return Mono.just(connection);
        }

        @Override
        public Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                                                 Connection actual,
                                                 SqlRequest request) {
            releases.incrementAndGet();
            return Mono.empty();
        }

        private Statement statement(String sql) {
            boolean sideIndex = sql.contains("token_index");
            return proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "bind", "bindNull", "add", "fetchSize", "returnGeneratedValues" -> self;
                case "execute" -> {
                    if (sideIndex) {
                        sideIndexExecutions.incrementAndGet();
                        yield Flux.error(sideIndexFailure);
                    }
                    businessExecutions.incrementAndGet();
                    yield Flux.just(result(7L));
                }
                default -> throw new AssertionError(method.getName());
            });
        }

        private Result result(long rows) {
            return proxy(Result.class, (self, method, arguments) -> {
                if ("getRowsUpdated".equals(method.getName())) {
                    return Mono.just(rows);
                }
                throw new AssertionError(method.getName());
            });
        }

        private void assertStages() {
            assertEquals(1, businessExecutions.get(),
                    "business DML must complete before the side-index failure");
            assertEquals(1, sideIndexExecutions.get(),
                    "the side-index stage must fail after the business result exists");
            assertEquals(1, releases.get(), "the connection must be released exactly once");
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
