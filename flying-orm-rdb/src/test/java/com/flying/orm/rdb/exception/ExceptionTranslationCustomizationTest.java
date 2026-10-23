package com.flying.orm.rdb.exception;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.R2dbcBadGrammarException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.sql.SQLException;
import java.sql.Connection;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ExceptionTranslationCustomizationTest {
    private final SqlRequest request = new SqlRequest("select id from items", List.of());

    @Test
    void firstChangedTranslationWinsAndDoesNotChangeExecutionFacts() {
        AtomicInteger unused = new AtomicInteger();
        RdbDialect original = RdbDialect.postgresql();
        RdbDialect configured = original.withExceptionTranslator(RdbExceptionTranslator.firstMatch(
                error -> error,
                error -> new IllegalStateException("application database error", error),
                error -> { unused.incrementAndGet(); return error; }));
        SQLException cause = new SQLException("bad SQL", "42000");
        RuntimeException translated = configured.withSchema(original.schema()).translateException(cause);
        assertInstanceOf(IllegalStateException.class, translated);
        assertSame(cause, translated.getCause().getCause());
        assertEquals(0, unused.get());
        assertInstanceOf(RdbException.class, original.translateException(cause));
        GeneratedKeyReadException result = new GeneratedKeyReadException(3, cause);
        assertSame(result, configured.translateException(result));
        assertEquals(3, result.affectedRows());
        IllegalArgumentException business = new IllegalArgumentException("business");
        assertSame(business, configured.translateException(business));
    }

    @Test
    void jdbcAndR2dbcUseTheConfiguredTranslation() {
        RdbDialect dialect = RdbDialect.postgresql().withExceptionTranslator(
                error -> new IllegalStateException(error.kind().name(), error));
        SQLException jdbcFailure = new SQLException("bad SQL", "42000");
        JdbcConnectionAccess access = JdbcConnectionAccess.of(
                ignored -> { throw jdbcFailure; }, (connection, ignored) -> fail("no acquired connection"));
        JdbcSqlExecutor jdbc = JdbcSqlExecutor.create(access, dialect);
        assertEquals("BAD_SQL", assertThrows(IllegalStateException.class, () -> jdbc.query(request)).getMessage());
        assertEquals("BAD_SQL", assertThrows(IllegalStateException.class, () -> jdbc.rowsUpdated(request)).getMessage());

        R2dbcBadGrammarException reactiveFailure = new R2dbcBadGrammarException("bad SQL", "42000", 0);
        R2dbcSqlExecutor reactive = R2dbcSqlExecutor.create(R2dbcConnectionAccess.of(
                ignored -> Mono.error(reactiveFailure),
                (signal, connection, ignored) -> Mono.error(new AssertionError("no acquired connection"))), dialect);
        assertEquals("BAD_SQL", assertThrows(IllegalStateException.class,
                () -> reactive.query(request).collectList().block()).getMessage());
        assertSame(reactiveFailure, assertThrows(IllegalStateException.class,
                () -> reactive.rowsUpdated(request).block()).getCause().getCause());
        BatchWriteRequest batch = BatchWriteRequests.request("insert into items(id) values (?)", 1, List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL, Flux.<Object[]>just(new Object[]{1}), BatchWriteOptions.of(1));
        BatchExecutionEvidenceException jdbcBatch = assertThrows(BatchExecutionEvidenceException.class,
                () -> JdbcBatchWriter.create(access, dialect).writeBatch(batch));
        BatchExecutionEvidenceException reactiveBatch = assertThrows(BatchExecutionEvidenceException.class,
                () -> reactive.writeBatch(batch, null).block());
        for (BatchExecutionEvidenceException failure : List.of(jdbcBatch, reactiveBatch)) {
            assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertEquals(BatchExecutionState.FAILED, failure.evidence().state());
            assertEquals(RdbErrorKind.BAD_SQL, failure.evidence().failure().kind());
            assertEquals(0, failure.evidence().successfulCount());
        }
    }

    @Test
    void cleanupFailureIsMappedAfterTheSameConnectionWorkFinishes() {
        AtomicInteger released = new AtomicInteger();
        Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    throw new AssertionError("ORM must not own the connection: " + method.getName());
                });
        SQLException failure = new SQLException("release", "08006");
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(JdbcConnectionAccess.of(
                ignored -> connection, (acquired, ignored) -> { released.incrementAndGet(); throw failure; }),
                RdbDialect.postgresql().withExceptionTranslator(
                        error -> new IllegalStateException("mapped release", error)));
        assertEquals("mapped release", assertThrows(IllegalStateException.class,
                () -> executor.withConnection(request, scoped -> 1)).getMessage());
        assertEquals(1, released.get());
    }

    @Test
    void sameConnectionReactiveWorkTranslatesOnceAtItsOuterBoundary() {
        AtomicInteger translations = new AtomicInteger();
        R2dbcBadGrammarException failure = new R2dbcBadGrammarException("bad SQL", "42000", 0);
        io.r2dbc.spi.Connection connection = (io.r2dbc.spi.Connection) Proxy.newProxyInstance(
                io.r2dbc.spi.Connection.class.getClassLoader(), new Class<?>[]{io.r2dbc.spi.Connection.class},
                (proxy, method, args) -> { throw failure; });
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(R2dbcConnectionAccess.of(
                ignored -> Mono.just(connection), (signal, acquired, ignored) -> Mono.empty()),
                RdbDialect.postgresql().withExceptionTranslator(error -> {
                    translations.incrementAndGet();
                    return new RdbException(error.kind(), "application", error.sqlState(), error.errorCode(), error);
                }));
        RdbException error = assertThrows(RdbException.class,
                () -> executor.withConnection(request, scoped -> scoped.rowsUpdated(request)).block());
        assertEquals("application", error.getMessage());
        assertEquals(1, translations.get());
        assertSame(failure, error.getCause().getCause());
    }
}
