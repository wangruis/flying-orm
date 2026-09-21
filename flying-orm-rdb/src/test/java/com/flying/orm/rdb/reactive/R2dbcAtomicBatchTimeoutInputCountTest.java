package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcAtomicBatchTimeoutInputCountTest {

    @Test
    void classifiesInputFailureBeforeTheFirstChunkWithoutAcquiringAConnection() {
        assertInputFailureState(new TimeoutException("input timed out"), BatchExecutionState.TIMED_OUT);
        assertInputFailureState(new CancellationException("input cancelled"), BatchExecutionState.CANCELLED);
    }

    @Test
    void reportsAcceptedRowsWhenInputFailsBeforeTheFirstChunkExists() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("input failed");
        AtomicInteger connectionRequests = new AtomicInteger();
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                connectionRequests.incrementAndGet();
                return Mono.error(new AssertionError("input failure must not acquire a connection"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        BatchWriteRequest request = request(Flux.concat(
                Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(inputFailure)), 2);

        BatchExecutionEvidenceException error = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(connectionFactory), RdbDialect.postgresql()).writeBatch(request).block(Duration.ofSeconds(2)));

        assertEquals(0, connectionRequests.get());
        assertEquals(1, error.evidence().inputCount());
        assertSame(inputFailure, error.getCause());
        assertEquals(0, error.evidence().successfulCount());
    }

    @Test
    void reportsActiveChunkWhenExternalTransactionExecutionFails() {
        BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{"name-0"}));
        Connection connection = connectionThatFailsBusinessWrite();

        BatchExecutionEvidenceException error = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> externalExecutor(connection).writeBatch(request).block(Duration.ofSeconds(2)));

        assertEquals(1, error.evidence().inputCount());
    }

    @Test
    void protectedSideIndexFailureKeepsMainSqlEvidence() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into batch_people(name_col) values (?)", List.of("name-0")),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from batch_people_tokens where id = ? and field_tag = ?",
                "insert into batch_people_tokens(id, field_tag, token_value) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("name_col", List.of(new byte[]{1}))));
        BatchWriteRequest request = request(Flux.<Object[]>just(
                ProtectedBatchRows.extend(new Object[]{"name-0"}, work)));

        BatchExecutionEvidenceException error = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> externalExecutor(connectionThatFailsSideIndexWrite())
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertEquals(1, error.evidence().inputCount());
        assertEquals(1, error.evidence().successfulCount());
    }

    private static void assertInputFailureState(Throwable failure, BatchExecutionState expectedState) {
        AtomicInteger connectionRequests = new AtomicInteger();
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                connectionRequests.incrementAndGet();
                return Mono.error(new AssertionError("input failure must not acquire a connection"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };

        BatchExecutionEvidenceException error = assertThrows(
                BatchExecutionEvidenceException.class,
                () -> R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(connectionFactory), RdbDialect.postgresql())
                        .writeBatchEvidence(request(Flux.error(failure), 2))
                        .block(Duration.ofSeconds(2)));

        assertEquals(0, connectionRequests.get());
        assertEquals(expectedState, error.evidence().state());
        assertEquals(0L, error.evidence().inputCount());
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows) {
        return request(rows, 1);
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows, int chunkSize) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.of(chunkSize));
    }

    private static R2dbcSqlExecutor externalExecutor(Connection connection) {
        return R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), RdbDialect.postgresql());
    }

    private static ConnectionFactory unusedConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new AssertionError("external transaction must bypass the connection factory"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    private static Connection connectionThatFailsBusinessWrite() {
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> throw new IllegalStateException("business write failed");
            case "toString" -> "failing-business-connection";
            default -> throw new AssertionError("external transaction must not be managed: " + method.getName());
        });
    }

    private static Connection connectionThatFailsSideIndexWrite() {
        AtomicInteger statements = new AtomicInteger();
        Statement[] statement = new Statement[1];
        statement[0] = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull", "add" -> statement[0];
            case "execute" -> statements.get() == 1
                    ? Flux.just(rowsUpdated(1L))
                    : Flux.error(new IllegalStateException("side index write failed"));
            case "toString" -> "protected-batch-statement";
            default -> throw new UnsupportedOperationException(method.getName());
        });
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> {
                statements.incrementAndGet();
                yield statement[0];
            }
            case "toString" -> "protected-batch-connection";
            default -> throw new AssertionError("external transaction must not be managed: " + method.getName());
        });
    }

    private static Result rowsUpdated(long value) {
        return proxy(Result.class, (self, method, arguments) -> switch (method.getName()) {
            case "getRowsUpdated" -> Mono.just(value);
            case "toString" -> "rows-updated-result";
            default -> throw new UnsupportedOperationException(method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
