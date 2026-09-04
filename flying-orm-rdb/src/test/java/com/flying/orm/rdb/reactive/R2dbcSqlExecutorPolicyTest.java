package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcSqlExecutorPolicyTest {

    @Test
    void keepsBatchLimitsOffTheOrdinarySqlDecoratorChain() {
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactoryThatMustNotAcquire());

        assertInstanceOf(R2dbcSqlExecutor.class,
                executor.withBatchMemoryLimits(BatchMemoryLimits.defaults()));
    }

    @Test
    void rejectsAnOversizedBatchBeforeAcquiringAConnection() {
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactoryThatMustNotAcquire())
                .withBatchMemoryLimits(new BatchMemoryLimits(1, 1, 10, 1_024, 10));
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into events(id) values (?)",
                1,
                List.of(Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1L}),
                BatchWriteOptions.atomic(2));

        assertThrows(BatchMemoryLimitExceededException.class,
                () -> executor.writeBatch(request).block());
    }

    @Test
    void rejectsMultipleStatementsOnceAtTheReactiveDriverBoundary() {
        AtomicBoolean connectionRequested = new AtomicBoolean();
        AtomicBoolean statementCreated = new AtomicBoolean();
        AtomicBoolean connectionClosed = new AtomicBoolean();
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> {
                        statementCreated.set(true);
                        throw new AssertionError("unsafe SQL reached createStatement");
                    }
                    case "close" -> {
                        connectionClosed.set(true);
                        yield Mono.empty();
                    }
                    case "toString" -> "r2dbc-sql-boundary-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.defer(() -> {
                    connectionRequested.set(true);
                    return Mono.just(connection);
                });
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        Flux<?> execution = assertDoesNotThrow(() -> executor.query(
                new SqlRequest("select 1; delete from users", List.of())));

        assertFalse(connectionRequested.get());
        assertThrows(IllegalArgumentException.class, execution::blockLast);
        assertTrue(connectionRequested.get());
        assertFalse(statementCreated.get());
        assertTrue(connectionClosed.get());
    }

    private static ConnectionFactory connectionFactoryThatMustNotAcquire() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new AssertionError("connection must not be acquired while configuring the executor");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }
}
