package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBatchSqlBoundaryTest {

    @Test
    void keepsMainBatchTransportCompilationOutOfChunkState() {
        assertFalse(Arrays.stream(R2dbcBatchWriterChunks.class.getDeclaredFields())
                          .anyMatch(field -> field.getType() == R2dbcBindMarkers.class));
    }

    @Test
    void rejectsMarkerMismatchBeforeOpeningAConnectionOrSubscribingRows() {
        AtomicBoolean connectionRequested = new AtomicBoolean();
        AtomicBoolean rowsSubscribed = new AtomicBoolean();
        ConnectionFactory connectionFactory = sqlServerFactory(connectionRequested);
        Publisher<Object[]> rows = Flux.defer(() -> {
            rowsSubscribed.set(true);
            return Flux.<Object[]>just(new Object[]{1});
        });
        BatchWriteRequest request = new BatchWriteRequest(
                SqlStatementPlan.canonical(
                        "update alpha set first_col = ?, second_col = ?",
                        SqlBindMarkerStyle.CANONICAL,
                        1),
                List.of(Integer.class),
                rows,
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.none(),
                BatchWriteCompletion.noop());

        Mono<?> execution = R2dbcSqlExecutor.create(connectionFactory).writeBatch(request);

        assertThrows(IllegalArgumentException.class, execution::block);
        assertFalse(connectionRequested.get());
        assertFalse(rowsSubscribed.get());
    }

    @Test
    void rejectsSqlServerStatementsBeforeOpeningAConnectionOrSubscribingRows() {
        AtomicBoolean connectionRequested = new AtomicBoolean();
        AtomicBoolean rowsSubscribed = new AtomicBoolean();
        ConnectionFactory connectionFactory = sqlServerFactory(connectionRequested);
        Publisher<Object[]> rows = Flux.defer(() -> {
            rowsSubscribed.set(true);
            return Flux.<Object[]>just(new Object[]{1, 2});
        });
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update alpha set value_col=? update beta set value_col=?",
                2,
                List.of(Integer.class, Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.atomic(1));

        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory);
        Mono<?> execution = executor.writeBatch(request);

        assertFalse(connectionRequested.get());
        assertFalse(rowsSubscribed.get());
        assertThrows(IllegalArgumentException.class, execution::block);
        assertFalse(connectionRequested.get());
        assertFalse(rowsSubscribed.get());
    }

    @Test
    void usesTheAdaptedTransportSqlForEveryPerRowExecution() {
        List<String> statementSql = new ArrayList<>();
        Connection connection = successfulConnection(statementSql);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update sample set value_col = ?",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{1}, new Object[]{2}),
                BatchWriteOptions.atomic(2),
                BatchRowCountPolicy.EXACTLY_ONE);

        R2dbcSqlExecutor.create(connectionFactory(connection)).writeBatch(request)
                .block(Duration.ofSeconds(2));

        assertEquals(List.of(
                "update sample set value_col = @P0",
                "update sample set value_col = @P0"), statementSql);
    }

    private static ConnectionFactory sqlServerFactory(AtomicBoolean connectionRequested) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                connectionRequested.set(true);
                return Mono.error(new AssertionError("connection opened before batch SQL validation"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "Microsoft SQL Server";
            }
        };
    }

    private static ConnectionFactory connectionFactory(Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "Microsoft SQL Server";
            }
        };
    }

    private static Connection successfulConnection(List<String> statementSql) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "commitTransaction", "rollbackTransaction",
                         "setAutoCommit", "close" -> Mono.empty();
                    case "createStatement" -> {
                        statementSql.add((String) arguments[0]);
                        yield successfulStatement();
                    }
                    case "toString" -> "batch-transport-plan-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement successfulStatement() {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> proxy;
                    case "execute" -> Flux.just(successfulResult());
                    case "toString" -> "batch-transport-plan-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result successfulResult() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(1L);
                    case "toString" -> "batch-transport-plan-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
