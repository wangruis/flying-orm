package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcAtomicReceiptReplayInputCountTest {

    @Test
    void reportsAcceptedRowsWhenReceiptReplayInputFailsBeforeTheFirstChunkExists() {
        assertReplayInputFailure(new IllegalArgumentException("receipt replay input failed"));
    }

    @Test
    void reportsAcceptedRowsWhenReceiptReplayInputTimesOutBeforeTheFirstChunkExists() {
        assertReplayInputFailure(new TimeoutException("receipt replay input timed out"));
    }

    private static void assertReplayInputFailure(Throwable inputFailure) {
        Object[] row = new Object[]{"name-0"};
        BatchWriteRequest request = request(Flux.concat(
                Flux.<Object[]>just(row), Flux.error(inputFailure)));
        BatchPayloadHasher hasher = new BatchPayloadHasher();
        String planHash = hasher.hashPlan(request);
        String payloadHash = hasher.hashRows(List.<Object[]>of(row));
        AtomicInteger receiptQueries = new AtomicInteger();
        AtomicInteger businessStatements = new AtomicInteger();
        ConnectionFactory connectionFactory = receiptConnectionFactory(
                planHash, payloadHash, receiptQueries, businessStatements);

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(connectionFactory)
                        .writeBatch(request)
                        .block(Duration.ofSeconds(2)));

        assertSame(inputFailure, error.getCause());
        assertEquals(1, error.result().inputCount());
        assertEquals(1, error.result().chunks().size());
        assertEquals(1, error.result().chunks().getFirst().inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, error.result().chunks().getFirst().status());
        assertEquals(1, receiptQueries.get());
        assertEquals(0, businessStatements.get());
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.atomic(2).withReceipt("receipt-replay-operation"));
    }

    private static ConnectionFactory receiptConnectionFactory(String planHash,
                                                               String payloadHash,
                                                               AtomicInteger receiptQueries,
                                                               AtomicInteger businessStatements) {
        Connection connection = receiptConnection(
                planHash, payloadHash, receiptQueries, businessStatements);
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    private static Connection receiptConnection(String planHash,
                                                String payloadHash,
                                                AtomicInteger receiptQueries,
                                                AtomicInteger businessStatements) {
        Row row = (Row) Proxy.newProxyInstance(
                Row.class.getClassLoader(),
                new Class<?>[]{Row.class},
                (proxy, method, arguments) -> {
                    if ("get".equals(method.getName())) {
                        int index = (Integer) arguments[0];
                        return switch (index) {
                            case 0 -> planHash;
                            case 1 -> payloadHash;
                            case 2, 3 -> 1L;
                            default -> null;
                        };
                    }
                    return defaultValue(method.getReturnType());
                });
        Result result = (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> {
                    if ("map".equals(method.getName())) {
                        @SuppressWarnings("unchecked")
                        BiFunction<Row, RowMetadata, Object> mapper =
                                (BiFunction<Row, RowMetadata, Object>) arguments[0];
                        return Flux.just(mapper.apply(row, null));
                    }
                    return defaultValue(method.getReturnType());
                });
        Statement[] statement = new Statement[1];
        statement[0] = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull" -> statement[0];
                    case "execute" -> Flux.just(result);
                    default -> defaultValue(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> {
                        String sql = (String) arguments[0];
                        if (sql.startsWith("select plan_hash, payload_hash")) {
                            receiptQueries.incrementAndGet();
                        } else {
                            businessStatements.incrementAndGet();
                        }
                        yield statement[0];
                    }
                    case "close" -> Mono.empty();
                    default -> defaultValue(method.getReturnType());
                });
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
