package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class R2dbcObservationParameterSnapshotTest {

    @Test
    void observesTheSameFrozenParametersThatAreBoundToTheDatabase() {
        byte[] source = {1, 2, 3};
        AtomicReference<byte[]> bound = new AtomicReference<>();
        AtomicReference<byte[]> observed = new AtomicReference<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(connectionFactory(bound))
                .withObserver(observer(observed));

        Mono<Long> execution = executor.rowsUpdated(
                new SqlRequest("update sample set payload = ?", List.of(source)));
        source[0] = 9;
        execution.block(Duration.ofSeconds(2));

        assertArrayEquals(new byte[]{1, 2, 3}, bound.get());
        assertArrayEquals(new byte[]{1, 2, 3}, observed.get());
    }

    private static SqlExecutionObserver observer(AtomicReference<byte[]> observed) {
        return new SqlExecutionObserver() {
            @Override
            public boolean requiresParameterValues() {
                return true;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // Parameter-aware callback is used below.
            }

            @Override
            public void onExecution(SqlExecutionObservation observation, List<Object> parameters) {
                observed.set(((byte[]) parameters.getFirst()).clone());
            }
        };
    }

    private static ConnectionFactory connectionFactory(AtomicReference<byte[]> bound) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection(bound));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection connection(AtomicReference<byte[]> bound) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "createStatement" -> statement(bound);
                    case "close" -> Mono.empty();
                    case "toString" -> "observation-parameter-snapshot-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement statement(AtomicReference<byte[]> bound) {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "bind" -> {
                        bound.set(((byte[]) args[1]).clone());
                        yield proxy;
                    }
                    case "execute" -> Flux.just(result());
                    case "toString" -> "observation-parameter-snapshot-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result result() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(1L);
                    case "toString" -> "observation-parameter-snapshot-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
