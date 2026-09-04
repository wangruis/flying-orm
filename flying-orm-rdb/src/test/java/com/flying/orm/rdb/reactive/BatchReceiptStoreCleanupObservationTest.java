package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchReceiptStoreCleanupObservationTest {

    @Test
    void doesNotReportACleanupFailureWhenTheReceiptConnectionClosesNormally() {
        IllegalStateException queryFailure = new IllegalStateException("receipt read failed");
        AtomicReference<ResourceCleanupObservation> cleanup = new AtomicReference<>();
        ConnectionFactory factory = connectionFactory(queryFailure);
        BatchReceiptStore store = new BatchReceiptStore(
                factory,
                R2dbcBindMarkers.from(factory),
                observer(cleanup));
        BatchWriteOptions.Recovery recovery = new BatchWriteOptions.Recovery(
                BatchWriteOptions.RecoveryMode.RECEIPT,
                "operation",
                BatchWriteOptions.Recovery.DEFAULT_RECEIPT_TABLE,
                Duration.ZERO);

        IllegalStateException actual = assertThrows(
                IllegalStateException.class,
                () -> store.findOperation(recovery, 0).block());

        assertSame(queryFailure, actual);
        assertNull(cleanup.get());
    }

    private static SqlExecutionObserver observer(AtomicReference<ResourceCleanupObservation> cleanup) {
        return new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // This test only observes resource cleanup facts.
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanup.set(observation);
            }
        };
    }

    private static ConnectionFactory connectionFactory(RuntimeException queryFailure) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add", "returnGeneratedValues", "fetchSize" -> proxy;
                    case "execute" -> Flux.error(queryFailure);
                    default -> defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "close" -> Mono.empty();
                    case "isAutoCommit" -> true;
                    default -> defaultValue(method.getReturnType());
                });
        return new ConnectionFactory() {
            @Override
            public Mono<Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }


    private static Object defaultValue(Class<?> type) {
        if (type == boolean.class) {
            return false;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        return null;
    }
}
