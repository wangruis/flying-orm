package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
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
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBatchTransactionResolutionTest {

    @Test
    void keepsResolvedTransactionAbsenceForWholeMonoBatchSubscription() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger ownedAcquisitions = new AtomicInteger();
        AtomicInteger ownedExecutions = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        Connection externalConnection = successfulConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        connectionFactory(ownedAcquisitions, successfulConnection(ownedExecutions)))
                .withTransactionParticipant(() -> transactionLookups.incrementAndGet() == 1
                        ? Mono.empty()
                        : Mono.just(R2dbcTransactionContext.external(externalConnection)));

        executor.writeBatch(request(BatchWriteOptions.atomic(1))).block(Duration.ofSeconds(2));

        assertEquals(1, transactionLookups.get());
        assertEquals(1, ownedAcquisitions.get());
        assertEquals(1, ownedExecutions.get());
        assertEquals(0, externalExecutions.get());
    }

    @Test
    void keepsResolvedTransactionAbsenceForWholeChunkFluxSubscription() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger ownedAcquisitions = new AtomicInteger();
        AtomicInteger ownedExecutions = new AtomicInteger();
        AtomicInteger externalExecutions = new AtomicInteger();
        Connection externalConnection = successfulConnection(externalExecutions);
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        connectionFactory(ownedAcquisitions, successfulConnection(ownedExecutions)))
                .withTransactionParticipant(() -> transactionLookups.incrementAndGet() == 1
                        ? Mono.empty()
                        : Mono.just(R2dbcTransactionContext.external(externalConnection)));

        List<BatchChunkResult> chunks = executor.writeBatchChunks(
                request(BatchWriteOptions.independent(1, 1))).collectList().block(Duration.ofSeconds(2));

        assertEquals(1, chunks.size());
        assertEquals(1, transactionLookups.get());
        assertEquals(1, ownedAcquisitions.get());
        assertEquals(1, ownedExecutions.get());
        assertEquals(0, externalExecutions.get());
    }

    @Test
    void observesTransactionResolutionFailureForMonoAndChunkBatchCalls() {
        AtomicInteger transactionLookups = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("transaction lookup failed");
        List<SqlExecutionObservation> sqlEvents = new ArrayList<>();
        List<BatchExecutionObservation> batchEvents = new ArrayList<>();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(
                        connectionFactory(new AtomicInteger(), successfulConnection(new AtomicInteger())))
                .withTransactionParticipant(() -> {
                    transactionLookups.incrementAndGet();
                    return Mono.error(failure);
                })
                .withObservers(sqlEvents::add, batchEvents::add);

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> executor.writeBatch(request(BatchWriteOptions.atomic(1)))
                        .block(Duration.ofSeconds(2))));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> executor.writeBatchChunks(request(BatchWriteOptions.independent(1, 1)))
                        .collectList()
                        .block(Duration.ofSeconds(2))));

        assertEquals(2, transactionLookups.get());
        assertEquals(2, sqlEvents.size());
        sqlEvents.forEach(event -> assertEquals(SqlExecutionStatus.ERROR, event.status()));
        assertEquals(2, batchEvents.size());
        batchEvents.forEach(event -> assertNotNull(((BatchExecutionObservation.Summary) event).failure()));
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update sample set value_col = ?",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                options,
                BatchRowCountPolicy.EXACTLY_ONE);
    }

    private static ConnectionFactory connectionFactory(AtomicInteger acquisitions, Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                acquisitions.incrementAndGet();
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection successfulConnection(AtomicInteger executions) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "commitTransaction", "rollbackTransaction",
                         "setAutoCommit", "close" -> Mono.empty();
                    case "createStatement" -> successfulStatement(executions);
                    case "toString" -> "batch-transaction-resolution-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement successfulStatement(AtomicInteger executions) {
        return (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> proxy;
                    case "execute" -> {
                        executions.incrementAndGet();
                        yield Flux.just(successfulResult());
                    }
                    case "toString" -> "batch-transaction-resolution-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result successfulResult() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(1L);
                    case "toString" -> "batch-transaction-resolution-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
