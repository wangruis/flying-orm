package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBatchEvidenceBoundaryTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

    @Test
    void externalAtomicInputFailureAfterSuccessfulChunkRemainsPartialWithoutTransactionManagement() {
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        IllegalStateException inputFailure = new IllegalStateException("input unavailable");
        Connection external = connection(writes, commits, rollbacks, closes, Mono.empty());
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(external), RdbDialect.h2());
        Flux<Object[]> rows = Flux.<Object[]>just(new Object[]{1}).concatWith(Flux.error(inputFailure));

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request(rows, BatchWriteOptions.of(1))).block(WAIT));

        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(0, commits.get());
        assertEquals(0, rollbacks.get());
        assertEquals(0, closes.get());
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows, BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)", 1, List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL, rows, options, BatchRowCountPolicy.ANY);
    }

    private static ConnectionFactory factory(Publisher<? extends Connection> connections) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return connections;
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection connection(AtomicInteger writes) {
        return connection(writes, new AtomicInteger(), new AtomicInteger(), new AtomicInteger(), Mono.empty());
    }

    private static Connection connection(AtomicInteger writes,
                                         AtomicInteger commits,
                                         AtomicInteger rollbacks,
                                         AtomicInteger closes,
                                         Publisher<Void> commitResult) {
        Result result = proxy(Result.class, (self, method, arguments) -> switch (method.getName()) {
            case "getRowsUpdated" -> Mono.just(1L);
            default -> throw new AssertionError("unexpected result call: " + method.getName());
        });
        Statement statement = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull", "add" -> self;
            case "execute" -> {
                writes.incrementAndGet();
                yield Flux.just(result);
            }
            default -> throw new AssertionError("unexpected statement call: " + method.getName());
        });
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "isAutoCommit" -> true;
            case "setAutoCommit", "beginTransaction" -> Mono.empty();
            case "commitTransaction" -> Mono.defer(() -> {
                commits.incrementAndGet();
                return Mono.from(commitResult);
            });
            case "rollbackTransaction" -> Mono.fromRunnable(rollbacks::incrementAndGet);
            case "close" -> Mono.fromRunnable(closes::incrementAndGet);
            case "createStatement" -> statement;
            default -> throw new AssertionError("unexpected connection call: " + method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
