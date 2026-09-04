package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBatchEvidenceBoundaryTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

    @Test
    void connectionAcquisitionFailureRetainsAcceptedInputEvidence() {
        AtomicInteger cancellations = new AtomicInteger();
        List<BatchExecutionEvidence> observed = new ArrayList<>();
        IllegalStateException unavailable = new IllegalStateException("connection unavailable");
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory(Mono.error(unavailable)))
                .withBatchObserver(new BatchExecutionObserver() {
                    @Override
                    public void onExecution(BatchExecutionObservation observation) {
                    }

                    @Override
                    public void onExecutionEvidence(BatchExecutionEvidence evidence) {
                        observed.add(evidence);
                    }
                });
        Flux<Object[]> rows = Flux.<Object[]>just(new Object[]{1})
                .concatWith(Flux.never()).doOnCancel(cancellations::incrementAndGet);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> executor.writeBatchEvidence(request(rows, BatchWriteOptions.atomic(1))).block(WAIT));

        assertEquals(BatchExecutionState.FAILED, failure.evidence().state());
        assertEquals(BatchCommitFact.NOT_APPLICABLE, failure.evidence().commitFact());
        assertEquals(1L, failure.evidence().inputCount());
        assertEquals(1, cancellations.get());
        assertEquals(1, observed.size());
        assertSame(failure.evidence(), observed.getFirst());
    }

    @Test
    void evidenceRejectsReceiptRecoveryBeforeConsumingInputOrAcquiringConnection() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger acquisitions = new AtomicInteger();
        AtomicInteger writes = new AtomicInteger();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory(Mono.defer(() -> {
            acquisitions.incrementAndGet();
            return Mono.just(connection(writes));
        })));
        Flux<Object[]> rows = Flux.<Object[]>just(new Object[]{1})
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet());

        assertThrows(UnsupportedOperationException.class, () -> executor.writeBatchEvidence(
                request(rows, BatchWriteOptions.atomic(1).withReceipt("evidence-operation"))).block(WAIT));

        assertEquals(0, subscriptions.get());
        assertEquals(0, acquisitions.get());
        assertEquals(0, writes.get());
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
            case "setAutoCommit", "beginTransaction", "commitTransaction", "rollbackTransaction", "close" -> Mono.empty();
            case "createStatement" -> statement;
            default -> throw new AssertionError("unexpected connection call: " + method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
