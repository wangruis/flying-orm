package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.core.sql.render.SqlRequest;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchEvidenceNoCyclicWaitTest {

    @Test
    void forwardingEvidenceReturnsBeforeAndWithoutRegisteringExternalCompletion() {
        AtomicInteger completionRegistrations = new AtomicInteger();
        Connection connection = successfulConnection();
        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(
                connection,
                listener -> {
                    completionRegistrations.incrementAndGet();
                    return true;
                });
        ReactiveSqlExecutor delegate = R2dbcSqlExecutor.create(unusedConnectionFactory())
                .withTransactionParticipant(() -> Mono.just(transaction));
        ReactiveSqlExecutor forwarding = new ForwardingReactiveSqlExecutor(delegate) {
        };

        BatchExecutionEvidence evidence = forwarding.writeBatchEvidence(request())
                .block(Duration.ofSeconds(2));

        assertEquals(BatchCommitFact.PENDING_EXTERNAL, evidence.commitFact());
        assertEquals(0, completionRegistrations.get());
    }

    @Test
    void observedForwarderPublishesSuccessfulEvidence() {
        AtomicInteger observations = new AtomicInteger();
        EvidenceExecutor delegate = new EvidenceExecutor();
        BatchExecutionObserver observer = new BatchExecutionObserver() {
            @Override
            public void onExecution(BatchExecutionObservation observation) {
                // 这个测试只核对 evidence 默认回调。
            }

            @Override
            public void onExecutionEvidence(BatchExecutionEvidence evidence) {
                observations.incrementAndGet();
            }
        };

        delegate.withBatchObserver(observer).writeBatchEvidence(request()).block(Duration.ofSeconds(2));

        assertEquals(1, observations.get());
    }

    @Test
    void memoryLimitForwarderChecksEvidenceBeforeCallingItsDelegate() {
        EvidenceExecutor delegate = new EvidenceExecutor();
        ReactiveSqlExecutor limited = delegate.withBatchMemoryLimits(
                new BatchMemoryLimits(1, 32, 10, 1L, 100_000));

        assertThrows(BatchMemoryLimitExceededException.class,
                () -> limited.writeBatchEvidence(request()).block(Duration.ofSeconds(2)));
        assertEquals(0, delegate.evidenceCalls.get());
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{1}),
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.ANY);
    }

    private static ConnectionFactory unusedConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                throw new AssertionError("external transaction must not acquire another connection");
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static Connection successfulConnection() {
        Result result = proxy(Result.class, (self, method, arguments) -> switch (method.getName()) {
            case "getRowsUpdated" -> Mono.just(1L);
            default -> throw new AssertionError("unexpected result call: " + method.getName());
        });
        Statement statement = proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
            case "bind", "bindNull", "add" -> self;
            case "execute" -> Flux.just(result);
            default -> throw new AssertionError("unexpected statement call: " + method.getName());
        });
        return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
            case "createStatement" -> statement;
            default -> throw new AssertionError("external connection must not be managed: " + method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static final class EvidenceExecutor implements ReactiveSqlExecutor {

        private final AtomicInteger evidenceCalls = new AtomicInteger();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            return Mono.just(0L);
        }

        @Override
        public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
            evidenceCalls.incrementAndGet();
            return Mono.just(BatchExecutionEvidence.of(
                    BatchWriteOptions.Mode.ATOMIC,
                    BatchExecutionState.SUCCESS,
                    BatchCommitFact.NOT_APPLICABLE,
                    List.of()));
        }
    }
}
