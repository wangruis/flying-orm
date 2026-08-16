package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchReceiptIntegrityException;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证批量恢复回执的数值边界。回执决定 UNKNOWN 分片能否被认定为已提交，
 * 因此超范围或带小数的计数必须明确失败，不能被 {@link Number#longValue()} 悄悄截断。
 */
class BatchReceiptStoreTest {

    @Test
    void rejectsReceiptCountsThatCannotBeRepresentedExactlyAsLong() {
        assertEquals(9_007_199_254_740_993L,
                     BatchReceiptStore.exactLong(row(new BigInteger("9007199254740993"), 1L),
                                                 0,
                                                 "row_count"));
        assertInvalidCount(new BigInteger("9223372036854775808"));
        assertInvalidCount(new BigDecimal("1.5"));
        assertInvalidCount(-1L);
    }

    /** reserve 和 complete 不是普通 DML，只有恰好一行才能建立可恢复的提交事实。 */
    @Test
    void requiresExactlyOneAffectedReceiptRow() {
        assertEquals(1L, BatchReceiptStore.requireExactlyOne("reserve", 1L));
        assertThrows(BatchReceiptIntegrityException.class,
                     () -> BatchReceiptStore.requireExactlyOne("reserve", 0L));
        assertThrows(BatchReceiptIntegrityException.class,
                     () -> BatchReceiptStore.requireExactlyOne("complete", 2L));
    }

    /** 回执查询允许不存在，但绝不允许把重复事实静默截断为第一条。 */
    @Test
    void rejectsDuplicateReceiptRows() {
        StepVerifier.create(BatchReceiptStore.zeroOrOne(Flux.just("first", "duplicate"), "find"))
                    .expectError(BatchReceiptIntegrityException.class)
                    .verify();
        StepVerifier.create(BatchReceiptStore.zeroOrOne(Flux.just("only"), "find"))
                    .expectNext("only")
                    .verifyComplete();
        StepVerifier.create(BatchReceiptStore.zeroOrOne(Flux.empty(), "find"))
                    .verifyComplete();
    }

    @Test
    void requiresReceiptCountsToMatchTheCompleteRecoveryEvidence() {
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken(
                "operation-1", 0, "batch_receipt", "plan", "payload", 2L, 2L);

        assertEquals(new BatchReceiptStore.Receipt("payload", 2L, 2L),
                     BatchReceiptStore.requireMatching(token,
                                                       new BatchReceiptStore.Receipt("payload", 2L, 2L)));
        assertThrows(BatchReceiptIntegrityException.class,
                     () -> BatchReceiptStore.requireMatching(
                             token, new BatchReceiptStore.Receipt("payload", 3L, 2L)));
        assertThrows(BatchReceiptIntegrityException.class,
                     () -> BatchReceiptStore.requireMatching(
                             token, new BatchReceiptStore.Receipt("payload", 2L, 1L)));
    }

    @Test
    void allowsAffectedRowsToRemainPolicyOptionalButNeverNegative() {
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken(
                "operation-1", 0, "batch_receipt", "plan", "payload", 2L, null);

        assertEquals(1L,
                     BatchReceiptStore.requireMatching(
                             token, new BatchReceiptStore.Receipt("payload", 2L, 1L)).affectedRows());
        assertThrows(BatchReceiptIntegrityException.class,
                     () -> BatchReceiptStore.requireMatching(
                             token, new BatchReceiptStore.Receipt("payload", 2L, -1L)));
    }

    @Test
    void rejectsReplayRowCountOutsideInMemoryChunkCapacityWithIntegrityError() {
        BatchReceiptStore.Receipt receipt = new BatchReceiptStore.Receipt(
                "payload", (long) Integer.MAX_VALUE + 1L, 1L);

        assertThrows(BatchReceiptIntegrityException.class, receipt::exactInputRowCount);
    }

    /** 即使配置回执 SQL 时限，连接等待和排队也完全交给连接池。 */
    @Test
    void delegatesReceiptConnectionWaitingToThePool() {
        BatchReceiptStore store = store(neverConnectionFactory(), new AtomicInteger(), new AtomicInteger());

        StepVerifier.withVirtualTime(() -> store.find(completeToken(), Duration.ofMillis(10)))
                    .thenAwait(Duration.ofSeconds(6))
                    .thenCancel()
                    .verify();
    }

    /** 回执读取在驱动错误或取消时结果未确认，连接只能失效，不能按正常连接归池。 */
    @Test
    void invalidatesUnconfirmedReceiptReadConnections() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        BatchReceiptStore failing = store(connectionFactory(Flux.error(new IllegalStateException("failed"))),
                                          closes,
                                          invalidations);

        StepVerifier.create(failing.find(completeToken())).expectError().verify();
        assertEquals(0, closes.get());
        assertEquals(1, invalidations.get());

        closes.set(0);
        invalidations.set(0);
        BatchReceiptStore cancelled = store(connectionFactory(Flux.never()), closes, invalidations);
        StepVerifier.create(cancelled.find(completeToken()))
                    .thenAwait(Duration.ofMillis(1))
                    .thenCancel()
                    .verify();
        assertEquals(0, closes.get());
        assertEquals(1, invalidations.get());
    }

    /** 已确认回执读取的 close 失败后，invalidate 的 fatal 不能被吞成已读取成功。 */
    @Test
    void propagatesCleanupFatalAfterConfirmedReceiptReadCloseFailure() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError fatal = new OutOfMemoryError("receipt invalidation fatal");
        BatchReceiptStore store = store(connectionFactory(Flux.just(receiptResult())),
                                        closes,
                                        invalidations,
                                        cleanupObserver(cleanupObservations),
                                        new IllegalStateException("receipt connection close failed"),
                                        fatal);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> store.find(completeToken()).block());

        assertSame(fatal, observed);
        assertEquals(1, closes.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
    }

    /** 已确认回执读取的 close 自身发出 fatal 时，成功 invalidate 也不能把它降级成读取成功。 */
    @Test
    void propagatesCloseFatalAfterConfirmedReceiptReadWhenInvalidationSucceeds() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError fatal = new OutOfMemoryError("receipt close fatal");
        BatchReceiptStore store = store(connectionFactory(Flux.just(receiptResult())),
                                        closes,
                                        invalidations,
                                        cleanupObserver(cleanupObservations),
                                        fatal,
                                        null);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> store.find(completeToken()).block());

        assertSame(fatal, observed);
        assertEquals(1, closes.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
    }

    /** 回执读取后的 close 与 invalidate 共享一次清理预算，不能把五秒边界串成十秒。 */
    @Test
    void sharesOneCleanupDeadlineAcrossConfirmedReadCloseAndInvalidation() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory factory = connectionFactory(Flux.just(receiptResult()));
        BatchReceiptStore store = new BatchReceiptStore(
                factory,
                R2dbcBindMarkers.from(factory),
                SqlExecutionObserver.noop(),
                R2dbcConnectionInvalidator.of(
                        ignored -> Mono.<Void>never().doOnSubscribe(subscription -> closes.incrementAndGet()),
                        ignored -> Mono.<Void>never().doOnSubscribe(subscription -> invalidations.incrementAndGet())));

        StepVerifier.withVirtualTime(() -> store.find(completeToken()))
                    .thenAwait(SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT.plusMillis(2))
                    .expectNext(new BatchReceiptStore.Receipt("", 1L, 1L))
                    .expectComplete()
                    .verify(Duration.ofSeconds(1));

        assertEquals(1, closes.get());
        assertEquals(1, invalidations.get());
    }

    /** usingWhen 在资源域内保留 cleanup wrapper；其 cause 仍必须是同一 fatal，供公共入口统一恢复。 */
    @Test
    void propagatesCleanupFatalAfterUnconfirmedReceiptRead() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        OutOfMemoryError fatal = new OutOfMemoryError("receipt invalidation fatal");
        BatchReceiptStore store = store(connectionFactory(Flux.error(new IllegalStateException("receipt failed"))),
                                        closes,
                                        invalidations,
                                        cleanupObserver(cleanupObservations),
                                        null,
                                        fatal);

        RuntimeException wrapper = assertThrows(RuntimeException.class,
                                                 () -> store.find(completeToken()).block());

        assertSame(fatal, wrapper.getCause());
        assertSame(fatal, ReactiveSqlExecutionProtection.translate(wrapper));
        assertEquals(0, closes.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                     cleanupObservations.getFirst().phase());
    }

    private static void assertInvalidCount(Number count) {
        BatchReceiptIntegrityException error = assertThrows(
                BatchReceiptIntegrityException.class,
                () -> BatchReceiptStore.exactLong(row(count, 1L), 0, "row_count"));
        if (!(count instanceof Long value && value < 0L)) {
            assertInstanceOf(IllegalArgumentException.class, error.getCause());
        }
    }

    private static Row row(Object first, Object second) {
        return (Row) Proxy.newProxyInstance(
                BatchReceiptStoreTest.class.getClassLoader(),
                new Class<?>[]{Row.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("get") && args[0] instanceof Integer index) {
                        return index == 0 ? first : second;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static BatchChunkResult.RecoveryToken completeToken() {
        return new BatchChunkResult.RecoveryToken(
                "operation-1", 0, "batch_receipt", "plan", "payload", 1L, 1L);
    }

    private static BatchReceiptStore store(ConnectionFactory factory,
                                           AtomicInteger closes,
                                           AtomicInteger invalidations) {
        return store(factory, closes, invalidations, SqlExecutionObserver.noop(), null, null);
    }

    private static BatchReceiptStore store(ConnectionFactory factory,
                                           AtomicInteger closes,
                                           AtomicInteger invalidations,
                                           SqlExecutionObserver observer,
                                           Throwable closeFailure,
                                           Throwable invalidationFailure) {
        return new BatchReceiptStore(
                factory,
                R2dbcBindMarkers.from(factory),
                observer,
                R2dbcConnectionInvalidator.of(
                        ignored -> {
                            closes.incrementAndGet();
                            return closeFailure == null
                                    ? Mono.empty()
                                    : rawError(closeFailure);
                        },
                        ignored -> {
                            invalidations.incrementAndGet();
                            return invalidationFailure == null
                                    ? Mono.empty()
                                    : rawError(invalidationFailure);
                        }));
    }

    private static SqlExecutionObserver cleanupObserver(List<ResourceCleanupObservation> observations) {
        return new SqlExecutionObserver() {
            @Override
            public void onExecution(com.flying.orm.rdb.observation.SqlExecutionObservation observation) {
                // 本夹具只验证连接清理事件。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                observations.add(observation);
            }
        };
    }

    private static ConnectionFactory neverConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.never();
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "test";
            }
        };
    }

    private static ConnectionFactory connectionFactory(Publisher<? extends Result> execution) {
        Connection connection = (Connection) Proxy.newProxyInstance(
                BatchReceiptStoreTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> {
                    if (method.getName().equals("createStatement")) {
                        return statement(execution);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "test";
            }
        };
    }

    private static Statement statement(Publisher<? extends Result> execution) {
        return (Statement) Proxy.newProxyInstance(
                BatchReceiptStoreTest.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind" -> proxy;
                    case "execute" -> execution;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result receiptResult() {
        return (Result) Proxy.newProxyInstance(
                BatchReceiptStoreTest.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (ignored, method, arguments) -> {
                    if (method.getName().equals("map")) {
                        @SuppressWarnings("unchecked")
                        BiFunction<Row, RowMetadata, Object> mapper =
                                (BiFunction<Row, RowMetadata, Object>) arguments[0];
                        return Flux.just(mapper.apply(row(1L, 1L), null));
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static <T> Publisher<T> rawError(Throwable error) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {

            private boolean terminated;

            @Override
            public void request(long ignored) {
                if (!terminated) {
                    terminated = true;
                    subscriber.onError(error);
                }
            }

            @Override
            public void cancel() {
                terminated = true;
            }
        });
    }
}
