package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchReceiptMismatchException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcIndependentReceiptPlanReuseTest {

    @Test
    void writeBatchReplaysEveryChunkOnEachSubscriptionWithoutWriting() {
        replaysEveryChunkOnEachSubscriptionWithoutWriting(false);
    }

    @Test
    void writeBatchChunksReplaysEveryChunkOnEachSubscriptionWithoutWriting() {
        replaysEveryChunkOnEachSubscriptionWithoutWriting(true);
    }

    @Test
    void writeBatchRejectsChangedPlanAndChangedPayloadBeforeWriting() {
        rejectsChangedPlanAndChangedPayloadBeforeWriting(false);
    }

    @Test
    void writeBatchChunksRejectsChangedPlanAndChangedPayloadBeforeWriting() {
        rejectsChangedPlanAndChangedPayloadBeforeWriting(true);
    }

    @Test
    void writeBatchReplaysAfterReservationConflictAndReleasesTheTransactionFirst() {
        replaysAfterReservationConflictAndReleasesTheTransactionFirst(false);
    }

    @Test
    void writeBatchChunksReplaysAfterReservationConflictAndReleasesTheTransactionFirst() {
        replaysAfterReservationConflictAndReleasesTheTransactionFirst(true);
    }

    @Test
    void emptyWriteBatchDoesNotAcquireConnectionsWithOrWithoutReceipts() {
        emptyInputDoesNotAcquireConnectionsWithOrWithoutReceipts(false);
    }

    @Test
    void emptyWriteBatchChunksDoesNotAcquireConnectionsWithOrWithoutReceipts() {
        emptyInputDoesNotAcquireConnectionsWithOrWithoutReceipts(true);
    }

    private static void replaysEveryChunkOnEachSubscriptionWithoutWriting(boolean streaming) {
        AtomicInteger subscriptions = new AtomicInteger();
        BatchWriteRequest request = request(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.range(0, 3).map(value -> new Object[]{value});
        }), true);
        ReceiptDriver driver = new ReceiptDriver(request);
        Mono<BatchWriteResult> execution = execution(driver, request, streaming);

        for (int attempt = 0; attempt < 2; attempt++) {
            BatchWriteResult result = execution.block(Duration.ofSeconds(2));
            assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
            assertEquals(3, result.inputCount());
            assertEquals(3, result.affectedRows());
            assertEquals(List.of(0, 1, 2), result.chunks().stream().map(BatchChunkResult::chunkIndex).toList());
            assertEquals(List.of(0L, 1L, 2L), result.chunks().stream().map(BatchChunkResult::startOffset).toList());
        }
        assertEquals(2, subscriptions.get());
        assertEquals(6, driver.reads);
        assertEquals(0, driver.reservations);
        assertEquals(0, driver.businessStatements);
        assertEquals(driver.acquires, driver.closes);
    }

    private static void rejectsChangedPlanAndChangedPayloadBeforeWriting(boolean streaming) {
        for (boolean changedPlan : new boolean[]{false, true}) {
            BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{0}), true);
            ReceiptDriver driver = new ReceiptDriver(request);
            if (changedPlan) {
                driver.planHash = "different-plan";
            } else {
                driver.payloads.put(0, "different-payload");
            }

            BatchWriteException failure = assertThrows(BatchWriteException.class,
                    () -> execution(driver, request, streaming).block(Duration.ofSeconds(2)));

            assertInstanceOf(BatchReceiptMismatchException.class,
                    ThrowableGraph.findCause(failure, BatchReceiptMismatchException.class));
            assertEquals(0, driver.businessStatements);
            assertEquals(0, driver.reservations);
            assertEquals(driver.acquires, driver.closes);
        }
    }

    private static void replaysAfterReservationConflictAndReleasesTheTransactionFirst(boolean streaming) {
        BatchWriteRequest request = request(Flux.<Object[]>just(new Object[]{0}), true);
        ReceiptDriver driver = new ReceiptDriver(request);
        driver.reservationConflict = true;

        BatchWriteResult result = execution(driver, request, streaming).block(Duration.ofSeconds(2));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, result.inputCount());
        assertEquals(1, result.affectedRows());
        assertEquals(2, driver.reads);
        assertEquals(1, driver.reservations);
        assertEquals(1, driver.rollbacks);
        assertEquals(0, driver.businessStatements);
        assertEquals(3, driver.acquires);
        assertEquals(driver.acquires, driver.closes);
    }

    private static void emptyInputDoesNotAcquireConnectionsWithOrWithoutReceipts(boolean streaming) {
        for (boolean receipt : new boolean[]{false, true}) {
            BatchWriteRequest request = request(Flux.empty(), receipt);
            ReceiptDriver driver = new ReceiptDriver(request);

            BatchWriteResult result = execution(driver, request, streaming).block(Duration.ofSeconds(2));

            assertEquals(0, result.inputCount());
            assertEquals(List.of(), result.chunks());
            assertEquals(0, driver.acquires);
            assertEquals(0, driver.reads);
            assertEquals(0, driver.businessStatements);
        }
    }

    private static BatchWriteRequest request(Publisher<Object[]> rows, boolean receipt) {
        BatchWriteOptions options = BatchWriteOptions.independent(1);
        if (receipt) {
            options = options.withReceipt("independent-plan-reuse", Duration.ZERO);
        }
        return BatchWriteRequests.request("insert into samples(value_col) values (?)", 1,
                List.of(Integer.class), SqlBindMarkerStyle.CANONICAL, rows, options);
    }

    private static Mono<BatchWriteResult> execution(ReceiptDriver driver,
                                                    BatchWriteRequest request,
                                                    boolean streaming) {
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(driver);
        return streaming
                ? executor.writeBatchChunks(request).collectList()
                        .map(chunks -> BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, chunks))
                : executor.writeBatch(request);
    }

    private static final class ReceiptDriver implements ConnectionFactory {
        private String planHash;
        private final Map<Integer, String> payloads = new HashMap<>();
        private boolean reservationConflict;
        private int acquires;
        private int closes;
        private int reads;
        private int reservations;
        private int rollbacks;
        private int businessStatements;

        private ReceiptDriver(BatchWriteRequest request) {
            BatchPayloadHasher hasher = new BatchPayloadHasher();
            planHash = hasher.hashPlan(request);
            for (int index = 0; index < 3; index++) {
                payloads.put(index, hasher.hashRows(List.<Object[]>of(new Object[]{index})));
            }
        }

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.fromSupplier(() -> {
                assertEquals(acquires, closes, "the previous receipt/transaction connection must be released");
                acquires++;
                return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement((String) arguments[0]);
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "setAutoCommit" -> Mono.empty();
                    case "rollbackTransaction" -> Mono.fromRunnable(() -> rollbacks++);
                    case "close" -> Mono.fromRunnable(() -> closes++);
                    default -> throw new AssertionError("unexpected connection call: " + method.getName());
                });
            });
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "H2";
        }

        private Statement statement(String sql) {
            boolean read = sql.startsWith("select plan_hash, payload_hash");
            boolean reserve = sql.startsWith("insert into ")
                    && sql.toLowerCase(Locale.ROOT).contains("flying_orm_batch_receipt");
            if (!read && !reserve) {
                businessStatements++;
                throw new AssertionError("replay must not execute business SQL: " + sql);
            }
            Map<Integer, Object> bindings = new HashMap<>();
            return proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "bind" -> {
                    bindings.put((Integer) arguments[0], arguments[1]);
                    yield self;
                }
                case "execute" -> {
                    if (reserve) {
                        reservations++;
                        yield Flux.error(new RdbException(RdbErrorKind.DUPLICATE_KEY,
                                "receipt reserved concurrently", "23505", null,
                                new IllegalStateException("duplicate receipt")));
                    }
                    reads++;
                    yield Flux.just(receiptResult((Integer) bindings.get(1)));
                }
                default -> throw new AssertionError("unexpected statement call: " + method.getName());
            });
        }

        private Result receiptResult(int chunkIndex) {
            return proxy(Result.class, (self, method, arguments) -> {
                if (!"map".equals(method.getName())) {
                    throw new AssertionError("unexpected result call: " + method.getName());
                }
                if (reservationConflict && reservations == 0) {
                    return Flux.empty();
                }
                Row row = proxy(Row.class, (rowSelf, rowMethod, rowArguments) -> {
                    if (!"get".equals(rowMethod.getName())) {
                        throw new AssertionError("unexpected row call: " + rowMethod.getName());
                    }
                    return switch ((Integer) rowArguments[0]) {
                        case 0 -> planHash;
                        case 1 -> payloads.get(chunkIndex);
                        case 2, 3 -> 1L;
                        default -> throw new AssertionError("unexpected receipt column");
                    };
                });
                @SuppressWarnings("unchecked")
                BiFunction<Row, RowMetadata, Object> mapper = (BiFunction<Row, RowMetadata, Object>) arguments[0];
                return Flux.just(mapper.apply(row, null));
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
