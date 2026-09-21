package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcIndependentBatchInputFactsRegressionTest {
    private static final Duration TEST_WAIT = Duration.ofSeconds(5);

    @Test
    void rowLimitKeepsAcceptedPartialChunkAndCommittedPrefix() {
        assertRowLimit(15);
    }

    @Test
    void rowLimitAtChunkBoundaryDoesNotCountRejectedRow() {
        assertRowLimit(10);
    }

    @Test
    void rowLimitBeforeFirstChunkKeepsAllAcceptedRows() {
        assertRowLimit(5);
    }

    @Test
    void sourceFailureKeepsAcceptedPartialChunkAndOriginalCause() {
        Fixture fixture = new Fixture();
        IllegalStateException original = new IllegalStateException("input failed");
        BatchExecutionEvidenceException failure = fixture.failure(Flux.concat(rows(15), Flux.error(original)), 0);
        assertSame(original, failure.getCause());
        assertInputFacts(failure.evidence(), 15, 10);
        assertEquals(0, fixture.commits.get());
        assertEquals(fixture.acquisitions.get(), fixture.closes.get());
    }

    private static void assertRowLimit(int maxRows) {
        Fixture fixture = new Fixture();
        BatchExecutionEvidenceException failure = fixture.failure(rows(maxRows + 1), maxRows);
        R2dbcBatchRowLimitExceededException cause = assertInstanceOf(
                R2dbcBatchRowLimitExceededException.class, failure.getCause());
        assertEquals(maxRows, cause.exceededOffset());
        assertInputFacts(failure.evidence(), maxRows, maxRows / 10 * 10);
        assertEquals(0, fixture.commits.get());
        assertEquals(fixture.acquisitions.get(), fixture.closes.get());
        assertEquals(0, fixture.rollbacks.get(), "unexecuted input must not create a transaction");
    }

    private static void assertInputFacts(BatchExecutionEvidence result, int accepted, int executed) {
        assertEquals(executed == 0 ? BatchExecutionState.FAILED : BatchExecutionState.PARTIAL, result.state());
        assertEquals(accepted, result.inputCount());
        assertEquals(executed, result.affectedRows().value());
        assertEquals(LongStream.range(0, executed).boxed().toList(),
                result.successfulOffsets().boxed().toList());
        assertEquals(0, result.failedCount(), "unexecuted accepted tail is not a failed SQL position");
    }

    private static Flux<Object[]> rows(int count) {
        return Flux.range(0, count).map(value -> new Object[]{value});
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static final class Fixture implements ConnectionFactory {
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.fromSupplier(() -> {
                acquisitions.incrementAndGet();
                AtomicBoolean autoCommit = new AtomicBoolean(true);
                return proxy(Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> autoCommit.get();
                    case "beginTransaction" -> Mono.fromRunnable(() -> autoCommit.set(false));
                    case "commitTransaction" -> Mono.fromRunnable(commits::incrementAndGet);
                    case "rollbackTransaction" -> Mono.fromRunnable(rollbacks::incrementAndGet);
                    case "setAutoCommit" -> Mono.fromRunnable(() -> autoCommit.set((Boolean) arguments[0]));
                    case "close" -> Mono.fromRunnable(closes::incrementAndGet);
                    case "createStatement" -> statement();
                    default -> throw new AssertionError("unexpected connection operation: " + method.getName());
                });
            });
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "H2";
        }

        private Statement statement() {
            AtomicInteger rowCount = new AtomicInteger(1);
            return proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "bind", "bindNull", "fetchSize" -> proxy;
                case "add" -> {
                    rowCount.incrementAndGet();
                    yield proxy;
                }
                case "execute" -> Flux.just(proxy(Result.class, (result, operation, values) -> {
                    if (operation.getName().equals("getRowsUpdated")) {
                        return Mono.just((long) rowCount.get());
                    }
                    throw new AssertionError("unexpected result operation: " + operation.getName());
                }));
                default -> throw new AssertionError("unexpected statement operation: " + method.getName());
            });
        }

        private BatchExecutionEvidenceException failure(Flux<Object[]> rows, int maxRows) {
            return assertThrows(BatchExecutionEvidenceException.class, () -> R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(this), RdbDialect.h2()).writeBatch(
                    BatchWriteRequests.request("insert into input_facts(value_col) values (?)", 1,
                            List.of(Integer.class), SqlBindMarkerStyle.CANONICAL, rows,
                            BatchWriteOptions.of(10).withMaxRows(maxRows), BatchRowCountPolicy.ANY))
                    .block(TEST_WAIT));
        }
    }
}
