package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Type;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditR2dbcLobRegressionTest {

    // Bounds only protect the test runner from a deadlock; latches establish the ordering.
    private static final Duration TEST_WAIT = Duration.ofSeconds(5);

    @Test
    void cancellationDuringSuccessfulCaptureIncludesLocatorsBeforeClosingOwnedConnection() throws Exception {
        assertCancellationHandoff(false, false);
    }

    @Test
    void cancellationDuringFailedCaptureDiscardsLocatorsWithoutClosingExternalConnection() throws Exception {
        assertCancellationHandoff(true, true);
    }

    @Test
    void closedScopeRejectsMappingBeforeAnyRowValueIsRead() {
        R2dbcLargeObjectScope scope = new R2dbcLargeObjectScope();
        scope.cancel().block(TEST_WAIT);
        AtomicInteger reads = new AtomicInteger();
        RowMetadata metadata = metadata(column(R2dbcType.BLOB, "payload"));
        Row row = row(metadata, index -> {
            reads.incrementAndGet();
            throw new AssertionError("a closed scope must not acquire a row locator");
        });

        Mono<DynamicRow> mapped = R2dbcLargeObjectRows.mapper(
                metadata, SqlExecutionOptions.safeDefaults(), scope).map(row);

        assertThrows(IllegalStateException.class, () -> mapped.block(TEST_WAIT));
        assertEquals(0, reads.get());
    }

    @Test
    void scalarMappingDoesNotResolveALargeObjectScope() {
        AtomicInteger reads = new AtomicInteger();
        RowMetadata metadata = metadata(column(R2dbcType.INTEGER, "id"));
        Row row = row(metadata, index -> {
            reads.incrementAndGet();
            return 7;
        });

        Mono<DynamicRow> mapped = R2dbcLargeObjectRows.mapper(
                metadata, SqlExecutionOptions.safeDefaults(),
                () -> {
                    throw new AssertionError("scalar rows must not create a LOB scope");
                }).map(row);

        assertEquals(1, reads.get(), "the driver row must be read in the mapping callback");
        DynamicRow result = mapped.block(TEST_WAIT);
        assertNotNull(result);
        assertEquals(7, result.value(0));
    }

    private static void assertCancellationHandoff(boolean failRead, boolean external) throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        AtomicInteger streams = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        Blob blob = blob(streams, discards, events);
        Clob clob = clob(streams, discards, events);
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("close")) {
                        return Mono.fromRunnable(() -> events.add("close"));
                    }
                    throw new AssertionError("unexpected connection operation: " + method.getName());
                });
        R2dbcExecutionSession.Resources resources = new R2dbcExecutionSession.Resources(connection,
                new com.flying.orm.core.sql.render.SqlRequest("select payload", List.of()));
        R2dbcLargeObjectScope scope = resources.largeObjects();
        R2dbcExecutionSession session = new R2dbcExecutionSession(
                new R2dbcConnectionAccess() {
                    public Publisher<? extends Connection> getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
                        return Mono.just(connection);
                    }
                    public Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                            Connection current, com.flying.orm.core.sql.render.SqlRequest request) {
                        return external ? Mono.empty() : current.close();
                    }
                }, R2dbcBindMarkers.from(com.flying.orm.rdb.dialect.RdbDialect.h2()),
                SqlExecutionObserver.noop(), null);
        CountDownLatch insideFinalColumn = new CountDownLatch(1);
        CountDownLatch releaseFinalColumn = new CountDownLatch(1);
        CountDownLatch cancellationStarted = new CountDownLatch(1);
        AtomicBoolean driverCallbackActive = new AtomicBoolean();
        AtomicBoolean everyReadInsideCallback = new AtomicBoolean(true);
        AtomicBoolean everyReadHoldsScope = new AtomicBoolean(true);
        AtomicInteger reads = new AtomicInteger();
        AtomicInteger readFailures = new AtomicInteger();
        RowMetadata metadata = metadata(
                column(R2dbcType.BLOB, "payload"),
                column(R2dbcType.CLOB, "description"),
                column(R2dbcType.INTEGER, "id"));
        Row row = row(metadata, index -> {
            reads.incrementAndGet();
            if (!driverCallbackActive.get()) {
                everyReadInsideCallback.set(false);
            }
            if (!Thread.holdsLock(scope)) {
                everyReadHoldsScope.set(false);
            }
            if (index == 0) {
                return blob;
            }
            if (index == 1) {
                return clob;
            }
            // Both locators have left the driver; the final column still belongs to its callback.
            insideFinalColumn.countDown();
            await(releaseFinalColumn, "release of the final driver column");
            if (failRead) {
                readFailures.incrementAndGet();
                throw new IllegalStateException("final column read failed");
            }
            return 7;
        });
        R2dbcLargeObjectRows.Mapper mapper =
                R2dbcLargeObjectRows.mapper(metadata, SqlExecutionOptions.safeDefaults(), scope);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<Mono<DynamicRow>> mapped = workers.submit(() -> {
                driverCallbackActive.set(true);
                try {
                    return mapper.map(row);
                } finally {
                    driverCallbackActive.set(false);
                }
            });
            await(insideFinalColumn, "entry into the final driver column");
            Future<?> cancelled = workers.submit(() -> {
                cancellationStarted.countDown();
                session.release(resources, SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.CANCEL, null).block(TEST_WAIT);
            });
            await(cancellationStarted, "cancellation start");

            // Checking the common monitor makes this regression deterministic, not scheduler-dependent.
            assertTrue(everyReadHoldsScope.get(), "capture and cleanup must use the same short critical section");
            assertTrue(everyReadInsideCallback.get(), "Row access must not escape the driver callback");
            assertEquals(3, reads.get());
            releaseFinalColumn.countDown();

            // Deliberately never subscribe to this Mono, including the failed-capture discard publisher.
            assertNotNull(mapped.get(TEST_WAIT.toMillis(), TimeUnit.MILLISECONDS));
            cancelled.get(TEST_WAIT.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(failRead ? 1 : 0, readFailures.get());
            assertEquals(0, streams.get(), "cancellation must not materialize pending locators");
            assertEquals(2, discards.get(), "cleanup must own both locators even without a row subscription");
            assertEquals(external
                    ? List.of("blob-discard", "clob-discard")
                    : List.of("blob-discard", "clob-discard", "close"), events);
        } finally {
            releaseFinalColumn.countDown();
            workers.shutdownNow();
        }
    }

    private static Blob blob(AtomicInteger streams, AtomicInteger discards, List<String> events) {
        return new Blob() {
            @Override
            public Publisher<ByteBuffer> stream() {
                return Flux.defer(() -> {
                    streams.incrementAndGet();
                    return Flux.just(ByteBuffer.wrap(new byte[]{1, 2}));
                });
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.fromRunnable(() -> {
                    discards.incrementAndGet();
                    events.add("blob-discard");
                });
            }
        };
    }

    private static Clob clob(AtomicInteger streams, AtomicInteger discards, List<String> events) {
        return new Clob() {
            @Override
            public Publisher<CharSequence> stream() {
                return Flux.defer(() -> {
                    streams.incrementAndGet();
                    return Flux.<CharSequence>just("text");
                });
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.fromRunnable(() -> {
                    discards.incrementAndGet();
                    events.add("clob-discard");
                });
            }
        };
    }

    private static ColumnMetadata column(Type type, String name) {
        return new ColumnMetadata() {
            @Override
            public Type getType() {
                return type;
            }

            @Override
            public String getName() {
                return name;
            }
        };
    }

    private static RowMetadata metadata(ColumnMetadata... columns) {
        return new RowMetadata() {
            @Override
            public ColumnMetadata getColumnMetadata(int index) {
                return columns[index];
            }

            @Override
            public ColumnMetadata getColumnMetadata(String name) {
                return Arrays.stream(columns)
                        .filter(column -> column.getName().equals(name))
                        .findFirst().orElseThrow();
            }

            @Override
            public List<? extends ColumnMetadata> getColumnMetadatas() {
                return List.of(columns);
            }
        };
    }

    private static Row row(RowMetadata metadata, IntFunction<Object> reader) {
        return new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                return type.cast(reader.apply(index));
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                List<? extends ColumnMetadata> columns = metadata.getColumnMetadatas();
                for (int index = 0; index < columns.size(); index++) {
                    if (columns.get(index).getName().equals(name)) {
                        return get(index, type);
                    }
                }
                throw new IllegalArgumentException("unknown column: " + name);
            }
        };
    }

    private static void await(CountDownLatch latch, String reason) {
        try {
            assertTrue(latch.await(TEST_WAIT.toMillis(), TimeUnit.MILLISECONDS), reason);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for " + reason, interrupted);
        }
    }
}
