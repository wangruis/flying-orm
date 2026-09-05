package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcGeneratedKeyWriterLayoutTest {

    private static final SqlExecutionOptions OPTIONS = SqlExecutionOptions.safeDefaults();

    @Test
    void skipsGeneratedKeySizingWhenTheByteBudgetIsDisabled() {
        for (SqlExecutionOptions options : List.of(OPTIONS, OPTIONS.withMaxRows(500))) {
            CountingText text = new CountingText("x".repeat(256));
            Metadata metadata = new Metadata(R2dbcType.VARCHAR, "key_", 1);
            Fixture fixture = new Fixture(() -> List.of(result(
                    Flux.range(0, 500).map(ignored -> row(metadata, text)), new AtomicInteger())));

            SqlWriteResult actual = fixture.write(options).block();

            assertEquals(500, actual.affectedRows());
            assertEquals(500, actual.generatedKeys().size());
            actual.generatedKeys().forEach(key -> assertSame(text, key.value(0)));
            assertEquals(0, text.characterReads, "disabled byte budgets must not scan key payloads");
            assertEquals(2, metadata.listReads);
            assertEquals(1, fixture.executes);
            assertEquals(1, fixture.closes);
        }
    }

    @Test
    void appliesTheByteBudgetAcrossGeneratedKeyResults() {
        // Each one-column row has weight 312: row 24 + reference 8 + text 24 + ASCII 256.
        for (long limit : new long[]{624, 623}) {
            CountingText text = new CountingText("x".repeat(256));
            Metadata metadata = new Metadata(R2dbcType.VARCHAR, "key_", 1);
            Fixture fixture = new Fixture(() -> List.of(
                    result(Flux.just(count(7), row(metadata, text)), new AtomicInteger()),
                    result(Flux.just(row(metadata, text)), new AtomicInteger())));

            if (limit == 624) {
                SqlWriteResult actual = fixture.write(OPTIONS.withMaxResultBytes(limit)).block();
                assertEquals(7, actual.affectedRows());
                assertEquals(2, actual.generatedKeys().size());
            } else {
                GeneratedKeyReadException failure = assertThrows(GeneratedKeyReadException.class,
                        () -> fixture.write(OPTIONS.withMaxResultBytes(limit)).block());
                SqlResultMemoryLimitExceededException budget = assertInstanceOf(
                        SqlResultMemoryLimitExceededException.class, failure.getCause());
                assertEquals(624, budget.attemptedBytes());
                assertEquals(1, budget.overflowIndex());
                assertEquals(7, failure.affectedRows());
            }
            assertEquals(512, text.characterReads);
            assertEquals(4, metadata.listReads, "each Result keeps its own layout");
            assertEquals(1, fixture.executes);
            assertEquals(1, fixture.closes);
        }
    }

    @Test
    void buildsOneLayoutForFiveHundredGeneratedRowsInOneResult() {
        Metadata metadata = new Metadata(R2dbcType.BIGINT, "key_", 10);
        AtomicInteger consumptions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(
                Flux.range(1, 500).map(value -> {
                    Object[] values = new Object[10];
                    for (int index = 0; index < values.length; index++) {
                        values[index] = value * 100L + index;
                    }
                    return row(metadata, values);
                }), consumptions)));

        SqlWriteResult actual = fixture.write(OPTIONS.withMaxRows(2_000).withMaxResultBytes(1_048_576)).block();

        assertEquals(500, actual.affectedRows());
        assertEquals(500, actual.generatedKeys().size());
        assertEquals(100L, actual.generatedKeys().getFirst().get("key_0"));
        assertEquals(50_009L, actual.generatedKeys().getLast().get("key_9"));
        assertEquals(1, consumptions.get());
        assertEquals(1, fixture.executes);
        assertEquals(1, fixture.closes);
        assertEquals(2, metadata.listReads, "column layout and LOB plan are built once per Result");
        assertEquals(10, metadata.nameReads);
        assertEquals(10, metadata.typeReads);
    }

    @Test
    void keepsDifferentResultLayoutsAndUpdateCountsSeparate() {
        Metadata first = new Metadata(R2dbcType.BIGINT, "first_", 1);
        Metadata second = new Metadata(R2dbcType.VARCHAR, "second_", 2);
        AtomicInteger consumptions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(
                result(Flux.just(count(7), row(first, 11L), row(first, 12L)), consumptions),
                result(Flux.just(row(second, "a", "b"), count(3)), consumptions)));

        SqlWriteResult actual = fixture.write(OPTIONS).block();

        assertEquals(10, actual.affectedRows());
        assertEquals(3, actual.generatedKeys().size());
        assertEquals(12L, actual.generatedKeys().get(1).get("first_0"));
        assertEquals("a", actual.generatedKeys().get(2).get("second_0"));
        assertEquals("b", actual.generatedKeys().get(2).get("second_1"));
        assertEquals(2, consumptions.get());
        assertEquals(2, first.listReads);
        assertEquals(2, second.listReads);
        assertEquals(1, fixture.closes);
    }

    @Test
    void doesNotReuseLayoutsAcrossSubscriptions() {
        AtomicInteger subscriptions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> {
            int value = subscriptions.incrementAndGet();
            Metadata metadata = new Metadata(R2dbcType.INTEGER, "subscription_" + value + "_", 1);
            return List.of(result(Flux.just(row(metadata, value)), new AtomicInteger()));
        });
        Mono<SqlWriteResult> pending = fixture.write(OPTIONS);

        assertEquals(1, pending.block().generatedKeys().getFirst().get("subscription_1_0"));
        assertEquals(2, pending.block().generatedKeys().getFirst().get("subscription_2_0"));
        assertEquals(2, fixture.executes);
        assertEquals(2, fixture.closes);
    }

    @Test
    void checksRowBudgetBeforeReadingTheNextResultLayout() {
        Metadata first = new Metadata(R2dbcType.BIGINT, "id_", 1);
        Metadata second = new Metadata(R2dbcType.BIGINT, "next_", 1);
        Fixture fixture = new Fixture(() -> List.of(
                result(Flux.just(row(first, 1L)), new AtomicInteger()),
                result(Flux.just(row(second, 2L)), new AtomicInteger())));

        GeneratedKeyReadException failure = assertThrows(GeneratedKeyReadException.class,
                () -> fixture.write(OPTIONS.withMaxRows(1)).block());

        assertInstanceOf(SqlRowLimitExceededException.class, failure.getCause());
        assertEquals(2, failure.affectedRows());
        assertEquals(0, second.listReads);
        assertEquals(1, fixture.closes);
    }

    @Test
    void preservesResultMemoryBudgetAndObservedWriteCount() {
        Metadata metadata = new Metadata(R2dbcType.VARCHAR, "value_", 1);
        Fixture fixture = new Fixture(() -> List.of(result(
                Flux.just(count(9), row(metadata, "generated value")), new AtomicInteger())));

        GeneratedKeyReadException failure = assertThrows(GeneratedKeyReadException.class,
                () -> fixture.write(OPTIONS.withMaxResultBytes(1)).block());

        assertInstanceOf(SqlResultMemoryLimitExceededException.class, failure.getCause());
        assertEquals(9, failure.affectedRows());
        assertEquals(1, fixture.closes);
    }

    @Test
    void preservesErrorSegmentsBeforeAndAfterWriteEvidence() {
        R2dbcNonTransientResourceException driverFailure = new R2dbcNonTransientResourceException("key read failed");
        Result.Message message = proxy(Result.Message.class, (proxy, method, arguments) -> {
            if (method.getName().equals("exception")) {
                return driverFailure;
            }
            throw new UnsupportedOperationException(method.getName());
        });
        Fixture before = new Fixture(() -> List.of(result(Flux.just(message), new AtomicInteger())));
        RdbException translated = assertThrows(RdbException.class, () -> before.write(OPTIONS).block());
        assertSame(driverFailure, translated.getCause());
        assertEquals(1, before.closes);

        Fixture after = new Fixture(() -> List.of(result(Flux.just(count(4), message), new AtomicInteger())));
        GeneratedKeyReadException failure = assertThrows(GeneratedKeyReadException.class,
                () -> after.write(OPTIONS).block());
        assertSame(driverFailure, failure.getCause());
        assertEquals(4, failure.affectedRows());
        assertEquals(1, after.closes);
    }

    @Test
    void reusesLobLayoutWhileConsumingEveryLocatorOnce() {
        Metadata metadata = new Metadata(R2dbcType.BLOB, "payload_", 1);
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(Flux.just(
                row(metadata, blob(Flux.just(ByteBuffer.wrap(new byte[]{1}))
                                       .doOnComplete(completions::incrementAndGet), discards)),
                row(metadata, blob(Flux.just(ByteBuffer.wrap(new byte[]{2}))
                                       .doOnComplete(completions::incrementAndGet), discards))), new AtomicInteger())));

        SqlWriteResult actual = fixture.write(OPTIONS).block();

        assertArrayEquals(new byte[]{1}, (byte[]) actual.generatedKeys().getFirst().value(0));
        assertArrayEquals(new byte[]{2}, (byte[]) actual.generatedKeys().getLast().value(0));
        assertEquals(2, completions.get());
        assertEquals(0, discards.get(), "consumed locators must not be discarded a second time");
        assertEquals(1, fixture.closes);
        assertEquals(2, metadata.listReads);
    }

    @TestFactory
    Stream<DynamicTest> deadlineRetainsOnlyObservedGeneratedKeyWriteEvidence() {
        return Stream.of(false, true).map(observed -> DynamicTest.dynamicTest(
                "returning keys deadline [write observed=" + observed + "]", () -> {
                    AtomicInteger consumptions = new AtomicInteger();
                    Flux<Result.Segment> segments = observed
                            ? Flux.<Result.Segment>just(count(4)).concatWith(Flux.never()) : Flux.never();
                    Fixture fixture = new Fixture(() -> List.of(result(segments, consumptions)));
                    Duration timeout = Duration.ofSeconds(30);
                    AtomicReference<Runnable> deadline = new AtomicReference<>();
                    AtomicReference<Throwable> error = new AtomicReference<>();
                    AtomicInteger values = new AtomicInteger();
                    String hook = getClass().getName() + ".keyEvidenceDeadline";
                    Schedulers.onScheduleHook(hook, task -> {
                        deadline.compareAndSet(null, task);
                        return task;
                    });
                    Disposable subscription = null;
                    try {
                        subscription = fixture.write(OPTIONS.withTimeout(timeout).withCleanupTimeout(Duration.ZERO))
                                .subscribe(ignored -> values.incrementAndGet(), error::set);
                        assertEquals(1, consumptions.get());
                        assertNotNull(deadline.get());
                        deadline.get().run();

                        Throwable timeoutFailure = error.get();
                        if (observed) {
                            GeneratedKeyReadException failure = assertInstanceOf(
                                    GeneratedKeyReadException.class, timeoutFailure);
                            assertEquals(4L, failure.affectedRows());
                            timeoutFailure = failure.getCause();
                        }
                        assertEquals(timeout, assertInstanceOf(
                                SqlExecutionTimeoutException.class, timeoutFailure).timeout());
                        assertEquals(0, values.get());
                        assertEquals(1, fixture.closes);
                    } finally {
                        if (subscription != null) {
                            subscription.dispose();
                        }
                        Schedulers.resetOnScheduleHook(hook);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> sharesOneExecutionDeadlineAcrossLobRowsAndColumns() {
        return Stream.of(false, true).flatMap(query ->
                Stream.of(Duration.ZERO, Duration.ofSeconds(30)).map(timeout ->
                        DynamicTest.dynamicTest((query ? "query" : "returning keys") + " [timeout=" + timeout + "]",
                                () -> assertLobDeadline(query, timeout))));
    }

    @Test
    void deadlineBeforeGeneratedKeyResultDeliveryRetainsObservedWriteEvidence() {
        Metadata metadata = new Metadata(R2dbcType.BIGINT, "id_", 1);
        AtomicInteger completions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(
                Flux.just(count(1), row(metadata, 11L)).doOnComplete(completions::incrementAndGet),
                new AtomicInteger())));
        AtomicReference<Runnable> deadline = new AtomicReference<>();
        AtomicReference<SqlWriteResult> pendingResult = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        AtomicInteger values = new AtomicInteger();
        String hook = getClass().getName() + ".keyResultDeliveryDeadline";
        Schedulers.onScheduleHook(hook, task -> {
            deadline.compareAndSet(null, task);
            return task;
        });
        Hooks.onEachOperator(hook, Operators.<Object, Object>lift((operator, actual) ->
                new CoreSubscriber<Object>() {
                    public Context currentContext() { return actual.currentContext(); }
                    public void onSubscribe(Subscription subscription) { actual.onSubscribe(subscription); }
                    public void onNext(Object value) {
                        if (value instanceof SqlWriteResult result && pendingResult.compareAndSet(null, result)) {
                            // The driver completed; pause the first result signal before the deadline receives it.
                            assertNotNull(deadline.get());
                            deadline.get().run();
                        }
                        actual.onNext(value);
                    }
                    public void onError(Throwable failure) { actual.onError(failure); }
                    public void onComplete() { actual.onComplete(); }
                }));
        Disposable subscription = null;
        try {
            subscription = fixture.write(OPTIONS.withTimeout(Duration.ofSeconds(30))
                            .withCleanupTimeout(Duration.ZERO))
                    .subscribe(ignored -> values.incrementAndGet(), error::set);

            assertEquals(1, completions.get());
            assertNotNull(pendingResult.get());
            assertEquals(1L, pendingResult.get().affectedRows());
            assertEquals(11L, pendingResult.get().generatedKeys().getFirst().get("id_0"));
            assertEquals(0, values.get());
            assertEquals(1, fixture.executes);
            assertEquals(1, fixture.closes);
            GeneratedKeyReadException failure = assertInstanceOf(GeneratedKeyReadException.class, error.get());
            assertEquals(1L, failure.affectedRows());
            assertInstanceOf(SqlExecutionTimeoutException.class, failure.getCause());
        } finally {
            if (subscription != null) {
                subscription.dispose();
            }
            Hooks.resetOnEachOperator(hook);
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    private void assertLobDeadline(boolean query, Duration timeout) {
        Metadata metadata = new Metadata(R2dbcType.BLOB, "payload_", 3);
        AtomicInteger streams = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger consumptions = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(Flux.range(1, 3).map(value -> row(metadata,
                blob(Flux.just(ByteBuffer.wrap(new byte[]{value.byteValue()}))
                        .doOnSubscribe(ignored -> streams.incrementAndGet())
                        .doOnComplete(completions::incrementAndGet), discards),
                blob(Flux.just(ByteBuffer.wrap(new byte[]{(byte) (value + 10)}))
                        .doOnSubscribe(ignored -> streams.incrementAndGet())
                        .doOnComplete(completions::incrementAndGet), discards),
                new byte[]{(byte) (value + 20)})), consumptions)));
        SqlExecutionOptions options = OPTIONS.withTimeout(timeout).withCleanupTimeout(Duration.ZERO);
        AtomicInteger scheduledTasks = new AtomicInteger();
        String hook = getClass().getName() + ".lobExecutionDeadline";
        Schedulers.onScheduleHook(hook, task -> {
            scheduledTasks.incrementAndGet();
            return task;
        });
        try {
            List<DynamicRow> actual = query ? fixture.query(options).collectList().block()
                    : fixture.write(options).block().generatedKeys();

            assertEquals(3, actual.size());
            for (int index = 0; index < actual.size(); index++) {
                assertArrayEquals(new byte[]{(byte) (index + 1)}, (byte[]) actual.get(index).value(0));
                assertArrayEquals(new byte[]{(byte) (index + 11)}, (byte[]) actual.get(index).value(1));
                assertArrayEquals(new byte[]{(byte) (index + 21)}, (byte[]) actual.get(index).value(2));
            }
            assertEquals(6, streams.get(), "each locator must be subscribed once");
            assertEquals(6, completions.get());
            assertEquals(0, discards.get(), "consumed locators must not be discarded again");
            assertEquals(1, consumptions.get());
            assertEquals(1, fixture.executes);
            assertEquals(1, fixture.closes);
            assertEquals(timeout.isZero() ? 0 : 1, scheduledTasks.get(),
                    "LOB row and column counts must not add execution deadlines");
        } finally {
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    @TestFactory
    Stream<DynamicTest> executionDeadlineCancelsStreamingLobAndDiscardsPendingLob() {
        return Stream.of(false, true).map(query -> DynamicTest.dynamicTest(
                query ? "query deadline cleanup" : "returning keys deadline cleanup",
                () -> assertLobTimeout(query)));
    }

    private void assertLobTimeout(boolean query) {
        Metadata metadata = new Metadata(R2dbcType.BLOB, "payload_", 2);
        AtomicInteger streams = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger streamedDiscards = new AtomicInteger();
        AtomicInteger pendingStreams = new AtomicInteger();
        AtomicInteger pendingDiscards = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(Flux.just(row(metadata,
                blob(Flux.<ByteBuffer>never().doOnSubscribe(ignored -> streams.incrementAndGet())
                        .doOnCancel(cancellations::incrementAndGet), streamedDiscards),
                blob(Flux.just(ByteBuffer.wrap(new byte[]{2}))
                        .doOnSubscribe(ignored -> pendingStreams.incrementAndGet()), pendingDiscards))),
                new AtomicInteger())));
        Duration timeout = Duration.ofSeconds(30);
        SqlExecutionOptions options = OPTIONS.withTimeout(timeout).withCleanupTimeout(Duration.ZERO);
        AtomicReference<Runnable> deadline = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicInteger values = new AtomicInteger();
        String hook = getClass().getName() + ".lobDeadlineCleanup";
        Schedulers.onScheduleHook(hook, task -> {
            deadline.compareAndSet(null, task);
            return task;
        });
        Disposable subscription = null;
        try {
            Flux<?> execution = query ? fixture.query(options) : fixture.write(options).flux();
            subscription = execution.subscribe(ignored -> values.incrementAndGet(), failure::set);
            assertEquals(1, streams.get());
            assertNotNull(deadline.get());

            // Drive the real outer timer after LOB registration, without waiting for wall-clock time.
            deadline.get().run();

            Throwable timeoutFailure = failure.get();
            if (!query) {
                GeneratedKeyReadException keyFailure = assertInstanceOf(
                        GeneratedKeyReadException.class, timeoutFailure);
                assertEquals(1L, keyFailure.affectedRows());
                timeoutFailure = keyFailure.getCause();
            }
            SqlExecutionTimeoutException timedOut = assertInstanceOf(
                    SqlExecutionTimeoutException.class, timeoutFailure);
            assertEquals(timeout, timedOut.timeout());
            assertInstanceOf(TimeoutException.class, timedOut.getCause());
            assertEquals(0, values.get());
            assertEquals(1, cancellations.get());
            assertEquals(0, streamedDiscards.get());
            assertEquals(0, pendingStreams.get());
            assertEquals(1, pendingDiscards.get());
            assertEquals(1, fixture.closes);
        } finally {
            if (subscription != null) {
                subscription.dispose();
            }
            Schedulers.resetOnScheduleHook(hook);
        }
    }

    @Test
    void cancellationDiscardsThePendingLocatorAndClosesTheConnection() {
        Metadata metadata = new Metadata(R2dbcType.BLOB, "payload_", 2);
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger streams = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        AtomicInteger pendingStreams = new AtomicInteger();
        Fixture fixture = new Fixture(() -> List.of(result(Flux.just(row(metadata,
                blob(Flux.<ByteBuffer>never().doOnSubscribe(ignored -> streams.incrementAndGet())
                         .doOnCancel(cancellations::incrementAndGet), discards),
                blob(Flux.just(ByteBuffer.wrap(new byte[]{2}))
                         .doOnSubscribe(ignored -> pendingStreams.incrementAndGet()), discards))), new AtomicInteger())));

        Disposable subscription = fixture.write(OPTIONS).subscribe();
        assertEquals(1, streams.get());
        subscription.dispose();

        assertEquals(1, cancellations.get());
        assertEquals(0, pendingStreams.get());
        assertEquals(1, discards.get(), "only the locator whose stream never started needs discard");
        assertEquals(1, fixture.closes);
    }

    private static Blob blob(Publisher<ByteBuffer> content, AtomicInteger discards) {
        return new Blob() {
            public Publisher<ByteBuffer> stream() { return content; }
            public Publisher<Void> discard() { return Mono.fromRunnable(discards::incrementAndGet); }
        };
    }

    private static final class CountingText implements CharSequence {
        private final String value;
        private int characterReads;

        private CountingText(String value) { this.value = value; }
        public int length() { return value.length(); }
        public char charAt(int index) { characterReads++; return value.charAt(index); }
        public CharSequence subSequence(int start, int end) { return value.subSequence(start, end); }
        @Override public String toString() { return value; }
    }

    private static Result.UpdateCount count(long rows) {
        return () -> rows;
    }

    private static Result.RowSegment row(Metadata metadata, Object... values) {
        Row row = new Row() {
            public RowMetadata getMetadata() { return metadata; }
            public <T> T get(int index, Class<T> type) { return type.cast(values[index]); }
            public <T> T get(String name, Class<T> type) {
                return get(metadata.names.indexOf(name), type);
            }
        };
        return () -> row;
    }

    @SuppressWarnings("unchecked")
    private static Result result(Flux<? extends Result.Segment> segments, AtomicInteger consumptions) {
        return proxy(Result.class, (proxy, method, arguments) -> {
            if (method.getName().equals("flatMap")) {
                Function<Result.Segment, Publisher<?>> mapper =
                        (Function<Result.Segment, Publisher<?>>) arguments[0];
                return segments.doOnSubscribe(ignored -> consumptions.incrementAndGet())
                               .concatMap(segment -> Flux.from(mapper.apply(segment)), 1);
            }
            if (method.getName().equals("map")) {
                BiFunction<Row, RowMetadata, ?> mapper = (BiFunction<Row, RowMetadata, ?>) arguments[0];
                return segments.doOnSubscribe(ignored -> consumptions.incrementAndGet())
                               .ofType(Result.RowSegment.class)
                               .map(segment -> mapper.apply(segment.row(), segment.row().getMetadata()));
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }

    private static final class Metadata implements RowMetadata {
        private final List<String> names = new ArrayList<>();
        private final List<ColumnMetadata> columns = new ArrayList<>();
        private int listReads;
        private int nameReads;
        private int typeReads;

        private Metadata(R2dbcType type, String prefix, int width) {
            for (int index = 0; index < width; index++) {
                String name = prefix + index;
                names.add(name);
                columns.add(new ColumnMetadata() {
                    public String getName() { nameReads++; return name; }
                    public R2dbcType getType() { typeReads++; return type; }
                    public Class<?> getJavaType() { return type.getJavaType(); }
                });
            }
        }

        public ColumnMetadata getColumnMetadata(int index) { return columns.get(index); }
        public ColumnMetadata getColumnMetadata(String name) { return columns.get(names.indexOf(name)); }
        public List<? extends ColumnMetadata> getColumnMetadatas() { listReads++; return columns; }
    }

    private static final class Fixture {
        private final R2dbcSqlExecutor executor;
        private int executes;
        private int closes;

        private Fixture(Supplier<List<Result>> results) {
            Statement statement = proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "execute" -> { executes++; yield Flux.fromIterable(results.get()); }
                case "returnGeneratedValues", "fetchSize" -> proxy;
                default -> throw new UnsupportedOperationException(method.getName());
            });
            Connection connection = proxy(Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "createStatement" -> statement;
                case "close" -> { closes++; yield Mono.empty(); }
                default -> throw new UnsupportedOperationException(method.getName());
            });
            executor = R2dbcSqlExecutor.create(new ConnectionFactory() {
                public Publisher<? extends Connection> create() { return Mono.just(connection); }
                public ConnectionFactoryMetadata getMetadata() { return () -> "PostgreSQL"; }
            });
        }

        private Mono<SqlWriteResult> write(SqlExecutionOptions options) {
            return executor.rowsUpdatedReturningKeys(new SqlRequest(
                    "insert into generated_values(payload) select payload from source_values", List.of()), options);
        }

        private Flux<DynamicRow> query(SqlExecutionOptions options) {
            return executor.query(new SqlRequest("select payload from source_values", List.of()), options);
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
