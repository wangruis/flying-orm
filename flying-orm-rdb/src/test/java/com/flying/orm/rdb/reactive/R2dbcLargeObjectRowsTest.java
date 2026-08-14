package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证 R2DBC 行级大字段物化的单次订阅和未开始句柄清理契约。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
class R2dbcLargeObjectRowsTest {

    /** 下游提前结束后仍要排空已取得的结果段，再允许连接级清理完成。 */
    @Test
    void drainsRemainingRowsBeforeConnectionCleanupAfterCancellation() {
        R2dbcLargeObjectScope scope = new R2dbcLargeObjectScope();
        AtomicInteger consumed = new AtomicInteger();

        StepVerifier.create(Flux.from(R2dbcCancellationDrain.drain(
                            Flux.range(1, 3).doOnNext(ignored -> consumed.incrementAndGet()),
                            scope,
                            java.time.Duration.ofSeconds(1))).take(1))
                    .expectNext(1)
                    .verifyComplete();
        StepVerifier.create(scope.complete()).verifyComplete();

        assertEquals(3, consumed.get());
    }

    /** 排空超过清理时限时必须取消驱动结果并让连接清理转入失效路径。 */
    @Test
    void abortsResultDrainWhenCleanupTimeoutExpires() {
        R2dbcLargeObjectScope scope = new R2dbcLargeObjectScope();
        AtomicBoolean cancelled = new AtomicBoolean();
        Publisher<Integer> source = subscriber -> subscriber.onSubscribe(new org.reactivestreams.Subscription() {
            private boolean emitted;

            @Override
            public void request(long demand) {
                if (!emitted) {
                    emitted = true;
                    subscriber.onNext(1);
                }
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        });

        StepVerifier.create(Flux.from(R2dbcCancellationDrain.drain(
                            source, scope, java.time.Duration.ofMillis(10))).take(1))
                    .expectNext(1)
                    .verifyComplete();
        StepVerifier.create(scope.complete())
                    .expectError(java.util.concurrent.TimeoutException.class)
                    .verify();

        assertTrue(cancelled.get());
    }

    /** 排空等待期间才登记的 LOB 行也必须在中止后释放同行未开始的 locator。 */
    @Test
    void discardsLargeObjectsRegisteredWhileResultDrainIsStopping() {
        R2dbcLargeObjectScope scope = new R2dbcLargeObjectScope();
        AtomicInteger discards = new AtomicInteger();
        AtomicReference<reactor.core.Disposable> materialization = new AtomicReference<>();
        scope.registerDrain(Mono.never(), () -> materialization.get().dispose(),
                java.time.Duration.ofMillis(10));

        StepVerifier.create(scope.complete())
                    .then(() -> materialization.set(scope.materialize(
                            row(Blob.from(Flux.never()), trackingClob(discards)),
                            SqlExecutionOptions.safeDefaults()).subscribe()))
                    .expectError(java.util.concurrent.TimeoutException.class)
                    .verify();

        assertEquals(1, discards.get());
    }

    /** 元数据明确为普通标量时，映射计划直接产生 DynamicRow。 */
    @Test
    void mapsKnownScalarRowsWithoutReactiveInnerPublisher() {
        RowMetadata metadata = metadata("id", "name");
        Row row = row(metadata, 1L, "alice");
        R2dbcLargeObjectRows.Mapper mapper = R2dbcLargeObjectRows.mapper(
                metadata, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope());

        DynamicRow mapped = assertInstanceOf(DynamicRow.class, mapper.mapValue(row));

        assertEquals(1L, mapped.get("id"));
        assertEquals("alice", mapped.get("name"));
    }

    /** 下游尚未请求行时，映射结果不能越过 Reactive Streams demand 发出。 */
    @Test
    void doesNotEmitDriverRowBeforeDownstreamDemand() {
        RowMetadata metadata = metadata("id", "name");
        Row row = row(metadata, 1L, "alice");
        Result result = result(row);

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                    result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 0)
                    .expectSubscription()
                    .expectNoEvent(java.time.Duration.ofMillis(10))
                    .thenRequest(1)
                    .assertNext(mapped -> {
                        assertEquals(1L, mapped.get("id"));
                        assertEquals("alice", mapped.get("name"));
                    })
                    .verifyComplete();
    }

    /** 消费完一行且没有剩余 demand 时，不得预取下一驱动行。 */
    @Test
    void doesNotRequestNextDriverRowWithoutRemainingDemand() {
        RowMetadata metadata = metadata("id");
        AtomicInteger requests = new AtomicInteger();
        Result result = trackingResult(requests, null, row(metadata, 1L), row(metadata, 2L));

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                            result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 0)
                    .thenRequest(1)
                    .assertNext(mapped -> assertEquals(1L, mapped.get("id")))
                    .then(() -> assertEquals(1, requests.get()))
                    .thenCancel()
                    .verify();
    }

    /** 首行 LOB 尚未结束时，即使 demand 充足也不能取得下一驱动行。 */
    @Test
    void doesNotRequestNextDriverRowWhileLargeObjectIsActive() {
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload");
        AtomicInteger requests = new AtomicInteger();
        Result result = trackingResult(requests, null,
                row(metadata, Blob.from(Flux.never())),
                row(metadata, Blob.from(Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1})))));

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                            result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 2)
                    .expectSubscription()
                    .then(() -> assertEquals(1, requests.get()))
                    .thenCancel()
                    .verify();
    }

    /** 活动 LOB 期间驱动失败必须立即取消 LOB，并恢复错误图中的原始 JVM fatal。 */
    @Test
    void interruptsActiveLargeObjectWhenDriverFails() {
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload");
        AtomicBoolean lobCancelled = new AtomicBoolean();
        OutOfMemoryError fatal = new OutOfMemoryError("driver fatal");
        Result result = trackingResult(new AtomicInteger(), new IllegalStateException("wrapper", fatal),
                row(metadata, Blob.from(Flux.<java.nio.ByteBuffer>never()
                        .doOnCancel(() -> lobCancelled.set(true)))));

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                            result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 1)
                    .expectErrorSatisfies(error -> assertSame(fatal, error))
                    .verify();

        assertTrue(lobCancelled.get());
    }

    /** onNext 内的非法 demand 只能在当前 onNext 返回后发送 onError。 */
    @Test
    void serializesInvalidDemandFailureAfterCurrentRow() {
        RowMetadata metadata = metadata("id");
        AtomicBoolean inOnNext = new AtomicBoolean();
        AtomicBoolean overlappingError = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        Flux.from(R2dbcLargeObjectRows.map(
                result(row(metadata, 1L)), SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope()))
            .subscribe(new org.reactivestreams.Subscriber<>() {
                private org.reactivestreams.Subscription subscription;

                @Override
                public void onSubscribe(org.reactivestreams.Subscription current) {
                    subscription = current;
                    current.request(1);
                }

                @Override
                public void onNext(DynamicRow ignored) {
                    inOnNext.set(true);
                    subscription.request(0);
                    inOnNext.set(false);
                }

                @Override
                public void onError(Throwable error) {
                    overlappingError.set(inOnNext.get());
                    failure.set(error);
                }

                @Override
                public void onComplete() {
                }
            });

        assertTrue(failure.get() instanceof IllegalArgumentException);
        assertTrue(!overlappingError.get());
    }

    /** 分段映射必须保持驱动行顺序，并只按下游 demand 交付结果。 */
    @Test
    void preservesDriverRowOrderAcrossDemandBoundaries() {
        RowMetadata metadata = metadata("id", "name");
        Row first = row(metadata, 1L, "alice");
        Row second = row(metadata, 2L, "bob");
        Result result = result(first, second);

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                            result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 0)
                    .expectSubscription()
                    .thenRequest(1)
                    .assertNext(mapped -> assertEquals(1L, mapped.get("id")))
                    .thenRequest(1)
                    .assertNext(mapped -> assertEquals(2L, mapped.get("id")))
                    .verifyComplete();
    }

    /** 当前 LOB 尚未物化完成时，后续行的 LOB 不能被并行订阅。 */
    @Test
    void doesNotSubscribeNextLargeObjectWhileCurrentRowIsActive() {
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload");
        Row first = row(metadata, Blob.from(Flux.never()));
        AtomicInteger subscriptions = new AtomicInteger();
        Row second = row(metadata, Blob.from(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1}));
        })));
        Result result = result(first, second);

        StepVerifier.create(Flux.from(R2dbcLargeObjectRows.map(
                    result, SqlExecutionOptions.safeDefaults(), new R2dbcLargeObjectScope())), 2)
                    .expectSubscription()
                    .then(() -> assertEquals(0, subscriptions.get()))
                    .thenCancel()
                    .verify();
    }

    /** 首个 LOB 失败时，后续尚未订阅的 locator 必须先 discard 再把原错误出站。 */
    @Test
    void discardsPendingLargeObjectsWhenAnEarlierStreamFails() {
        AssertionError primary = new AssertionError("lob read failed");
        AtomicInteger discards = new AtomicInteger();
        DynamicRow row = row(Blob.from(Flux.error(primary)), trackingClob(discards));

        StepVerifier.create(R2dbcLargeObjectRows.materialize(row, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> assertSame(primary, error))
                    .verify();

        assertEquals(1, discards.get());
    }

    /** 下游取消活动 LOB 时，活动流由取消释放，后续未开始 locator 仍须 discard。 */
    @Test
    void discardsPendingLargeObjectsWhenMaterializationIsCancelled() {
        AtomicInteger discards = new AtomicInteger();
        DynamicRow row = row(Blob.from(Flux.never()), trackingClob(discards));

        StepVerifier.create(R2dbcLargeObjectRows.materialize(row, SqlExecutionOptions.safeDefaults()))
                    .thenCancel()
                    .verify();

        assertEquals(1, discards.get());
    }

    /** 后续列读取同步失败时，前面已从驱动取得但尚未订阅的 LOB 也必须释放。 */
    @Test
    void discardsCapturedLargeObjectsWhenReadingALaterColumnFails() {
        AssertionError primary = new AssertionError("later column failed");
        AtomicInteger discards = new AtomicInteger();
        Blob blob = trackingBlob(discards);
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload", "broken");
        Row row = new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                if (index == 0) {
                    return type.cast(blob);
                }
                throw primary;
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                throw new UnsupportedOperationException("indexed row access expected");
            }
        };

        StepVerifier.create(R2dbcLargeObjectRows.map(row, metadata, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> assertSame(primary, error))
                    .verify();

        assertEquals(1, discards.get());
    }

    /** 后续列错误图中的 JVM fatal 必须在已捕获 LOB 释放后恢复为原对象。 */
    @Test
    void promotesNestedPrimaryVirtualMachineErrorAfterCapturedLargeObjectDiscard() {
        OutOfMemoryError fatal = new OutOfMemoryError("row fatal");
        IllegalStateException primary = new IllegalStateException("driver wrapper", fatal);
        AtomicInteger discards = new AtomicInteger();
        Blob blob = trackingBlob(discards);
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload", "broken");
        Row row = failingRow(metadata, blob, primary);

        StepVerifier.create(R2dbcLargeObjectRows.map(row, metadata, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> assertSame(fatal, error))
                    .verify();

        assertEquals(1, discards.get());
    }

    /** 同一 locator 被多个列复用时只能订阅一次，并把同一物化值回填到全部列。 */
    @Test
    void materializesAliasedLargeObjectOnceForEveryColumn() {
        AtomicInteger subscriptions = new AtomicInteger();
        Blob blob = Blob.from(Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1, 2}));
        }));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("first", blob);
        values.put("second", blob);

        StepVerifier.create(R2dbcLargeObjectRows.materialize(
                            DynamicRow.copyOf(values), SqlExecutionOptions.safeDefaults()))
                    .assertNext(row -> {
                        assertEquals(List.of(1, 2), bytes(row.get("first")));
                        assertEquals(List.of(1, 2), bytes(row.get("second")));
                    })
                    .verifyComplete();

        assertEquals(1, subscriptions.get());
    }

    /** 清理本身出现普通错误时仍保留原读取失败，并以无环 suppressed 形式带出清理诊断。 */
    @Test
    void retainsPrimaryFailureWhenCapturedLargeObjectDiscardAlsoFails() {
        AssertionError primary = new AssertionError("later column failed");
        IllegalStateException cleanup = new IllegalStateException("discard failed");
        Blob blob = failingDiscardBlob(cleanup);
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload", "broken");
        Row row = failingRow(metadata, blob, primary);

        StepVerifier.create(R2dbcLargeObjectRows.map(row, metadata, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> {
                        assertSame(primary, error);
                        assertTrue(java.util.Arrays.asList(error.getSuppressed()).contains(cleanup));
                        assertTrue(java.util.Arrays.asList(cleanup.getSuppressed()).stream()
                                                   .noneMatch(candidate -> candidate == primary));
                    })
                    .verify();
    }

    /** 清理异常图中的 JVM 致命错误优先原样传播，同时保留普通读取失败作为诊断。 */
    @Test
    void promotesDiscardVirtualMachineErrorAfterCapturedRowFailure() {
        AssertionError primary = new AssertionError("later column failed");
        OutOfMemoryError fatal = new OutOfMemoryError("discard fatal");
        Blob blob = failingDiscardBlob(new IllegalStateException("discard wrapper", fatal));
        RowMetadata metadata = metadataWithFirstType(R2dbcType.BLOB, "payload", "broken");
        Row row = failingRow(metadata, blob, primary);

        StepVerifier.create(R2dbcLargeObjectRows.map(row, metadata, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> {
                        assertSame(fatal, error);
                        assertTrue(reaches(error, primary));
                        assertTrue(!reaches(primary, error));
                    })
                    .verify();
    }

    /** 活动 LOB 流包装的 JVM fatal 必须恢复为原对象，不能在清理层退化成普通 wrapper。 */
    @Test
    void propagatesVirtualMachineErrorNestedInLargeObjectStreamFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("LOB stream fatal");
        DynamicRow row = row(Blob.from(Flux.error(
                new IllegalStateException("driver wrapper", fatal))), trackingClob(new AtomicInteger()));

        StepVerifier.create(R2dbcLargeObjectRows.materialize(row, SqlExecutionOptions.safeDefaults()))
                    .expectErrorSatisfies(error -> assertSame(fatal, error))
                    .verify();
    }

    private static Result result(Row... rows) {
        return (Result) java.lang.reflect.Proxy.newProxyInstance(
                Result.class.getClassLoader(), new Class<?>[]{Result.class}, (ignored, method, arguments) -> {
                    if ("map".equals(method.getName())) {
                        @SuppressWarnings("unchecked")
                        java.util.function.BiFunction<Row, RowMetadata, Object> mapper =
                                (java.util.function.BiFunction<Row, RowMetadata, Object>) arguments[0];
                        return Flux.fromArray(rows).map(row -> mapper.apply(row, row.getMetadata()));
                    }
                    if ("flatMap".equals(method.getName())) {
                        @SuppressWarnings("unchecked")
                        java.util.function.Function<Result.Segment, Publisher<?>> mapper =
                                (java.util.function.Function<Result.Segment, Publisher<?>>) arguments[0];
                        return Flux.fromArray(rows)
                                   .map(R2dbcLargeObjectRowsTest::segment)
                                   .concatMap(segment -> Flux.from(mapper.apply(segment)));
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result trackingResult(AtomicInteger requests, Throwable terminalFailure, Row... rows) {
        return (Result) java.lang.reflect.Proxy.newProxyInstance(
                Result.class.getClassLoader(), new Class<?>[]{Result.class}, (ignored, method, arguments) -> {
                    if (!"map".equals(method.getName())) {
                        throw new UnsupportedOperationException(method.getName());
                    }
                    @SuppressWarnings("unchecked")
                    java.util.function.BiFunction<Row, RowMetadata, Object> mapper =
                            (java.util.function.BiFunction<Row, RowMetadata, Object>) arguments[0];
                    return (Publisher<Object>) subscriber -> subscriber.onSubscribe(
                            new org.reactivestreams.Subscription() {
                                private int index;
                                private boolean done;

                                @Override
                                public void request(long demand) {
                                    if (done || demand <= 0) {
                                        return;
                                    }
                                    requests.incrementAndGet();
                                    if (index < rows.length) {
                                        Row current = rows[index++];
                                        subscriber.onNext(mapper.apply(current, current.getMetadata()));
                                    }
                                    if (!done && terminalFailure != null) {
                                        done = true;
                                        subscriber.onError(terminalFailure);
                                    } else if (!done && index == rows.length) {
                                        done = true;
                                        subscriber.onComplete();
                                    }
                                }

                                @Override
                                public void cancel() {
                                    done = true;
                                }
                            });
                });
    }

    private static Result.RowSegment segment(Row row) {
        return () -> row;
    }

    private static DynamicRow row(Blob blob, Clob clob) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("payload", blob);
        values.put("content", clob);
        return DynamicRow.copyOf(values);
    }

    private static Clob trackingClob(AtomicInteger discards) {
        return new Clob() {
            @Override
            public Publisher<CharSequence> stream() {
                return Flux.just("unused");
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.fromRunnable(discards::incrementAndGet);
            }
        };
    }

    private static Blob trackingBlob(AtomicInteger discards) {
        return new Blob() {
            @Override
            public Publisher<java.nio.ByteBuffer> stream() {
                return Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1}));
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.fromRunnable(discards::incrementAndGet);
            }
        };
    }

    private static Blob failingDiscardBlob(Throwable failure) {
        return new Blob() {
            @Override
            public Publisher<java.nio.ByteBuffer> stream() {
                return Flux.just(java.nio.ByteBuffer.wrap(new byte[]{1}));
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.error(failure);
            }
        };
    }

    private static Row failingRow(RowMetadata metadata, Blob blob, Throwable failure) {
        return new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                if (index == 0) {
                    return type.cast(blob);
                }
                throwUnchecked(failure);
                throw new AssertionError("unreachable");
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                throw new UnsupportedOperationException("indexed row access expected");
            }
        };
    }

    private static Row row(RowMetadata metadata, Object... values) {
        return new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                return type.cast(values[index]);
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                throw new UnsupportedOperationException("indexed row access expected");
            }
        };
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        throw (RuntimeException) failure;
    }

    private static RowMetadata metadata(String... names) {
        return metadataWithFirstType(R2dbcType.VARCHAR, names);
    }

    private static RowMetadata metadataWithFirstType(R2dbcType firstType, String... names) {
        List<? extends ColumnMetadata> columns = java.util.stream.IntStream.range(0, names.length)
                .mapToObj(index -> new ColumnMetadata() {
            @Override
            public io.r2dbc.spi.Type getType() {
                return index == 0 ? firstType : R2dbcType.VARCHAR;
            }

            @Override
            public String getName() {
                return names[index];
            }
        }).toList();
        return new RowMetadata() {
            @Override
            public ColumnMetadata getColumnMetadata(int index) {
                return columns.get(index);
            }

            @Override
            public ColumnMetadata getColumnMetadata(String name) {
                return columns.stream().filter(column -> column.getName().equals(name)).findFirst().orElseThrow();
            }

            @Override
            public List<? extends ColumnMetadata> getColumnMetadatas() {
                return columns;
            }
        };
    }

    private static List<Integer> bytes(Object value) {
        byte[] bytes = (byte[]) value;
        List<Integer> result = new java.util.ArrayList<>(bytes.length);
        for (byte current : bytes) {
            result.add((int) current);
        }
        return result;
    }

    private static boolean reaches(Throwable start, Throwable expected) {
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        java.util.Set<Throwable> visited = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            if (current.getCause() != null) {
                pending.add(current.getCause());
            }
            java.util.Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }
}
