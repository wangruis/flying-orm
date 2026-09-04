package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the actual Repository/Form/R2DBC chain; only driver SPI interfaces are substituted. */
class ReactiveIndependentBatchLifecycleTest {

    @Test
    void insertSummaryRetiresCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.INSERT, false);
    }

    @Test
    void insertChunksRetireCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.INSERT, true);
    }

    @Test
    void updateSummaryRetiresCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.UPDATE, false);
    }

    @Test
    void updateChunksRetireCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.UPDATE, true);
    }

    @Test
    void upsertSummaryRetiresCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.UPSERT, false);
    }

    @Test
    void upsertChunksRetireCommittedEntitiesBeforeReadingMoreInput() {
        assertRetiredPerChunk(Operation.UPSERT, true);
    }

    @Test
    void summaryCancellationKeepsTheGeneratedKeyOfACommittedAndClosedChunk() {
        assertCancellationKeepsCommittedKey(false);
    }

    @Test
    void chunksCancellationKeepsTheGeneratedKeyOfACommittedAndClosedChunk() {
        assertCancellationKeepsCommittedKey(true);
    }

    @Test
    void generatedKeysWithoutListenerDoNotRetainEarlierCommittedEntities() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<GeneratedEntity> entities = IntStream.rangeClosed(1, 16)
                        .mapToObj(GeneratedEntity::new).toList();

                BatchWriteResult result = execute(fixture.repository(GeneratedEntity.class), Operation.INSERT,
                        chunks, Flux.fromIterable(entities), options()).block(Duration.ofSeconds(2));

                assertCommitted(result, 16);
                assertEquals(IntStream.rangeClosed(1, 16).mapToObj(id -> 1_000L + id).toList(),
                        entities.stream().map(GeneratedEntity::getId).toList());
                fixture.assertClosed(16, 16, 0);
            }
        }
    }

    @Test
    void unknownCommitRestoresOnlyItsGeneratedKeyBeforeTheNextInput() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                fixture.unknownCommitAt = 2;
                List<GeneratedEntity> entities = IntStream.rangeClosed(1, 3)
                        .mapToObj(GeneratedEntity::new).toList();
                AtomicReference<Long> secondKeyBeforeThirdInput = new AtomicReference<>(-1L);
                Flux<GeneratedEntity> input = Flux.range(0, 3).map(index -> {
                    if (index == 2) {
                        secondKeyBeforeThirdInput.set(entities.get(1).getId());
                    }
                    return entities.get(index);
                });

                BatchWriteResult result = execute(fixture.repository(GeneratedEntity.class), Operation.INSERT,
                        chunks, input, options()).block(Duration.ofSeconds(2));

                assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
                assertEquals(3, result.inputCount());
                assertEquals(2L, result.affectedRows());
                assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.UNKNOWN,
                                BatchChunkResult.Status.COMMITTED),
                        result.chunks().stream().map(BatchChunkResult::status).toList());
                assertEquals(1_001L, entities.get(0).getId());
                assertNull(entities.get(1).getId());
                assertEquals(1_003L, entities.get(2).getId());
                assertNull(secondKeyBeforeThirdInput.get(), "UNKNOWN key must leave pending state per chunk");
                fixture.assertClosed(3, 2, 0);
            }
        }
    }

    @Test
    void postFailurePreservesEveryGeneratedKeyAndPostCallbackInTheCommittedChunk() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2));
                IllegalStateException callbackFailure = new IllegalStateException("first POST failed");
                List<Long> posts = new ArrayList<>();
                ReactiveFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                        .withListener(event -> {
                            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                                posts.add(event.entity().getId());
                                fixture.assertClosed(1, 1, 0);
                                if (posts.size() == 1) {
                                    return Mono.error(callbackFailure);
                                }
                            }
                            return Mono.empty();
                        });

                CommittedEntityLifecycleException failure = assertThrows(CommittedEntityLifecycleException.class,
                        () -> execute(repository, Operation.INSERT, chunks, Flux.fromIterable(entities),
                                twoRowOptions()).block(Duration.ofSeconds(2)));

                assertTrue(failure.committed());
                assertSame(callbackFailure, failure.getCause());
                BatchChunkResult committed = assertInstanceOf(BatchChunkResult.class, failure.result());
                assertEquals(BatchChunkResult.Status.COMMITTED, committed.status());
                assertEquals(2, committed.inputCount());
                assertEquals(List.of(1_001L, 1_002L), posts);
                assertEquals(posts, entities.stream().map(GeneratedEntity::getId).toList());
                fixture.assertClosed(1, 1, 0);
            }
        }
    }

    @Test
    void directPostErrorKeepsEveryCommittedKeyAndStopsTheRemainingPostCallbacks() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2));
                LinkageError fatal = new LinkageError("first POST linkage failure after chunk commit");
                List<Long> posts = new ArrayList<>();
                AtomicReference<List<Long>> keysAtFirstPost = new AtomicReference<>();
                AtomicReference<Object> firstPostResult = new AtomicReference<>();
                ReactiveFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                        .withListener(event -> {
                            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                                posts.add(event.entity().getId());
                                keysAtFirstPost.set(entities.stream().map(GeneratedEntity::getId).toList());
                                firstPostResult.set(event.result());
                                fixture.assertClosed(1, 1, 0);
                                throw fatal;
                            }
                            return Mono.empty();
                        });

                assertSame(fatal, assertThrows(LinkageError.class,
                        () -> execute(repository, Operation.INSERT, chunks, Flux.fromIterable(entities),
                                twoRowOptions()).block(Duration.ofSeconds(2))));

                assertEquals(List.of(1_001L, 1_002L), keysAtFirstPost.get());
                BatchChunkResult committed = assertInstanceOf(BatchChunkResult.class, firstPostResult.get());
                assertEquals(BatchChunkResult.Status.COMMITTED, committed.status());
                assertEquals(2, committed.inputCount());
                assertEquals(List.of(1_001L), posts, "direct Error must stop the second POST callback");
                assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList(),
                        "fatal callback failure must not restore any key in the committed chunk");
                fixture.assertClosed(1, 1, 0);
            }
        }
    }

    @Test
    void cancellationDuringPostKeepsEveryGeneratedKeyFromTheCommittedChunk() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2));
                List<Long> posts = new ArrayList<>();
                AtomicBoolean postCancelled = new AtomicBoolean();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                ReactiveFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                        .withListener(event -> {
                            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                                posts.add(event.entity().getId());
                                return Mono.<Void>never().doOnCancel(() -> postCancelled.set(true));
                            }
                            return Mono.empty();
                        });
                Disposable pending = execute(repository, Operation.INSERT, chunks,
                        Flux.fromIterable(entities), twoRowOptions()).subscribe(ignored -> { }, failure::set);
                try {
                    fixture.assertClosed(1, 1, 0);
                    assertEquals(List.of(1_001L), posts);
                    assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList());

                    pending.dispose();

                    assertTrue(postCancelled.get());
                    assertNull(failure.get());
                    assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList(),
                            "every key in a confirmed chunk must survive cancellation during its first POST");
                    fixture.assertClosed(1, 1, 0);
                } finally {
                    pending.dispose();
                }
            }
        }
    }

    @Test
    void cancellationDuringPostKeepsKeysOfOtherConfirmedConcurrentChunks() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2));
                List<Long> posts = new ArrayList<>();
                AtomicBoolean postCancelled = new AtomicBoolean();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                ReactiveFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                        .withListener(event -> {
                            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                                posts.add(event.entity().getId());
                                return Mono.<Void>never().doOnCancel(() -> postCancelled.set(true));
                            }
                            return Mono.empty();
                        });
                BatchWriteOptions concurrent = BatchWriteOptions.independent(1, 2)
                        .withMemoryLimits(32, 4_096L, 32).withMaxRowBytes(256L);
                Disposable pending = execute(repository, Operation.INSERT, chunks,
                        Flux.fromIterable(entities), concurrent).subscribe(ignored -> { }, failure::set);
                try {
                    fixture.assertClosed(2, 2, 0);
                    assertEquals(List.of(1_001L), posts, "later POST waits behind the first pending callback");
                    assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList());

                    pending.dispose();

                    assertTrue(postCancelled.get());
                    assertNull(failure.get());
                    assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList(),
                            "confirmed sibling keys must survive cancellation before their queued POST runs");
                    fixture.assertClosed(2, 2, 0);
                } finally {
                    pending.dispose();
                }
            }
        }
    }

    @Test
    void cancellationWhileCloseIsPendingKeepsAConfirmedGeneratedKey() {
        for (boolean chunks : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                fixture.heldCloseAt = 1;
                GeneratedEntity entity = new GeneratedEntity(1);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                List<Long> posts = new ArrayList<>();
                ReactiveFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                        .withListener(event -> {
                            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                                posts.add(event.entity().getId());
                            }
                            return Mono.empty();
                        });
                Disposable pending = execute(repository, Operation.INSERT, chunks, Flux.just(entity), options())
                        .subscribe(ignored -> { }, failure::set);
                try {
                    assertEquals(1, fixture.commits);
                    assertEquals(1, fixture.closeStarted);
                    assertEquals(0, fixture.closed, "the close publisher must still be pending");
                    assertTrue(posts.isEmpty(), "POST cannot hold a connection lease");
                    assertEquals(1_001L, entity.getId());

                    pending.dispose();

                    assertNull(failure.get());
                    assertEquals(1_001L, entity.getId(), "close completion does not own the commit truth");
                    assertEquals(0, fixture.rollbacks);
                    fixture.closeRelease.tryEmitEmpty();
                    fixture.assertClosed(1, 1, 0);
                    assertTrue(posts.isEmpty(), "cancelled callbacks must not restart when close finishes");
                    assertEquals(1_001L, entity.getId());
                } finally {
                    pending.dispose();
                }
            }
        }
    }

    @Test
    void summaryContinuesInputAfterOrdinaryPostFailureWithoutRewritingCommitTruth() {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            IllegalStateException callbackFailure = new IllegalStateException("first POST failed");
            List<PostObservation> posts = new ArrayList<>();
            ReactiveFormRepository<PayloadEntity> repository = failingFirstPostRepository(
                    fixture, callbackFailure, posts);

            CommittedEntityLifecycleException failure = assertThrows(CommittedEntityLifecycleException.class,
                    () -> repository.insertBatch(payloadInput(fixture, 8), options()).block(Duration.ofSeconds(2)));

            assertTrue(failure.committed());
            assertSame(callbackFailure, failure.getCause());
            BatchChunkResult committed = assertInstanceOf(BatchChunkResult.class, failure.result());
            assertEquals(BatchChunkResult.Status.COMMITTED, committed.status());
            assertEquals(IntStream.rangeClosed(1, 8).mapToObj(id -> new PostObservation(id, id, 0, id)).toList(), posts);
            fixture.assertClosed(8, 8, 0);
        }
    }

    @Test
    void chunksStopLaterInputAfterOrdinaryPostFailureWithoutRewritingCommitTruth() {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            IllegalStateException callbackFailure = new IllegalStateException("first POST failed");
            List<PostObservation> posts = new ArrayList<>();
            AtomicBoolean inputCancelled = new AtomicBoolean();
            ReactiveFormRepository<PayloadEntity> repository = failingFirstPostRepository(
                    fixture, callbackFailure, posts);
            Flux<PayloadEntity> input = payloadInput(fixture, 3)
                    .doOnCancel(() -> inputCancelled.set(true));

            CommittedEntityLifecycleException failure = assertThrows(CommittedEntityLifecycleException.class,
                    () -> repository.insertBatchChunks(input, options()).collectList().block(Duration.ofSeconds(2)));

            assertTrue(failure.committed());
            assertSame(callbackFailure, failure.getCause());
            BatchChunkResult committed = assertInstanceOf(BatchChunkResult.class, failure.result());
            assertEquals(BatchChunkResult.Status.COMMITTED, committed.status());
            assertEquals(1, committed.inputCount());
            assertEquals(List.of(new PostObservation(1, 1, 0, 1)), posts);
            assertEquals(1, fixture.inputs, "chunk-stream POST failure must not consume the next row");
            assertTrue(inputCancelled.get());
            fixture.assertClosed(1, 1, 0);
        }
    }

    @Test
    void summaryKeepsLaterInputFailurePrimaryAndSuppressesEarlierPostFailure() {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            IllegalStateException callbackFailure = new IllegalStateException("first POST failed");
            IllegalArgumentException inputFailure = new IllegalArgumentException("input failed after row two");
            List<PostObservation> posts = new ArrayList<>();
            ReactiveFormRepository<PayloadEntity> repository = failingFirstPostRepository(
                    fixture, callbackFailure, posts);
            Flux<PayloadEntity> input = payloadInput(fixture, 2).concatWith(Flux.error(inputFailure));

            BatchWriteException failure = assertThrows(BatchWriteException.class,
                    () -> repository.insertBatch(input, options()).block(Duration.ofSeconds(2)));

            assertSame(inputFailure, failure.getCause());
            assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.COMMITTED,
                            BatchChunkResult.Status.FAILED),
                    failure.result().chunks().stream().map(BatchChunkResult::status).toList());
            List<CommittedEntityLifecycleException> suppressed = Arrays.stream(failure.getSuppressed())
                    .filter(CommittedEntityLifecycleException.class::isInstance)
                    .map(CommittedEntityLifecycleException.class::cast).toList();
            assertEquals(1, suppressed.size());
            assertSame(callbackFailure, suppressed.getFirst().getCause());
            assertEquals(List.of(new PostObservation(1, 1, 0, 1), new PostObservation(2, 2, 0, 2)), posts);
            fixture.assertClosed(2, 2, 0);
        }
    }

    private static ReactiveFormRepository<PayloadEntity> failingFirstPostRepository(
            R2dbcFixture fixture, RuntimeException failure, List<PostObservation> posts) {
        return fixture.repository(PayloadEntity.class).withListener(event -> {
            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                posts.add(new PostObservation(event.entity().getId(), fixture.commits,
                        fixture.acquired - fixture.closed, fixture.inputs));
                if (event.entity().getId() == 1L) {
                    return Mono.error(failure);
                }
            }
            return Mono.empty();
        });
    }

    private static Flux<PayloadEntity> payloadInput(R2dbcFixture fixture, int count) {
        return Flux.range(1, count).map(id -> {
            fixture.inputs++;
            return new PayloadEntity(id);
        });
    }

    private static void assertRetiredPerChunk(Operation operation, boolean chunks) {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            EntityLifecyclePhase post = operation == Operation.UPDATE
                    ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
            List<PostObservation> posts = new ArrayList<>();
            ReactiveFormRepository<PayloadEntity> repository = fixture.repository(PayloadEntity.class)
                    .withListener(event -> {
                        if (event.phase() == post) {
                            posts.add(new PostObservation(event.entity().getId(), fixture.commits,
                                    fixture.acquired - fixture.closed, fixture.inputs));
                        }
                        return Mono.empty();
                    });
            BatchWriteResult result = execute(repository, operation, chunks, payloadInput(fixture, 16), options())
                    .block(Duration.ofSeconds(2));

            assertCommitted(result, 16);
            assertEquals(IntStream.rangeClosed(1, 16)
                            .mapToObj(id -> new PostObservation(id, id, 0, id)).toList(),
                    posts, "POST must run after its connection closes and before consuming the next row");
            fixture.assertClosed(16, 16, 0);
        }
    }

    private static void assertCancellationKeepsCommittedKey(boolean chunks) {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            GeneratedEntity entity = new GeneratedEntity(1);
            AtomicBoolean inputCancelled = new AtomicBoolean();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicReference<BatchWriteResult> unexpectedResult = new AtomicReference<>();
            Flux<GeneratedEntity> input = Flux.just(entity).concatWith(Flux.never())
                    .doOnCancel(() -> inputCancelled.set(true));
            Disposable pending = execute(fixture.repository(GeneratedEntity.class), Operation.INSERT,
                    chunks, input, options()).subscribe(unexpectedResult::set, failure::set);
            try {
                fixture.assertClosed(1, 1, 0);
                assertEquals(1_001L, entity.getId(), "cancellation must follow confirmed commit and close");
                assertNull(unexpectedResult.get(), "the summary must still be waiting for more input");

                pending.dispose();

                assertTrue(inputCancelled.get());
                assertNull(failure.get());
                assertEquals(1_001L, entity.getId(), "abort must preserve the already committed key");
                fixture.assertClosed(1, 1, 0);
            } finally {
                pending.dispose();
            }
        }
    }

    private static <T> Mono<BatchWriteResult> execute(ReactiveFormRepository<T> repository,
                                                     Operation operation,
                                                     boolean chunks,
                                                     Publisher<T> input,
                                                     BatchWriteOptions options) {
        if (chunks) {
            Flux<BatchChunkResult> results = switch (operation) {
                case INSERT -> repository.insertBatchChunks(input, options);
                case UPDATE -> repository.updateBatchChunks(input, options);
                case UPSERT -> repository.upsertBatchChunks(input, options);
            };
            return results.collectList().map(values -> BatchWriteResult.from(options.mode(), values));
        }
        return switch (operation) {
            case INSERT -> repository.insertBatch(input, options);
            case UPDATE -> repository.updateBatch(input, options);
            case UPSERT -> repository.upsertBatch(input, options);
        };
    }

    private static BatchWriteOptions options() {
        return BatchWriteOptions.independent(1, 1)
                .withMemoryLimits(32, 4_096L, 32).withMaxRowBytes(256L);
    }

    private static BatchWriteOptions twoRowOptions() {
        return BatchWriteOptions.independent(2, 1)
                .withMemoryLimits(32, 4_096L, 32).withMaxRowBytes(256L);
    }

    private static void assertCommitted(BatchWriteResult result, int rows) {
        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(rows, result.inputCount());
        assertEquals(rows, result.affectedRows());
        assertEquals(rows, result.chunks().size());
        for (int index = 0; index < rows; index++) {
            BatchChunkResult chunk = result.chunks().get(index);
            assertEquals(index, chunk.chunkIndex());
            assertEquals(index, chunk.startOffset());
            assertEquals(1, chunk.inputCount());
            assertEquals(1L, chunk.affectedRows());
            assertEquals(BatchChunkResult.Status.COMMITTED, chunk.status());
        }
    }

    private enum Operation { INSERT, UPDATE, UPSERT }

    private record PostObservation(long id, int commits, int activeLeases, int inputs) { }

    private static final class R2dbcFixture implements ConnectionFactory, AutoCloseable {
        private final ReactiveFormClient client = ReactiveFormClient.create(R2dbcSqlExecutor.create(this),
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()));
        private int inputs;
        private int acquired;
        private int closed;
        private int commits;
        private int rollbacks;
        private int executions;
        private int commitCalls;
        private int unknownCommitAt;
        private int closeStarted;
        private int heldCloseAt;
        private final Sinks.Empty<Void> closeRelease = Sinks.empty();

        private <T> ReactiveFormRepository<T> repository(Class<T> type) {
            return ReactiveFormRepository.create(client, client.entityModels().metadata(type).toDynamicForm(), type);
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "H2";
        }

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.fromSupplier(() -> {
                int connectionIndex = ++acquired;
                return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement();
                    case "isAutoCommit" -> true;
                    case "setAutoCommit" -> Mono.empty();
                    case "beginTransaction" -> Mono.empty();
                    case "commitTransaction" -> Mono.fromRunnable(() -> {
                        if (++commitCalls == unknownCommitAt) {
                            throw new IllegalStateException("commit result unknown");
                        }
                        commits++;
                    });
                    case "rollbackTransaction" -> Mono.fromRunnable(() -> rollbacks++);
                    case "close" -> Mono.defer(() -> {
                        closeStarted++;
                        Mono<Void> release = connectionIndex == heldCloseAt
                                ? closeRelease.asMono() : Mono.empty();
                        return release.then(Mono.fromRunnable(() -> closed++));
                    });
                    default -> throw new AssertionError("unexpected connection call " + method.getName());
                });
            });
        }

        private Statement statement() {
            AtomicBoolean generated = new AtomicBoolean();
            return proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "bind", "bindNull", "add", "fetchSize" -> self;
                case "returnGeneratedValues" -> {
                    generated.set(true);
                    yield self;
                }
                case "execute" -> {
                    long key = 1_000L + ++executions;
                    yield Flux.just(generated.get() ? keyResult(key) : countResult());
                }
                default -> throw new AssertionError("unexpected statement call " + method.getName());
            });
        }

        private static Result countResult() {
            return proxy(Result.class, (self, method, arguments) -> {
                if (method.getName().equals("getRowsUpdated")) {
                    return Mono.just(1L);
                }
                throw new AssertionError("unexpected result call " + method.getName());
            });
        }

        @SuppressWarnings("unchecked")
        private static Result keyResult(long key) {
            ColumnMetadata column = proxy(ColumnMetadata.class, (self, method, arguments) -> switch (method.getName()) {
                case "getName" -> "id";
                case "getType" -> R2dbcType.BIGINT;
                case "getJavaType" -> Long.class;
                default -> throw new AssertionError("unexpected column call " + method.getName());
            });
            RowMetadata metadata = proxy(RowMetadata.class, (self, method, arguments) -> switch (method.getName()) {
                case "getColumnMetadatas" -> List.of(column);
                case "getColumnMetadata" -> column;
                default -> throw new AssertionError("unexpected metadata call " + method.getName());
            });
            Row row = proxy(Row.class, (self, method, arguments) -> switch (method.getName()) {
                case "getMetadata" -> metadata;
                case "get" -> key;
                default -> throw new AssertionError("unexpected row call " + method.getName());
            });
            Result.RowSegment segment = () -> row;
            return proxy(Result.class, (self, method, arguments) -> {
                if (method.getName().equals("flatMap")) {
                    Function<Result.Segment, Publisher<Object>> mapping =
                            (Function<Result.Segment, Publisher<Object>>) arguments[0];
                    return Flux.from(mapping.apply(segment));
                }
                throw new AssertionError("unexpected result call " + method.getName());
            });
        }

        private void assertClosed(int expectedConnections, int expectedCommits, int expectedRollbacks) {
            assertEquals(expectedConnections, acquired);
            assertEquals(expectedConnections, closed);
            assertEquals(expectedCommits, commits);
            assertEquals(expectedRollbacks, rollbacks);
        }

        @Override
        public void close() {
            closeRelease.tryEmitEmpty();
            client.entityModels().close();
        }
    }

    @TableName("reactive_independent_payload")
    private static final class PayloadEntity {
        @TableId(type = IdType.INPUT)
        private final Long id;
        @Version
        private final Long version = 1L;
        private final String name;
        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private final byte[] lifecycleOnly = new byte[1_024];

        private PayloadEntity(int id) {
            this.id = (long) id;
            this.name = "entity-" + id;
        }

        public Long getId() { return id; }
        public Long getVersion() { return version; }
        public String getName() { return name; }
        public byte[] getLifecycleOnly() { return lifecycleOnly; }
    }

    @TableName("reactive_independent_generated")
    private static final class GeneratedEntity {
        @TableId(type = IdType.AUTO)
        private Long id;
        private final String name;
        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private final byte[] lifecycleOnly = new byte[1_024];

        private GeneratedEntity(int id) {
            this.name = "generated-" + id;
        }

        public Long getId() { return id; }
        public String getName() { return name; }
        public byte[] getLifecycleOnly() { return lifecycleOnly; }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
