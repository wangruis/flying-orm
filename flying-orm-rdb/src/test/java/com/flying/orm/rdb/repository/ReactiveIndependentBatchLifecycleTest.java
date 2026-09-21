package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.EntityPostWriteException;
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
    void ordinaryUpdatePostsEverySparseSuccessfulEntityButNotZeroCountEntity() {
        assertSparseUpdatePosts(0, 0, false);
    }

    @Test
    void ordinaryUpdatePostsEverySparseSuccessfulEntityButNotTwoCountEntity() {
        assertSparseUpdatePosts(0, 2, false);
    }

    @Test
    void ordinaryUpdateKeepsAbsoluteSparsePostsAndSqlFailureWhenOnePostFails() {
        assertSparseUpdatePosts(3, 0, true);
    }

    private void assertSparseUpdatePosts(int prefix, int conflictCount, boolean failPost) {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            fixture.rowCounts = index -> index == prefix + 1 ? conflictCount : 1;
            List<PayloadEntity> entities = IntStream.range(0, prefix + 6).mapToObj(PayloadEntity::new).toList();
            List<PayloadEntity> posts = new ArrayList<>();
            IllegalStateException listenerFailure = new IllegalStateException("sparse POST failure");
            var repository = fixture.repository(PayloadEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.POST_UPDATE) {
                    posts.add(event.entity());
                    assertEquals(0, fixture.closed, "POST runs before the single upper release");
                    if (failPost && event.entity().id == prefix) return Mono.error(listenerFailure);
                }
                return Mono.empty();
            });
            var failure = assertThrows(com.flying.orm.rdb.batch.BatchExecutionEvidenceException.class,
                    () -> repository.updateBatch(Flux.fromIterable(entities), BatchWriteOptions.of(3)).block(Duration.ofSeconds(3)));
            var optimistic = assertInstanceOf(IllegalStateException.class,
                    failure.getCause());
            assertEquals("batch row count did not match exactly-one policy", optimistic.getMessage());
            assertEquals(prefix + 1L, failure.evidence().conflicts().getFirst().inputOffset());
            assertEquals(1, failure.evidence().conflicts().size());
            long[] expected = IntStream.range(0, prefix + 3).filter(i -> i != prefix + 1).asLongStream().toArray();
            org.junit.jupiter.api.Assertions.assertArrayEquals(expected, failure.evidence().successfulOffsets().toArray());
            org.junit.jupiter.api.Assertions.assertArrayEquals(new long[]{prefix + 1L},
                    failure.evidence().failedOffsets().toArray());
            assertEquals(prefix + 3L, failure.evidence().inputCount(), "do not consume the next window");
            assertEquals(prefix + 3, fixture.executions);
            assertEquals(1, fixture.acquired);
            assertEquals(1, fixture.closed);
            for (String sql : fixture.sql) {
                String normalized = sql.toLowerCase(java.util.Locale.ROOT);
                assertTrue(normalized.contains(" where "), "entity update must retain its predicate");
                assertTrue(normalized.substring(normalized.indexOf(" where ")).contains("version"),
                        "the actual driver SQL must include the entity version predicate");
            }
            assertEquals(java.util.Arrays.stream(expected).boxed().toList(), posts.stream().map(PayloadEntity::getId).toList());
            for (PayloadEntity post : posts) assertSame(entities.get(post.id.intValue()), post);
            if (failPost) {
                EntityPostWriteException postFailure = sparsePostFailure(failure,
                        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
                org.junit.jupiter.api.Assertions.assertNotNull(postFailure);
                assertSame(listenerFailure, postFailure.getCause());
                assertEquals(EntityLifecyclePhase.POST_UPDATE, postFailure.phase());
            }
        }
    }

    private static EntityPostWriteException sparsePostFailure(Throwable failure, java.util.Set<Throwable> seen) {
        assertTrue(seen.add(failure), "failure attachment must be acyclic");
        EntityPostWriteException found = failure instanceof EntityPostWriteException post ? post : null;
        if (failure.getCause() != null) {
            EntityPostWriteException nested = sparsePostFailure(failure.getCause(), seen);
            if (nested != null) found = nested;
        }
        for (Throwable suppressed : failure.getSuppressed()) {
            EntityPostWriteException nested = sparsePostFailure(suppressed, seen);
            if (nested != null) found = nested;
        }
        seen.remove(failure);
        return found;
    }

    @Test
    void preRunsBeforeParametersAreReadOnBothEntryNames() {
        for (boolean evidence : new boolean[]{false, true}) try (R2dbcFixture fixture = new R2dbcFixture()) {
            var repository = fixture.repository(GeneratedEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.PRE_PERSIST) event.entity().name = "prepared";
                return Mono.empty();
            });
            execute(repository, Operation.INSERT, evidence, Flux.just(new GeneratedEntity(1)), options())
                    .block(Duration.ofSeconds(3));
            assertTrue(fixture.boundValues.contains("prepared"));
            org.junit.jupiter.api.Assertions.assertFalse(fixture.boundValues.contains("generated-1"));
        }
    }

    @Test
    void ordinaryAndEvidenceShareBoundedPrePostForEveryOperation() {
        for (Operation operation : Operation.values()) for (boolean evidence : new boolean[]{false, true}) {
            try (R2dbcFixture fixture = new R2dbcFixture()) {
                List<Long> posts = new ArrayList<>();
                var repository = fixture.repository(PayloadEntity.class).withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST || event.phase() == EntityLifecyclePhase.POST_UPDATE) {
                        posts.add(event.entity().id);
                        assertEquals(fixture.inputs, posts.size(), "bufferSize=1 POST precedes the next input");
                        assertEquals(1, fixture.acquired);
                        assertEquals(0, fixture.closed, "upper connection is released after the whole batch");
                    }
                    return Mono.empty();
                });
                Flux<PayloadEntity> rows = Flux.range(1, 16).map(index -> {
                    fixture.inputs++;
                    return new PayloadEntity(index);
                });
                var result = execute(repository, operation, evidence, rows, options()).block(Duration.ofSeconds(3));
                assertEquals(16, result.successfulCount());
                assertEquals(16, posts.size());
                fixture.assertClosed(1, 0, 0);
            }
        }
    }

    @Test
    void generatedKeysHaveOrdinaryEvidenceParityAndDoNotAccumulateRetention() {
        for (boolean evidence : new boolean[]{false, true}) try (R2dbcFixture fixture = new R2dbcFixture()) {
            List<GeneratedEntity> entities = IntStream.rangeClosed(1, 16).mapToObj(GeneratedEntity::new).toList();
            var result = execute(fixture.repository(GeneratedEntity.class), Operation.INSERT, evidence,
                    Flux.fromIterable(entities), options()).block(Duration.ofSeconds(3));
            assertEquals(16, result.successfulCount());
            assertEquals(IntStream.rangeClosed(1, 16).mapToObj(id -> 1_000L + id).toList(),
                    entities.stream().map(GeneratedEntity::getId).toList());
            fixture.assertClosed(1, 0, 0);
        }
    }

    @Test
    void postFailureKeepsPrimaryCauseKeysAndOtherCompletedCallbacks() {
        for (boolean evidence : new boolean[]{false, true}) try (R2dbcFixture fixture = new R2dbcFixture()) {
            List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2), new GeneratedEntity(3));
            List<Long> posts = new ArrayList<>();
            IllegalStateException cause = new IllegalStateException("POST failed");
            var repository = fixture.repository(GeneratedEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                    posts.add(event.entity().getId());
                    if (posts.size() == 1) return Mono.error(cause);
                }
                return Mono.empty();
            });
            EntityPostWriteException failure = assertThrows(EntityPostWriteException.class,
                    () -> execute(repository, Operation.INSERT, evidence, Flux.fromIterable(entities),
                            BatchWriteOptions.of(2)).block(Duration.ofSeconds(3)));
            assertSame(cause, failure.getCause());
            assertEquals(EntityLifecyclePhase.POST_PERSIST, failure.phase());
            assertEquals(List.of(1_001L, 1_002L), posts);
            assertEquals(1_001L, entities.get(0).id);
            assertEquals(1_002L, entities.get(1).id);
            assertNull(entities.get(2).id);
            fixture.assertClosed(1, 0, 0);
        }
    }

    @Test
    void cancellationDuringPostPreservesAppliedKeyAndReleasesConnection() {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            GeneratedEntity entity = new GeneratedEntity(1);
            Sinks.Empty<Void> waiting = Sinks.empty();
            AtomicBoolean postStarted = new AtomicBoolean();
            var repository = fixture.repository(GeneratedEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                    postStarted.set(true);
                    return waiting.asMono();
                }
                return Mono.empty();
            });
            Disposable subscription = RepositoryBatchEvidenceAccess.reactive(repository).insertEvidence(Flux.just(entity), options()).subscribe();
            assertTrue(postStarted.get());
            subscription.dispose();
            assertEquals(1_001L, entity.id);
            fixture.assertClosed(1, 0, 0);
        }
    }

    @Test
    void batchIsColdAndCreatesNewTrackerForEachSubscription() {
        try (R2dbcFixture fixture = new R2dbcFixture()) {
            var repository = fixture.repository(PayloadEntity.class);
            Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> work = RepositoryBatchEvidenceAccess.reactive(repository).insertEvidence(
                    Flux.just(new PayloadEntity(1)), options());
            assertEquals(0, fixture.acquired);
            assertEquals(1, work.block(Duration.ofSeconds(3)).successfulCount());
            assertEquals(1, work.block(Duration.ofSeconds(3)).successfulCount());
            fixture.assertClosed(2, 0, 0);
        }
    }

    private static <T> Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> execute(
            ReactiveFormRepository<T> repository, Operation operation, boolean evidence,
            Flux<T> rows, BatchWriteOptions options) {
        return switch (operation) {
            case INSERT -> evidence ? RepositoryBatchEvidenceAccess.reactive(repository).insertEvidence(rows, options) : repository.insertBatch(rows, options);
            case UPDATE -> evidence ? RepositoryBatchEvidenceAccess.reactive(repository).updateEvidence(rows, options) : repository.updateBatch(rows, options);
            case UPSERT -> evidence ? RepositoryBatchEvidenceAccess.reactive(repository).upsertEvidence(rows, options) : repository.upsertBatch(rows, options);
        };
    }

    private static BatchWriteOptions options() {
        return BatchWriteOptions.of(1).withMemoryLimits(32, 4_096L).withMaxRowBytes(256L);
    }

    private enum Operation { INSERT, UPDATE, UPSERT }

    private record PostObservation(long id, int commits, int activeLeases, int inputs) { }

    private static final class R2dbcFixture implements com.flying.orm.rdb.reactive.R2dbcConnectionAccess, AutoCloseable {
        private final List<Object> boundValues = new ArrayList<>();
        private final List<String> sql = new ArrayList<>();
        private final ReactiveFormClient client = ReactiveFormClient.create(R2dbcSqlExecutor.create(this, RdbDialect.h2()),
                FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()));
        private int inputs;
        private int acquired;
        private int closed;
        private int commits;
        private int rollbacks;
        private int executions;
        private java.util.function.IntUnaryOperator rowCounts = index -> 1;
        private int commitCalls;
        private int unknownCommitAt;
        private int closeStarted;
        private int heldCloseAt;
        private final Sinks.Empty<Void> closeRelease = Sinks.empty();

        private <T> ReactiveFormRepository<T> repository(Class<T> type) {
            return ReactiveFormRepository.create(client, client.entityModels().metadata(type).toDynamicForm(), type);
        }


        @Override
        public Publisher<? extends Connection> getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
            return Mono.fromSupplier(() -> {
                int connectionIndex = ++acquired;
                return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> {
                        sql.add((String) arguments[0]);
                        yield statement();
                    }
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

        @Override
        public Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal, Connection connection,
                com.flying.orm.core.sql.render.SqlRequest request) {
            return connection.close();
        }

        private Statement statement() {
            AtomicBoolean generated = new AtomicBoolean();
            return proxy(Statement.class, (self, method, arguments) -> switch (method.getName()) {
                case "bind" -> {
                    boundValues.add(arguments[1]);
                    yield self;
                }
                case "bindNull", "add", "fetchSize" -> self;
                case "returnGeneratedValues" -> {
                    generated.set(true);
                    yield self;
                }
                case "execute" -> {
                    long key = 1_000L + ++executions;
                    yield Flux.just(generated.get() ? keyResult(key) : countResult(rowCounts.applyAsInt(executions - 1)));
                }
                default -> throw new AssertionError("unexpected statement call " + method.getName());
            });
        }

        private static Result countResult(long count) {
            return proxy(Result.class, (self, method, arguments) -> {
                if (method.getName().equals("getRowsUpdated")) {
                    return Mono.just(count);
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
        private String name;
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
