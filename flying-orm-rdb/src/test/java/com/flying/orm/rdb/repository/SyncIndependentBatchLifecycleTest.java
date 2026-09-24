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
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.lifecycle.EntityPostWriteException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the actual Repository/Form/JDBC chain; only JDBC driver interfaces are substituted. */
class SyncIndependentBatchLifecycleTest {

    @Test
    void asyncPreFailureCancelsInputBeforeOpeningAConnection() {
        JdbcFixture fixture = new JdbcFixture();
        IllegalStateException expected = new IllegalStateException("PRE failure");
        AtomicInteger cancelled = new AtomicInteger();
        SyncFormRepository<PayloadEntity> repository = fixture.repository(PayloadEntity.class)
                .withListener(event -> Mono.error(expected));
        Flux<PayloadEntity> source = Flux.just(new PayloadEntity(1), new PayloadEntity(2))
                .publishOn(reactor.core.scheduler.Schedulers.parallel(), 1)
                .doOnCancel(cancelled::incrementAndGet);
        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> repository.insertBatch(source, options()));
        assertSame(expected, failure);
        assertEquals(1, cancelled.get());
        assertEquals(0, fixture.acquired);
        assertEquals(0, fixture.executions);
    }

    @Test
    void asyncInputAndAsyncPreListenerRemainNonBlockingAndOrdered() {
        for (Operation operation : Operation.values()) {
            JdbcFixture fixture = new JdbcFixture();
            List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();
            Thread caller = Thread.currentThread();
            SyncFormRepository<PayloadEntity> repository = fixture.repository(PayloadEntity.class)
                    .withListener(event -> {
                        if (event.phase() == EntityLifecyclePhase.PRE_PERSIST
                                || event.phase() == EntityLifecyclePhase.PRE_UPDATE) {
                            return Mono.<Void>fromRunnable(() -> events.add("pre:" + event.entity().id))
                                    .subscribeOn(reactor.core.scheduler.Schedulers.parallel());
                        }
                        assertSame(caller, Thread.currentThread(), "JDBC completion remains on its caller thread");
                        events.add("post:" + event.entity().id);
                        return Mono.empty();
                    });
            Flux<PayloadEntity> source = Flux.just(new PayloadEntity(1), new PayloadEntity(2))
                    .publishOn(reactor.core.scheduler.Schedulers.parallel(), 1);
            assertEquals(2, execute(repository, operation, false, source, options()).successfulCount());
            assertEquals(List.of("pre:1", "post:1", "pre:2", "post:2"), events);
            fixture.assertClosed(2, 0, 0);
        }
    }

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
        JdbcFixture fixture = new JdbcFixture();
        fixture.rowCounts = index -> index == prefix + 1 ? conflictCount : 1;
        List<PayloadEntity> entities = IntStream.range(0, prefix + 6).mapToObj(PayloadEntity::new).toList();
        List<PayloadEntity> posts = new ArrayList<>();
        IllegalStateException listenerFailure = new IllegalStateException("sparse POST failure");
        var repository = fixture.repository(PayloadEntity.class).withListener(event -> {
            if (event.phase() == EntityLifecyclePhase.POST_UPDATE) {
                posts.add(event.entity());
                assertEquals(0, fixture.closed, "POST runs before the single upper release");
                assertEquals(fixture.prepared, fixture.statementsClosed, "statement cleanup precedes POST");
                if (failPost && event.entity().id == prefix) return Mono.error(listenerFailure);
            }
            return Mono.empty();
        });
        var failure = assertThrows(com.flying.orm.rdb.batch.BatchExecutionEvidenceException.class,
                () -> repository.updateBatch(Flux.fromIterable(entities), BatchWriteOptions.of(3)));
        var optimistic = assertInstanceOf(com.flying.orm.rdb.batch.BatchOptimisticLockException.class,
                failure.getCause());
        assertEquals(prefix + 1L, optimistic.conflicts().getFirst().inputOffset());
        assertEquals(1, optimistic.conflicts().size());
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
        for (boolean evidence : new boolean[]{false, true}) {
            JdbcFixture fixture = new JdbcFixture();
            var repository = fixture.repository(GeneratedEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.PRE_PERSIST) event.entity().name = "prepared";
                return Mono.empty();
            });
            execute(repository, Operation.INSERT, evidence, Flux.just(new GeneratedEntity(1)), options());
            assertTrue(fixture.boundValues.contains("prepared"));
            assertFalse(fixture.boundValues.contains("generated-1"));
        }
    }

    @Test
    void ordinaryAndEvidenceShareBoundedLifecycleForEveryOperation() {
        for (Operation operation : Operation.values()) for (boolean evidence : new boolean[]{false, true}) {
            JdbcFixture fixture = new JdbcFixture();
            List<Long> posts = new ArrayList<>();
            var repository = fixture.repository(PayloadEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.POST_PERSIST || event.phase() == EntityLifecyclePhase.POST_UPDATE) {
                    posts.add(event.entity().id);
                    assertEquals(fixture.inputs, posts.size(), "POST must precede the next input at bufferSize=1");
                    assertEquals(1, fixture.acquired);
                    assertEquals(0, fixture.closed);
                    assertEquals(fixture.prepared, fixture.statementsClosed);
                }
                return Mono.empty();
            });
            Flux<PayloadEntity> rows = Flux.range(1, 16).map(fixture::input);
            var result = execute(repository, operation, evidence, rows, options());
            assertEquals(16, result.successfulCount());
            assertEquals(16, posts.size());
            fixture.assertClosed(16, 0, 0);
        }
    }

    @Test
    void generatedKeysWithoutListenerHaveOrdinaryEvidenceParity() {
        for (boolean evidence : new boolean[]{false, true}) {
            JdbcFixture fixture = new JdbcFixture();
            var repository = fixture.repository(GeneratedEntity.class);
            List<GeneratedEntity> entities = IntStream.rangeClosed(1, 16).mapToObj(GeneratedEntity::new).toList();
            var result = execute(repository, Operation.INSERT, evidence, Flux.fromIterable(entities), options());
            assertEquals(16, result.successfulCount());
            assertEquals(IntStream.rangeClosed(1, 16).mapToObj(id -> 1_000L + id).toList(),
                    entities.stream().map(GeneratedEntity::getId).toList());
            fixture.assertClosed(16, 0, 0);
        }
    }

    @Test
    void postFailurePreservesPrimaryCauseAndAllCompletedKeysAndCallbacks() {
        for (boolean evidence : new boolean[]{false, true}) {
            JdbcFixture fixture = new JdbcFixture();
            List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2), new GeneratedEntity(3));
            List<Long> posts = new ArrayList<>();
            IllegalStateException cause = new IllegalStateException("POST failed");
            var repository = fixture.repository(GeneratedEntity.class).withListener(event -> {
                if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                    posts.add(event.entity().id);
                    if (posts.size() == 1) return Mono.error(cause);
                }
                return Mono.empty();
            });
            EntityPostWriteException failure = assertThrows(EntityPostWriteException.class,
                    () -> execute(repository, Operation.INSERT, evidence, Flux.fromIterable(entities), BatchWriteOptions.of(2)));
            assertSame(cause, failure.getCause());
            assertEquals(EntityLifecyclePhase.POST_PERSIST, failure.phase());
            assertEquals(List.of(1_001L, 1_002L), posts);
            assertEquals(1_001L, entities.get(0).id);
            assertEquals(1_002L, entities.get(1).id);
            assertNull(entities.get(2).id);
            assertEquals(1, fixture.acquired);
            assertEquals(1, fixture.closed);
        }
    }

    @Test
    void laterSqlFailureDoesNotRestoreEarlierGeneratedKeys() {
        JdbcFixture fixture = new JdbcFixture();
        fixture.failedExecuteAt = 2;
        List<GeneratedEntity> entities = List.of(new GeneratedEntity(1), new GeneratedEntity(2), new GeneratedEntity(3));
        assertThrows(com.flying.orm.rdb.batch.BatchExecutionEvidenceException.class,
                () -> RepositoryBatchEvidenceAccess.sync(fixture.repository(GeneratedEntity.class)).insertEvidence(Flux.fromIterable(entities), options()));
        assertEquals(1_001L, entities.get(0).id);
        assertNull(entities.get(1).id);
        assertNull(entities.get(2).id);
        assertEquals(1, fixture.acquired);
        assertEquals(1, fixture.closed);
    }

    private static <T> com.flying.orm.rdb.batch.BatchExecutionEvidence execute(
            SyncFormRepository<T> repository, Operation operation, boolean evidence,
            Flux<T> rows, BatchWriteOptions options) {
        return switch (operation) {
            case INSERT -> evidence ? RepositoryBatchEvidenceAccess.sync(repository).insertEvidence(rows, options) : repository.insertBatch(rows, options);
            case UPDATE -> evidence ? RepositoryBatchEvidenceAccess.sync(repository).updateEvidence(rows, options) : repository.updateBatch(rows, options);
            case UPSERT -> evidence ? RepositoryBatchEvidenceAccess.sync(repository).upsertEvidence(rows, options) : repository.upsertBatch(rows, options);
        };
    }

    private static BatchWriteOptions options() {
        return BatchWriteOptions.of(1).withMemoryLimits(32, 4_096L).withMaxRowBytes(256L);
    }

    private enum Operation { INSERT, UPDATE, UPSERT }

    private record PostObservation(long id, int commits, int activeLeases, int inputs) { }

    private static final class JdbcFixture {
        private final List<Object> boundValues = new ArrayList<>();
        private final List<String> sql = new ArrayList<>();
        private final List<Long> preIds = new ArrayList<>();
        private final List<PostObservation> posts = new ArrayList<>();
        private int inputs;
        private int acquired;
        private int closed;
        private int prepared;
        private int statementsClosed;
        private int executions;
        private int commits;
        private int rollbacks;
        private int keysOpened;
        private int keysClosed;
        private int unknownCommitAt;
        private int failedExecuteAt;
        private java.util.function.IntUnaryOperator rowCounts = index -> 1;

        private <T> SyncFormRepository<T> repository(Class<T> type) {
            com.flying.orm.rdb.jdbc.JdbcConnectionAccess access = new com.flying.orm.rdb.jdbc.JdbcConnectionAccess() {
                public Connection getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
                    return connection(++acquired);
                }
                public void releaseConnection(Connection connection, com.flying.orm.core.sql.render.SqlRequest request)
                        throws SQLException {
                    connection.close();
                }
            };
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
            SyncFormClient client = SyncFormClient.create(JdbcSqlExecutor.create(access, RdbDialect.h2()),
                    JdbcBatchWriter.create(access, RdbDialect.h2()), renderer);
            return SyncFormRepository.create(client, client.entityModels().metadata(type).toDynamicForm(), type);
        }

        private PayloadEntity input(int id) {
            inputs++;
            return new PayloadEntity(id);
        }

        private Connection connection(int connectionIndex) {
            AtomicBoolean autoCommit = new AtomicBoolean(true);
            return proxy(Connection.class, (self, method, arguments) -> switch (method.getName()) {
                case "prepareStatement" -> {
                    sql.add((String) arguments[0]);
                    prepared++;
                    yield statement(connectionIndex);
                }
                case "close" -> {
                    closed++;
                    yield null;
                }
                default -> throw new AssertionError("unexpected Connection call: " + method.getName());
            });
        }

        private PreparedStatement statement(int connectionIndex) {
            AtomicInteger batchRows = new AtomicInteger();
            return proxy(PreparedStatement.class, (self, method, arguments) -> switch (method.getName()) {
                case "setObject" -> {
                    boundValues.add(arguments[1]);
                    assertFalse(arguments[1] instanceof byte[], "listener-only payload must not enter SQL");
                    yield null;
                }
                case "setNull", "cancel" -> null;
                case "addBatch" -> {
                    batchRows.incrementAndGet();
                    yield null;
                }
                case "executeBatch" -> {
                    int firstRow = executions;
                    executions += batchRows.get();
                    requireExecutionSuccess(connectionIndex);
                    int[] counts = new int[batchRows.get()];
                    for (int i = 0; i < counts.length; i++) counts[i] = rowCounts.applyAsInt(firstRow + i);
                    yield counts;
                }
                case "executeLargeUpdate" -> {
                    executions++;
                    requireExecutionSuccess(connectionIndex);
                    yield 1L;
                }
                case "getGeneratedKeys" -> generatedKeys(1_000L + executions);
                case "close" -> {
                    statementsClosed++;
                    yield null;
                }
                default -> throw new AssertionError("unexpected PreparedStatement call: " + method.getName());
            });
        }

        private void requireExecutionSuccess(int connectionIndex) throws SQLException {
            if (executions == failedExecuteAt) {
                throw new SQLException("chunk execution failed", "23000");
            }
        }

        private ResultSet generatedKeys(long key) {
            keysOpened++;
            AtomicInteger cursor = new AtomicInteger();
            ResultSetMetaData metadata = proxy(ResultSetMetaData.class,
                    (self, method, arguments) -> switch (method.getName()) {
                        case "getColumnCount" -> 1;
                        case "getColumnLabel", "getColumnName" -> "id";
                        case "getColumnType" -> java.sql.Types.BIGINT;
                        default -> throw new AssertionError("unexpected key metadata call: " + method.getName());
                    });
            return proxy(ResultSet.class, (self, method, arguments) -> switch (method.getName()) {
                case "next" -> cursor.getAndIncrement() == 0;
                case "getMetaData" -> metadata;
                case "getObject" -> key;
                case "close" -> {
                    keysClosed++;
                    yield null;
                }
                default -> throw new AssertionError("unexpected generated keys call: " + method.getName());
            });
        }

        private void assertObservedPerChunk(int rows) {
            assertEquals(rows, inputs);
            assertEquals(IntStream.rangeClosed(1, rows).mapToObj(id -> (long) id).toList(), preIds);
            assertEquals(IntStream.rangeClosed(1, rows)
                    .mapToObj(id -> new PostObservation(id, id, 0, id)).toList(), posts,
                    "POST must follow its commit/lease close and precede consumption of the next row");
        }

        private void assertClosed(int rows, int committed, int rolledBack) {
            assertEquals(rows, executions);
            assertEquals(committed, commits);
            assertEquals(rolledBack, rollbacks);
            assertEquals(1, acquired);
            assertEquals(acquired, closed);
            assertEquals(rows, prepared);
            assertEquals(prepared, statementsClosed);
            assertEquals(keysOpened, keysClosed);
        }
    }

    @TableName("sync_lifecycle_entities")
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

    @TableName("sync_generated_lifecycle_entities")
    private static final class GeneratedEntity {
        @TableId(type = IdType.AUTO)
        private Long id;
        private String name;
        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private final byte[] lifecycleOnly = new byte[1_024];

        private GeneratedEntity(int index) {
            this.name = "generated-" + index;
        }

        public Long getId() { return id; }
        public String getName() { return name; }
        public byte[] getLifecycleOnly() { return lifecycleOnly; }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
