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
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
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
    void summaryContinuesInputAfterOrdinaryPostFailures() {
        assertPostFailuresDoNotStopInput(false);
    }

    @Test
    void chunksContinueInputAfterOrdinaryPostFailures() {
        assertPostFailuresDoNotStopInput(true);
    }

    @Test
    void summaryKeepsLaterInputFailurePrimaryAndSuppressesEarlierPostFailure() {
        assertInputFailureRemainsPrimary(false);
    }

    @Test
    void chunksKeepLaterInputFailurePrimaryAndSuppressEarlierPostFailure() {
        assertInputFailureRemainsPrimary(true);
    }

    @Test
    void directPostErrorIsNotWrappedAsAnInputFailure() {
        JdbcFixture fixture = new JdbcFixture();
        LinkageError fatal = new LinkageError("POST linkage failure");
        SyncFormRepository<PayloadEntity> repository = fixture.repository(PayloadEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        throw fatal;
                    }
                    return Mono.empty();
                });

        assertSame(fatal, assertThrows(LinkageError.class,
                () -> repository.insertBatch(source(fixture, 3), options())));

        assertEquals(1, fixture.inputs, "direct Error must stop subsequent input after the committed chunk");
        fixture.assertClosed(1, 1, 0);
    }

    @Test
    void transactionBecomingExternalKeepsAbortFailurePrimaryAndPreservesPostFailure() {
        for (boolean chunks : new boolean[]{false, true}) {
            assertExternalTransactionFailureRemainsPrimary(chunks);
        }
    }

    @Test
    void generatedKeysWithoutListenerDoNotRetainEarlierCommittedEntities() {
        JdbcFixture fixture = new JdbcFixture();
        SyncFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class);
        List<GeneratedEntity> entities = IntStream.rangeClosed(1, 16)
                .mapToObj(GeneratedEntity::new).toList();

        BatchWriteResult result = repository.insertBatch(Flux.fromIterable(entities), options());

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(IntStream.rangeClosed(1, 16).mapToObj(id -> 1_000L + id).toList(),
                entities.stream().map(GeneratedEntity::getId).toList());
        fixture.assertClosed(16, 16, 0);
        assertEquals(16, fixture.keysOpened);
    }

    @Test
    void directPostErrorKeepsEveryGeneratedKeyInTheAlreadyCommittedChunk() {
        for (boolean chunks : new boolean[]{false, true}) {
            JdbcFixture fixture = new JdbcFixture();
            List<GeneratedEntity> entities = IntStream.rangeClosed(1, 4)
                    .mapToObj(GeneratedEntity::new).toList();
            LinkageError fatal = new LinkageError("first POST failed after the whole chunk committed");
            AtomicInteger postCalls = new AtomicInteger();
            AtomicReference<List<Long>> keysAtFirstPost = new AtomicReference<>();
            AtomicReference<Object> firstPostResult = new AtomicReference<>();
            AtomicInteger activeLeasesAtFirstPost = new AtomicInteger(-1);
            SyncFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class)
                    .withListener(event -> {
                        if (event.phase() == EntityLifecyclePhase.POST_PERSIST
                                && postCalls.incrementAndGet() == 1) {
                            keysAtFirstPost.set(entities.subList(0, 3).stream().map(GeneratedEntity::getId).toList());
                            firstPostResult.set(event.result());
                            activeLeasesAtFirstPost.set(fixture.acquired - fixture.closed);
                            throw fatal;
                        }
                        return Mono.empty();
                    });
            Flux<GeneratedEntity> input = Flux.fromIterable(entities).doOnNext(ignored -> fixture.inputs++);
            BatchWriteOptions options = BatchWriteOptions.independent(3, 1)
                    .withMemoryLimits(8, 16_384L, 8).withMaxRowBytes(256L);

            assertSame(fatal, assertThrows(LinkageError.class, () -> {
                if (chunks) {
                    repository.insertBatchChunks(input, options);
                } else {
                    repository.insertBatch(input, options);
                }
            }));

            assertEquals(List.of(1_001L, 1_002L, 1_003L), keysAtFirstPost.get());
            BatchChunkResult committed = assertInstanceOf(BatchChunkResult.class, firstPostResult.get());
            assertEquals(BatchChunkResult.Status.COMMITTED, committed.status());
            assertEquals(3, committed.inputCount());
            assertEquals(0, activeLeasesAtFirstPost.get());
            assertEquals(List.of(1_001L, 1_002L, 1_003L),
                    entities.subList(0, 3).stream().map(GeneratedEntity::getId).toList(),
                    "abort must not restore generated keys belonging to the already committed chunk");
            assertNull(entities.get(3).getId());
            assertEquals(1, postCalls.get(), "direct Error must stop later POST callbacks");
            assertEquals(3, fixture.inputs, "direct Error must not consume the next chunk");
            assertEquals(3, fixture.executions);
            assertEquals(1, fixture.commits);
            assertEquals(0, fixture.rollbacks);
            assertEquals(1, fixture.acquired);
            assertEquals(1, fixture.closed);
            assertEquals(1, fixture.prepared);
            assertEquals(1, fixture.statementsClosed);
            assertEquals(3, fixture.keysOpened);
            assertEquals(3, fixture.keysClosed);
        }
    }

    @Test
    void unknownCommitRestoresOnlyItsGeneratedKeyBeforeTheNextInput() {
        assertUnconfirmedGeneratedKeys(true);
    }

    @Test
    void failedChunkDoesNotRestoreKeysFromEarlierCommittedChunks() {
        assertUnconfirmedGeneratedKeys(false);
    }

    private static void assertRetiredPerChunk(Operation operation, boolean chunks) {
        JdbcFixture fixture = new JdbcFixture();
        SyncFormRepository<PayloadEntity> repository = observedRepository(fixture, operation, null, null);

        List<BatchChunkResult> results = execute(repository, operation, chunks, source(fixture, 16));

        assertEquals(16, results.size());
        for (int index = 0; index < results.size(); index++) {
            BatchChunkResult result = results.get(index);
            assertEquals(index, result.chunkIndex());
            assertEquals(index, result.startOffset());
            assertEquals(1, result.inputCount());
            assertEquals(1L, result.affectedRows());
            assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
        }
        fixture.assertObservedPerChunk(16);
        fixture.assertClosed(16, 16, 0);
    }

    private static void assertPostFailuresDoNotStopInput(boolean chunks) {
        JdbcFixture fixture = new JdbcFixture();
        IllegalStateException first = new IllegalStateException("first POST failed");
        IllegalArgumentException third = new IllegalArgumentException("third POST failed");
        SyncFormRepository<PayloadEntity> repository = observedRepository(
                fixture, Operation.INSERT, first, third);

        CommittedEntityLifecycleException failure = assertThrows(CommittedEntityLifecycleException.class,
                () -> execute(repository, Operation.INSERT, chunks, source(fixture, 8)));

        assertTrue(failure.committed());
        assertSame(first, failure.getCause());
        assertEquals(BatchChunkResult.Status.COMMITTED,
                assertInstanceOf(BatchChunkResult.class, failure.result()).status());
        List<CommittedEntityLifecycleException> later = committedSuppressed(failure);
        assertEquals(1, later.size());
        assertSame(third, later.getFirst().getCause());
        fixture.assertObservedPerChunk(8);
        fixture.assertClosed(8, 8, 0);
    }

    private static void assertInputFailureRemainsPrimary(boolean chunks) {
        JdbcFixture fixture = new JdbcFixture();
        IllegalStateException postFailure = new IllegalStateException("first POST failed");
        IllegalStateException inputFailure = new IllegalStateException("input failed before row three");
        SyncFormRepository<PayloadEntity> repository = observedRepository(
                fixture, Operation.INSERT, postFailure, null);
        AtomicInteger postsBeforeInputFailure = new AtomicInteger(-1);
        Flux<PayloadEntity> source = Flux.range(1, 3).map(id -> {
            if (id == 3) {
                postsBeforeInputFailure.set(fixture.posts.size());
                throw inputFailure;
            }
            return fixture.input(id);
        });

        BatchWriteException failure = assertThrows(BatchWriteException.class,
                () -> execute(repository, Operation.INSERT, chunks, source));

        assertSame(inputFailure, failure.getCause());
        assertEquals(2, postsBeforeInputFailure.get());
        assertEquals(List.of(BatchChunkResult.Status.COMMITTED, BatchChunkResult.Status.COMMITTED,
                        BatchChunkResult.Status.FAILED),
                failure.result().chunks().stream().map(BatchChunkResult::status).toList());
        assertEquals(0, failure.result().chunks().getLast().inputCount());
        List<CommittedEntityLifecycleException> suppressed = committedSuppressed(failure);
        assertEquals(1, suppressed.size());
        assertSame(postFailure, suppressed.getFirst().getCause());
        fixture.assertObservedPerChunk(2);
        fixture.assertClosed(2, 2, 0);
    }

    private static void assertUnconfirmedGeneratedKeys(boolean unknownCommit) {
        JdbcFixture fixture = new JdbcFixture();
        if (unknownCommit) {
            fixture.unknownCommitAt = 2;
        } else {
            fixture.failedExecuteAt = 2;
        }
        SyncFormRepository<GeneratedEntity> repository = fixture.repository(GeneratedEntity.class);
        List<GeneratedEntity> entities = IntStream.rangeClosed(1, 3)
                .mapToObj(GeneratedEntity::new).toList();
        AtomicReference<Long> secondKeyBeforeThirdInput = new AtomicReference<>(-1L);
        Flux<GeneratedEntity> source = Flux.range(0, 3).map(index -> {
            if (index == 2) {
                secondKeyBeforeThirdInput.set(entities.get(1).getId());
            }
            return entities.get(index);
        });

        BatchWriteResult result = repository.insertBatch(source, options());

        assertEquals(List.of(BatchChunkResult.Status.COMMITTED,
                        unknownCommit ? BatchChunkResult.Status.UNKNOWN : BatchChunkResult.Status.FAILED,
                        BatchChunkResult.Status.COMMITTED),
                result.chunks().stream().map(BatchChunkResult::status).toList());
        assertEquals(1_001L, entities.get(0).getId());
        assertNull(entities.get(1).getId());
        assertEquals(1_003L, entities.get(2).getId());
        assertNull(secondKeyBeforeThirdInput.get(), "unconfirmed key must be restored before the next row");
        fixture.assertClosed(3, 2, unknownCommit ? 0 : 1);
    }

    private static void assertExternalTransactionFailureRemainsPrimary(boolean chunks) {
        JdbcFixture fixture = new JdbcFixture();
        AtomicInteger externalCalls = new AtomicInteger();
        Connection external = proxy(Connection.class, (self, method, arguments) -> {
            externalCalls.incrementAndGet();
            throw new AssertionError("independent batch must not use an external connection");
        });
        JdbcTransactionContext transaction = JdbcTransactionContext.external(external);
        fixture.participant = () -> fixture.commits == 0 ? Optional.empty() : Optional.of(transaction);
        IllegalStateException postFailure = new IllegalStateException("first POST failed before external transaction");
        SyncFormRepository<PayloadEntity> repository = observedRepository(
                fixture, Operation.INSERT, postFailure, null);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> execute(repository, Operation.INSERT, chunks, source(fixture, 3)));

        assertEquals("com.flying.orm.rdb.jdbc.JdbcExternalTransactionModeException", failure.getClass().getName());
        assertNull(failure.getCause(), "the external mode rejection must not become a wrapped SQL failure");
        List<CommittedEntityLifecycleException> suppressed = committedSuppressed(failure);
        assertEquals(1, suppressed.size());
        assertSame(postFailure, suppressed.getFirst().getCause());
        assertEquals(2, fixture.inputs);
        assertEquals(List.of(1L, 2L), fixture.preIds);
        assertEquals(List.of(new PostObservation(1L, 1, 0, 1)), fixture.posts,
                "abort must not invoke the already retired first entity again");
        assertEquals(0, externalCalls.get());
        fixture.assertClosed(1, 1, 0);
    }

    private static List<CommittedEntityLifecycleException> committedSuppressed(Throwable failure) {
        return Arrays.stream(failure.getSuppressed())
                .filter(CommittedEntityLifecycleException.class::isInstance)
                .map(CommittedEntityLifecycleException.class::cast).toList();
    }

    private static SyncFormRepository<PayloadEntity> observedRepository(JdbcFixture fixture,
                                                                        Operation operation,
                                                                        RuntimeException firstFailure,
                                                                        RuntimeException thirdFailure) {
        EntityLifecyclePhase pre = operation == Operation.UPDATE
                ? EntityLifecyclePhase.PRE_UPDATE : EntityLifecyclePhase.PRE_PERSIST;
        EntityLifecyclePhase post = operation == Operation.UPDATE
                ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
        return fixture.repository(PayloadEntity.class).withListener(event -> {
            if (event.phase() == pre) {
                fixture.preIds.add(event.entity().getId());
            }
            if (event.phase() == post) {
                long id = event.entity().getId();
                fixture.posts.add(new PostObservation(id, fixture.commits,
                        fixture.acquired - fixture.closed, fixture.inputs));
                if (id == 1L && firstFailure != null) {
                    return Mono.error(firstFailure);
                }
                if (id == 3L && thirdFailure != null) {
                    return Mono.error(thirdFailure);
                }
            }
            return Mono.empty();
        });
    }

    private static Flux<PayloadEntity> source(JdbcFixture fixture, int rows) {
        return Flux.range(1, rows).map(fixture::input);
    }

    private static List<BatchChunkResult> execute(SyncFormRepository<PayloadEntity> repository,
                                                  Operation operation,
                                                  boolean chunks,
                                                  Flux<PayloadEntity> source) {
        if (chunks) {
            return switch (operation) {
                case INSERT -> repository.insertBatchChunks(source, options());
                case UPDATE -> repository.updateBatchChunks(source, options());
                case UPSERT -> repository.upsertBatchChunks(source, options());
            };
        }
        BatchWriteResult result = switch (operation) {
            case INSERT -> repository.insertBatch(source, options());
            case UPDATE -> repository.updateBatch(source, options());
            case UPSERT -> repository.upsertBatch(source, options());
        };
        return result.chunks();
    }

    private static BatchWriteOptions options() {
        return BatchWriteOptions.independent(1, 1)
                .withMemoryLimits(32, 4_096L, 32).withMaxRowBytes(256L);
    }

    private enum Operation { INSERT, UPDATE, UPSERT }

    private record PostObservation(long id, int commits, int activeLeases, int inputs) { }

    private static final class JdbcFixture {
        private final List<Long> preIds = new ArrayList<>();
        private final List<PostObservation> posts = new ArrayList<>();
        private JdbcTransactionParticipant participant = JdbcTransactionParticipant.none();
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

        private <T> SyncFormRepository<T> repository(Class<T> type) {
            DataSource dataSource = proxy(DataSource.class, (self, method, arguments) -> {
                if (method.getName().equals("getConnection")) {
                    return connection(++acquired);
                }
                throw new AssertionError("unexpected DataSource call: " + method.getName());
            });
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
            SyncFormClient client = SyncFormClient.create(JdbcSqlExecutor.create(dataSource, RdbDialect.h2()),
                    JdbcBatchWriter.create(dataSource).withTransactionParticipant(participant), renderer);
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
                    prepared++;
                    yield statement(connectionIndex);
                }
                case "getAutoCommit" -> autoCommit.get();
                case "setAutoCommit" -> {
                    autoCommit.set((boolean) arguments[0]);
                    yield null;
                }
                case "commit" -> {
                    if (connectionIndex == unknownCommitAt) {
                        throw new SQLException("commit outcome unknown", "08006");
                    }
                    commits++;
                    yield null;
                }
                case "rollback" -> {
                    rollbacks++;
                    yield null;
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
                    assertFalse(arguments[1] instanceof byte[], "listener-only payload must not enter SQL");
                    yield null;
                }
                case "setNull", "cancel", "setQueryTimeout" -> null;
                case "addBatch" -> {
                    batchRows.incrementAndGet();
                    yield null;
                }
                case "executeBatch" -> {
                    executions += batchRows.get();
                    requireExecutionSuccess(connectionIndex);
                    int[] counts = new int[batchRows.get()];
                    Arrays.fill(counts, 1);
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
            if (connectionIndex == failedExecuteAt) {
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
            assertEquals(rows, acquired);
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
        private final String name;
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
