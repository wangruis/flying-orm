package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Connection;
import org.reactivestreams.Publisher;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证单实体 Repository 在外部事务和数据库生成键失败时不会伪造提交状态。 */
class SingleEntityRepositoryFailureContractTest {

    @Test
    void reactivePostCallbackWaitsForExternalCommit() {
        CapturingR2dbcCompletion completion = new CapturingR2dbcCompletion();
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor(true, validGeneratedKey(), completion);
        IllegalStateException callback = new IllegalStateException("audit unavailable");
        AtomicInteger callbackCalls = new AtomicInteger();
        ReactiveFormRepository<PlainEntity> repository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(executor, renderer()), plainForm(), PlainEntity.class)
                .withListener(event -> {
                    if (event.phase() != EntityLifecyclePhase.POST_PERSIST) {
                        return Mono.empty();
                    }
                    callbackCalls.incrementAndGet();
                    return Mono.error(callback);
                });

        StepVerifier.create(repository.insert(new PlainEntity(1L, "sensor")))
                    .expectNext(1L)
                    .verifyComplete();
        assertEquals(0, callbackCalls.get());

        StepVerifier.create(completion.complete(TransactionOutcome.COMMITTED))
                    .expectErrorSatisfies(error -> {
                        CommittedEntityLifecycleException committed = assertInstanceOf(
                                CommittedEntityLifecycleException.class, error);
                        assertSame(callback, committed.getCause());
                    })
                    .verify();
        assertEquals(1, callbackCalls.get());
    }

    @Test
    void syncPostCallbackWaitsForExternalCommit() {
        CapturingJdbcCompletion completion = new CapturingJdbcCompletion();
        RecordingSyncExecutor executor = new RecordingSyncExecutor(true, validGeneratedKey(), completion);
        IllegalStateException callback = new IllegalStateException("audit unavailable");
        AtomicInteger callbackCalls = new AtomicInteger();
        SyncFormRepository<PlainEntity> repository = SyncFormRepository.create(
                        SyncFormClient.create(executor, unusedBatchExecutor(), renderer()),
                        plainForm(), PlainEntity.class)
                .withListener(event -> {
                    if (event.phase() != EntityLifecyclePhase.POST_PERSIST) {
                        return Mono.empty();
                    }
                    callbackCalls.incrementAndGet();
                    return Mono.error(callback);
                });

        assertEquals(1L, repository.insert(new PlainEntity(1L, "sensor")));
        assertEquals(0, callbackCalls.get());

        StepVerifier.create(completion.complete(TransactionOutcome.COMMITTED))
                    .expectErrorSatisfies(error -> {
                        CommittedEntityLifecycleException committed = assertInstanceOf(
                                CommittedEntityLifecycleException.class, error);
                        assertSame(callback, committed.getCause());
                    })
                    .verify();
        assertEquals(1, callbackCalls.get());
    }

    @Test
    void externalRollbackSkipsSingleEntityPostCallbacks() {
        AtomicInteger callbackCalls = new AtomicInteger();
        CapturingR2dbcCompletion reactiveCompletion = new CapturingR2dbcCompletion();
        RecordingReactiveExecutor reactiveExecutor = new RecordingReactiveExecutor(
                true, validGeneratedKey(), reactiveCompletion);
        ReactiveFormRepository<PlainEntity> reactiveRepository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(reactiveExecutor, renderer()), plainForm(), PlainEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        callbackCalls.incrementAndGet();
                    }
                    return Mono.empty();
                });

        StepVerifier.create(reactiveRepository.insert(new PlainEntity(1L, "reactive")))
                    .expectNext(1L)
                    .verifyComplete();
        StepVerifier.create(reactiveCompletion.complete(TransactionOutcome.ROLLED_BACK)).verifyComplete();

        CapturingJdbcCompletion syncCompletion = new CapturingJdbcCompletion();
        RecordingSyncExecutor syncExecutor = new RecordingSyncExecutor(true, validGeneratedKey(), syncCompletion);
        SyncFormRepository<PlainEntity> syncRepository = SyncFormRepository.create(
                        SyncFormClient.create(syncExecutor, unusedBatchExecutor(), renderer()),
                        plainForm(), PlainEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        callbackCalls.incrementAndGet();
                    }
                    return Mono.empty();
                });

        assertEquals(1L, syncRepository.insert(new PlainEntity(2L, "sync")));
        StepVerifier.create(syncCompletion.complete(TransactionOutcome.ROLLED_BACK)).verifyComplete();
        assertEquals(0, callbackCalls.get());
    }

    @Test
    void externalPostLifecycleWithoutCompletionFailsBeforeSql() {
        RecordingReactiveExecutor reactiveExecutor = new RecordingReactiveExecutor(true, validGeneratedKey());
        ReactiveFormRepository<PlainEntity> reactiveRepository = ReactiveFormRepository.create(
                        ReactiveFormClient.create(reactiveExecutor, renderer()), plainForm(), PlainEntity.class)
                .withListener(event -> Mono.empty());

        StepVerifier.create(reactiveRepository.insert(new PlainEntity(1L, "reactive")))
                    .expectErrorMessage("external transaction completion is required for POST entity lifecycle")
                    .verify();
        assertEquals(0, reactiveExecutor.writeCalls.get());

        RecordingSyncExecutor syncExecutor = new RecordingSyncExecutor(true, validGeneratedKey());
        SyncFormRepository<PlainEntity> syncRepository = SyncFormRepository.create(
                        SyncFormClient.create(syncExecutor, unusedBatchExecutor(), renderer()),
                        plainForm(), PlainEntity.class)
                .withListener(event -> Mono.empty());

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> syncRepository.insert(new PlainEntity(2L, "sync")));
        assertEquals("external transaction completion is required for POST entity lifecycle", failure.getMessage());
        assertEquals(0, syncExecutor.writeCalls.get());
    }

    @Test
    void reactiveGeneratedKeyFailureReportsCommittedOrEnlistedState() {
        assertReactiveGeneratedKeyState(false, true);
        assertReactiveGeneratedKeyState(true, false);
    }

    @Test
    void syncGeneratedKeyFailureReportsCommittedOrEnlistedState() {
        assertSyncGeneratedKeyState(false, true);
        assertSyncGeneratedKeyState(true, false);
    }

    @Test
    void generatedKeyReadFailureKeepsWriteOutcomeForBothExecutors() {
        GeneratedKeyReadException reactiveRead = new GeneratedKeyReadException(
                1L, new IllegalStateException("key stream failed"));
        RecordingReactiveExecutor reactiveExecutor = new RecordingReactiveExecutor(false, reactiveRead);
        ReactiveFormRepository<AutoEntity> reactiveRepository = ReactiveFormRepository.create(
                ReactiveFormClient.create(reactiveExecutor, renderer()), generatedForm(), AutoEntity.class);
        StepVerifier.create(reactiveRepository.insert(new AutoEntity("reactive")))
                    .expectErrorSatisfies(error -> assertGeneratedKeyReadState(
                            error, reactiveRead, GeneratedKeyResolutionException.WriteState.UNKNOWN))
                    .verify();

        GeneratedKeyReadException syncRead = new GeneratedKeyReadException(
                1L, new IllegalStateException("key result set failed"));
        RecordingSyncExecutor syncExecutor = new RecordingSyncExecutor(false, syncRead);
        SyncFormRepository<AutoEntity> syncRepository = SyncFormRepository.create(
                SyncFormClient.create(syncExecutor, unusedBatchExecutor(), renderer()),
                generatedForm(), AutoEntity.class);
        GeneratedKeyResolutionException error = assertThrows(
                GeneratedKeyResolutionException.class,
                () -> syncRepository.insert(new AutoEntity("sync")));
        assertGeneratedKeyReadState(error, syncRead, GeneratedKeyResolutionException.WriteState.UNKNOWN);

        GeneratedKeyReadException enlistedRead = new GeneratedKeyReadException(
                1L, new IllegalStateException("enlisted key stream failed"));
        RecordingReactiveExecutor enlistedExecutor = new RecordingReactiveExecutor(true, enlistedRead);
        ReactiveFormRepository<AutoEntity> enlistedRepository = ReactiveFormRepository.create(
                ReactiveFormClient.create(enlistedExecutor, renderer()), generatedForm(), AutoEntity.class);
        StepVerifier.create(enlistedRepository.insert(new AutoEntity("enlisted")))
                    .expectErrorSatisfies(failure -> assertGeneratedKeyReadState(
                            failure, enlistedRead, GeneratedKeyResolutionException.WriteState.ENLISTED))
                    .verify();
    }

    @Test
    void generatedKeyReadFailureKeepsNestedVirtualMachineErrorForBothExecutors() {
        OutOfMemoryError fatal = new OutOfMemoryError("generated key fatal");
        GeneratedKeyReadException reactiveRead = new GeneratedKeyReadException(
                1L, new IllegalStateException("key stream failed", fatal));
        RecordingReactiveExecutor reactiveExecutor = new RecordingReactiveExecutor(false, reactiveRead);
        ReactiveFormRepository<AutoEntity> reactiveRepository = ReactiveFormRepository.create(
                ReactiveFormClient.create(reactiveExecutor, renderer()), generatedForm(), AutoEntity.class);
        StepVerifier.create(reactiveRepository.insert(new AutoEntity("reactive")))
                    .expectErrorSatisfies(error -> assertSame(fatal, error))
                    .verify();

        GeneratedKeyReadException syncRead = new GeneratedKeyReadException(
                1L, new IllegalStateException("key result set failed", fatal));
        RecordingSyncExecutor syncExecutor = new RecordingSyncExecutor(false, syncRead);
        SyncFormRepository<AutoEntity> syncRepository = SyncFormRepository.create(
                SyncFormClient.create(syncExecutor, unusedBatchExecutor(), renderer()),
                generatedForm(), AutoEntity.class);
        assertSame(fatal, assertThrows(OutOfMemoryError.class,
                () -> syncRepository.insert(new AutoEntity("sync"))));
    }

    private static void assertReactiveGeneratedKeyState(boolean external, boolean committed) {
        RecordingReactiveExecutor executor = new RecordingReactiveExecutor(external, invalidGeneratedKey());
        ReactiveFormRepository<AutoEntity> repository = ReactiveFormRepository.create(
                ReactiveFormClient.create(executor, renderer()), generatedForm(), AutoEntity.class);

        StepVerifier.create(repository.insert(new AutoEntity("sensor")))
                    .expectErrorSatisfies(error -> assertGeneratedKeyState(error, committed))
                    .verify();
    }

    private static void assertSyncGeneratedKeyState(boolean external, boolean committed) {
        RecordingSyncExecutor executor = new RecordingSyncExecutor(external, invalidGeneratedKey());
        SyncFormRepository<AutoEntity> repository = SyncFormRepository.create(
                SyncFormClient.create(executor, unusedBatchExecutor(), renderer()),
                generatedForm(), AutoEntity.class);

        GeneratedKeyResolutionException error = assertThrows(
                GeneratedKeyResolutionException.class, () -> repository.insert(new AutoEntity("sensor")));
        assertGeneratedKeyState(error, committed);
    }

    private static void assertGeneratedKeyState(Throwable failure, boolean committed) {
        GeneratedKeyResolutionException error = assertInstanceOf(
                GeneratedKeyResolutionException.class, failure);
        assertEquals(1L, error.affectedRows());
        assertEquals(committed, error.committed());
        assertEquals(!committed, error.enlisted());
        assertFalse(error.unknown());
        assertInstanceOf(MappingException.class, error.getCause());
        assertFalse(error.getMessage().contains("not-a-number"));
        assertEquals("GENERATED_KEY_RESOLUTION_FAILED", error.toErrorReport().code());
    }

    private static void assertGeneratedKeyReadState(Throwable failure,
                                                    GeneratedKeyReadException cause,
                                                    GeneratedKeyResolutionException.WriteState state) {
        GeneratedKeyResolutionException error = assertInstanceOf(
                GeneratedKeyResolutionException.class, failure);
        assertEquals(1L, error.affectedRows());
        assertEquals(state, error.state());
        assertEquals(state == GeneratedKeyResolutionException.WriteState.COMMITTED, error.committed());
        assertEquals(state == GeneratedKeyResolutionException.WriteState.ENLISTED, error.enlisted());
        assertEquals(state == GeneratedKeyResolutionException.WriteState.UNKNOWN, error.unknown());
        assertSame(cause, error.getCause());
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addTerm(SqlTermHandler.equalsTo()).build(), RdbDialect.h2());
    }

    private static DynamicForm plainForm() {
        return DynamicForm.builder("plain", "plain_device")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static DynamicForm generatedForm() {
        return DynamicForm.builder("generated", "generated_device")
                          .addField(DynamicField.primaryKey("id", "BIGINT").generatedByIdentity())
                          .addField(DynamicField.of("name", "VARCHAR"))
                          .build();
    }

    private static SqlWriteResult validGeneratedKey() {
        return new SqlWriteResult(1L, List.of(DynamicRow.copyOf(Map.of("id", 7L))));
    }

    private static SqlWriteResult invalidGeneratedKey() {
        return new SqlWriteResult(1L, List.of(DynamicRow.copyOf(Map.of("id", "not-a-number"))));
    }

    private static SyncBatchExecutor unusedBatchExecutor() {
        return new SyncBatchExecutor() {
            @Override
            public BatchWriteResult writeBatch(BatchWriteRequest request) {
                throw new AssertionError("single entity test must not execute a batch");
            }

            @Override
            public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                throw new AssertionError("single entity test must not execute batch chunks");
            }
        };
    }

    private record PlainEntity(@TableId Long id, String name) {
    }

    private static final class AutoEntity {

        @TableId(type = IdType.AUTO)
        private Long id;

        private final String name;

        private AutoEntity(String name) {
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }
    }

    private static final class RecordingReactiveExecutor implements ReactiveSqlExecutor {

        private final boolean external;
        private final SqlWriteResult generatedKey;
        private final GeneratedKeyReadException generatedKeyFailure;
        private final R2dbcTransactionCompletion completion;
        private final AtomicInteger writeCalls = new AtomicInteger();

        private RecordingReactiveExecutor(boolean external, SqlWriteResult generatedKey) {
            this(external, generatedKey, R2dbcTransactionCompletion.unavailable());
        }

        private RecordingReactiveExecutor(boolean external, GeneratedKeyReadException generatedKeyFailure) {
            this.external = external;
            this.generatedKey = null;
            this.generatedKeyFailure = generatedKeyFailure;
            this.completion = R2dbcTransactionCompletion.unavailable();
        }

        private RecordingReactiveExecutor(boolean external,
                                          SqlWriteResult generatedKey,
                                          R2dbcTransactionCompletion completion) {
            this.external = external;
            this.generatedKey = generatedKey;
            this.generatedKeyFailure = null;
            this.completion = completion;
        }

        @Override
        public Mono<R2dbcTransactionContext> currentTransaction() {
            return external
                    ? Mono.just(R2dbcTransactionContext.external(connection(), "primary", completion))
                    : Mono.empty();
        }

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            writeCalls.incrementAndGet();
            return Mono.just(1L);
        }

        @Override
        public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return generatedKeyFailure == null ? Mono.just(generatedKey) : Mono.error(generatedKeyFailure);
        }
    }

    private static final class RecordingSyncExecutor implements SyncSqlExecutor {

        private final boolean external;
        private final SqlWriteResult generatedKey;
        private final GeneratedKeyReadException generatedKeyFailure;
        private final JdbcTransactionCompletion completion;
        private final AtomicInteger writeCalls = new AtomicInteger();

        private RecordingSyncExecutor(boolean external, SqlWriteResult generatedKey) {
            this(external, generatedKey, JdbcTransactionCompletion.unavailable());
        }

        private RecordingSyncExecutor(boolean external, GeneratedKeyReadException generatedKeyFailure) {
            this.external = external;
            this.generatedKey = null;
            this.generatedKeyFailure = generatedKeyFailure;
            this.completion = JdbcTransactionCompletion.unavailable();
        }

        private RecordingSyncExecutor(boolean external,
                                      SqlWriteResult generatedKey,
                                      JdbcTransactionCompletion completion) {
            this.external = external;
            this.generatedKey = generatedKey;
            this.generatedKeyFailure = null;
            this.completion = completion;
        }

        @Override
        public Optional<JdbcTransactionContext> currentTransaction() {
            return external
                    ? Optional.of(JdbcTransactionContext.external(jdbcConnection(), "primary", completion))
                    : Optional.empty();
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            writeCalls.incrementAndGet();
            return 1L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            if (generatedKeyFailure != null) {
                throw generatedKeyFailure;
            }
            return generatedKey;
        }
    }

    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> method.getReturnType() == boolean.class ? false : null);
    }

    private static java.sql.Connection jdbcConnection() {
        return (java.sql.Connection) Proxy.newProxyInstance(
                java.sql.Connection.class.getClassLoader(), new Class<?>[]{java.sql.Connection.class},
                (ignored, method, arguments) -> method.getReturnType() == boolean.class ? false : null);
    }

    private static final class CapturingR2dbcCompletion implements R2dbcTransactionCompletion {

        private Listener listener;

        @Override
        public boolean register(Listener listener) {
            this.listener = listener;
            return true;
        }

        private Publisher<Void> complete(TransactionOutcome outcome) {
            return listener.afterCompletion(outcome);
        }
    }

    private static final class CapturingJdbcCompletion implements JdbcTransactionCompletion {

        private Listener listener;

        @Override
        public boolean register(Listener listener) {
            this.listener = listener;
            return true;
        }

        private Publisher<Void> complete(TransactionOutcome outcome) {
            return listener.afterCompletion(outcome);
        }
    }
}
