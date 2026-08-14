package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionEventType;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionResultKind;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证通用观测包装器能看见批量分片、汇总和 UNKNOWN 恢复。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
class ObservedReactiveSqlExecutorTest {

    /** 默认选项、批量内存和观测装饰器都不能剥离原生 R2DBC 的原子字段保护写入能力。 */
    @Test
    void forwardsAtomicProtectedWriteThroughComposedDecorators() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                SqlRequest.nativeSql("insert into users(id) values(?)", List.of(1L)),
                null,
                List.of("id"),
                java.util.Map.of("id", 1L),
                "id = ?",
                "delete from users_tokens where id = ?",
                "insert into users_tokens(id, field_tag, token_hash) values(?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[32]))));
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults();
        SqlWriteResult expected = new SqlWriteResult(1L, List.of());
        AtomicReference<ProtectedWriteWork> observedWork = new AtomicReference<>();
        AtomicReference<SqlExecutionOptions> observedOptions = new AtomicReference<>();
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork actualWork,
                                                             SqlExecutionOptions actualOptions) {
                observedWork.set(actualWork);
                observedOptions.set(actualOptions);
                return Mono.just(expected);
            }
        };
        ReactiveSqlExecutor decorated = delegate.withDefaultExecutionOptions(options)
                                                .withBatchMemoryLimits(BatchMemoryLimits.defaults())
                                                .withObserver(observations::add);

        StepVerifier.create(decorated.atomicProtectedWrite(work, options))
                    .expectNext(expected)
                    .verifyComplete();

        assertSame(work, observedWork.get());
        assertSame(options, observedOptions.get());
        assertEquals(1, observations.size());
        assertEquals(SqlExecutionStatus.SUCCESS, observations.getFirst().status());
        assertEquals(work.writeRequest().sql(), observations.getFirst().sql());
        assertEquals(1L, observations.getFirst().rows());
    }

    /** 组合装饰器必须同时保留 ATOMIC 和 INDEPENDENT 受保护批量入口。 */
    @Test
    void forwardsProtectedBatchEntrypointsThroughComposedDecorators() {
        BatchWriteRequest atomic = request(BatchWriteOptions.atomic(1));
        BatchWriteRequest independent = request(BatchWriteOptions.independent(1));
        BatchChunkResult committed = BatchChunkResult.committed(0, 0, 1, 1);
        BatchWriteResult expected = BatchWriteResult.from(atomic.options().mode(), List.of(committed));
        List<BatchWriteRequest> observed = new ArrayList<>();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
                observed.add(request);
                return Mono.just(expected);
            }

            @Override
            public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
                observed.add(request);
                return Flux.just(committed);
            }
        };
        ReactiveSqlExecutor decorated = delegate
                .withDefaultExecutionOptions(SqlExecutionOptions.safeDefaults())
                .withBatchMemoryLimits(BatchMemoryLimits.defaults())
                .withObserver(ignored -> { });

        StepVerifier.create(decorated.writeProtectedBatch(atomic))
                    .expectNext(expected)
                    .verifyComplete();
        StepVerifier.create(decorated.writeProtectedBatchChunks(independent))
                    .expectNext(committed)
                    .verifyComplete();

        assertEquals(2, observed.size());
        assertSame(atomic, observed.get(0));
        assertSame(independent, observed.get(1));
    }

    @Test
    void observesBatchChunksAndSummary() {
        List<BatchExecutionObservation> observations = new ArrayList<>();
        List<SqlTransactionSource> transactionSources = new ArrayList<>();
        BatchWriteRequest request = request(BatchWriteOptions.independent(2));
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                return Flux.just(BatchChunkResult.committed(0, 0, 2, 2),
                                 BatchChunkResult.failed(1, 2, 1, new RuntimeException("bad row")));
            }
        };

        ReactiveSqlExecutor executor = delegate.withBatchObserver(new BatchExecutionObserver() {
            @Override
            public void onExecution(BatchExecutionObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onExecution(BatchExecutionObservation observation,
                                    SqlTransactionSource transactionSource) {
                observations.add(observation);
                transactionSources.add(transactionSource);
            }
        });

        StepVerifier.create(executor.writeBatchChunks(request))
                    .expectNextCount(2)
                    .verifyComplete();

        assertEquals(3, observations.size());
        assertEquals(SqlExecutionBackend.R2DBC, observations.getFirst().backend());
        assertEquals(BatchExecutionEventType.CHUNK, observations.get(0).eventType());
        assertEquals(BatchChunkResult.Status.COMMITTED, observations.get(0).chunkStatus());
        assertEquals(SqlExecutionResultKind.SUCCESS, observations.get(0).resultKind());
        assertEquals(BatchExecutionEventType.CHUNK, observations.get(1).eventType());
        assertEquals(BatchChunkResult.Status.FAILED, observations.get(1).chunkStatus());
        assertEquals(SqlExecutionResultKind.UNKNOWN, observations.get(1).resultKind());
        assertEquals(BatchExecutionEventType.SUMMARY, observations.get(2).eventType());
        assertEquals(BatchWriteResult.Status.PARTIAL, observations.get(2).summaryStatus());
        assertEquals(SqlExecutionResultKind.PARTIAL, observations.get(2).resultKind());
        assertEquals(SqlFailureCategory.UNKNOWN, observations.get(2).failureCategory());
        assertEquals(2L, observations.get(2).chunkCount());
        assertEquals(1L, observations.get(2).successfulChunkCount());
        assertEquals(1L, observations.get(2).failedChunkCount());
        assertEquals(List.of(SqlTransactionSource.INTERNAL,
                             SqlTransactionSource.INTERNAL,
                             SqlTransactionSource.INTERNAL), transactionSources);
    }

    @Test
    void observesBatchWriteResultChunksAndSummary() {
        List<BatchExecutionObservation> observations = new ArrayList<>();
        BatchWriteRequest request = request(BatchWriteOptions.atomic(2));
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Mono.just(BatchWriteResult.from(request.options().mode(),
                                                       List.of(BatchChunkResult.committed(0, 0, 2, 2))));
            }
        };

        ReactiveSqlExecutor executor = delegate.withBatchObserver(observations::add);

        StepVerifier.create(executor.writeBatch(request))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.COMMITTED)
                    .verifyComplete();

        assertEquals(2, observations.size());
        assertEquals(BatchExecutionEventType.CHUNK, observations.get(0).eventType());
        assertEquals(BatchExecutionEventType.SUMMARY, observations.get(1).eventType());
        assertEquals(SqlExecutionResultKind.SUCCESS, observations.get(1).resultKind());
        assertEquals(2L, observations.get(1).affectedRows());
        assertEquals(1L, observations.get(1).chunkCount());
        assertEquals(1L, observations.get(1).successfulChunkCount());
        assertEquals(0L, observations.get(1).failedChunkCount());
    }

    @Test
    void observesAtomicConflictDetailsCarriedByBatchException() {
        List<BatchExecutionObservation> observations = new ArrayList<>();
        BatchChunkResult conflict = BatchChunkResult.conflicted(
                0,
                0,
                2,
                List.of(BatchRowConflict.exactlyOne(1, 0)));
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(conflict));
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Mono.error(new BatchWriteException("atomic batch rolled back",
                                                          new IllegalStateException("version conflict"),
                                                          result));
            }
        };

        StepVerifier.create(delegate.withBatchObserver(observations::add)
                                    .writeBatch(request(BatchWriteOptions.defaults())))
                    .expectError(BatchWriteException.class)
                    .verify();

        assertEquals(2, observations.size());
        assertEquals(BatchChunkResult.Status.CONFLICTED, observations.getFirst().chunkStatus());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, observations.getFirst().failureCategory());
        assertEquals(SqlExecutionResultKind.OPTIMISTIC_LOCK, observations.getFirst().resultKind());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, observations.get(1).failureCategory());
    }

    @Test
    void observesUnknownRecoveryResult() {
        List<BatchExecutionObservation> observations = new ArrayList<>();
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken("op-1",
                                                                                  3,
                                                                                  "batch_receipt",
                                                                                  "plan",
                                                                                  "payload",
                                                                                  1L,
                                                                                  1L);
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
                return Mono.just(BatchResolution.unknown(token));
            }
        };

        ReactiveSqlExecutor executor = delegate.withBatchObserver(observations::add);

        StepVerifier.create(executor.resolveUnknown(token))
                    .expectNextMatches(resolution -> resolution.status() == BatchResolution.Status.UNKNOWN)
                    .verifyComplete();

        assertEquals(1, observations.size());
        assertEquals(BatchExecutionEventType.RECOVERY, observations.getFirst().eventType());
        assertEquals(BatchResolution.Status.UNKNOWN, observations.getFirst().recoveryStatus());
        assertEquals(SqlFailureCategory.UNKNOWN, observations.getFirst().failureCategory());
        assertEquals(SqlExecutionResultKind.UNKNOWN, observations.getFirst().resultKind());
        assertEquals(3, observations.getFirst().chunkIndex());
    }

    @Test
    void turnsSynchronousDelegateFailuresIntoObservedReactiveErrors() {
        List<SqlExecutionObservation> observations = new ArrayList<>();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                throw new IllegalStateException("synchronous query failure");
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }
        };

        ReactiveSqlExecutor executor = delegate.withObserver(observations::add);

        StepVerifier.create(executor.query(SqlRequest.nativeSql("select 1", List.of())))
                    .expectErrorMatches(error -> error instanceof IllegalStateException
                            && "synchronous query failure".equals(error.getMessage()))
                    .verify();

        assertEquals(1, observations.size());
        assertEquals(SqlExecutionBackend.R2DBC, observations.getFirst().backend());
        assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
        assertEquals("select 1", observations.getFirst().sql());
    }

    /** 通用响应式 SQL 装饰器不得把 observer 包装的 JVM 致命错误当作普通旁路故障吞掉。 */
    @Test
    void propagatesVirtualMachineErrorNestedInSqlObserverFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("reactive SQL observer fatal");
        AtomicInteger callbacks = new AtomicInteger();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(1L);
            }
        };

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> delegate.withObserver(ignored -> {
            if (callbacks.getAndIncrement() == 0) {
                throw new IllegalStateException("observer wrapper", fatal);
            }
        }).rowsUpdated(SqlRequest.nativeSql("update Users set enabled = true", List.of())).block());

        assertSame(fatal, observed);
    }

    /** 通用响应式批量装饰器与 SQL 观测保持相同的嵌套 VME 传播边界。 */
    @Test
    void propagatesVirtualMachineErrorNestedInBatchObserverFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("reactive batch observer fatal");
        AtomicInteger callbacks = new AtomicInteger();
        BatchWriteRequest batch = request(BatchWriteOptions.atomic(2));
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Mono.just(BatchWriteResult.from(
                        request.options().mode(),
                        List.of(BatchChunkResult.committed(0, 0, 2, 2))));
            }
        };

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> delegate.withBatchObserver(ignored -> {
            if (callbacks.getAndIncrement() == 0) {
                throw new IllegalStateException("observer wrapper", fatal);
            }
        }).writeBatch(batch).block());

        assertSame(fatal, observed);
    }

    @Test
    void forwardsExplicitExecutionOptionsThroughObservation() {
        AtomicReference<SqlExecutionOptions> queryOptions = new AtomicReference<>();
        AtomicReference<SqlExecutionOptions> updateOptions = new AtomicReference<>();
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
                queryOptions.set(options);
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
                updateOptions.set(options);
                return Mono.just(1L);
            }
        };
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                                                         .withConnectionAcquireTimeout(
                                                                 java.time.Duration.ofMillis(250));
        ReactiveSqlExecutor observed = delegate.withObserver(ignored -> { });
        SqlRequest request = new SqlRequest("select 1 where ? = ?", List.of(1, 1));

        StepVerifier.create(observed.query(request, options)).verifyComplete();
        StepVerifier.create(observed.rowsUpdated(request, options)).expectNext(1L).verifyComplete();

        assertEquals(options, queryOptions.get());
        assertEquals(options, updateOptions.get());
    }

    @Test
    void forwardsExternalTransactionThroughComposedExecutorDecorators() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> null);
        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(connection, "primary");
        ReactiveSqlExecutor delegate = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.just(0L);
            }

            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return Mono.just(transaction);
            }
        };

        AtomicReference<SqlTransactionSource> observedSource = new AtomicReference<>();
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public boolean requiresTransactionSource() {
                return true;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只关心带事务来源的新回调。
            }

            @Override
            public void onExecution(SqlExecutionObservation observation,
                                    SqlTransactionSource transactionSource) {
                observedSource.set(transactionSource);
            }
        };
        // 正式客户端会依次叠加这些包装器。事务上下文必须穿透每一层，DDL、观测和批量保护才能看见上层事务。
        ReactiveSqlExecutor executor = delegate.withDefaultExecutionOptions(SqlExecutionOptions.safeDefaults())
                                               .withBatchMemoryLimits(BatchMemoryLimits.defaults())
                                               .withObserver(observer);

        StepVerifier.create(executor.currentTransaction())
                    .assertNext(actual -> assertSame(transaction, actual))
                    .verifyComplete();
        StepVerifier.create(executor.rowsUpdated(new SqlRequest("update Users set enabled = ?", List.of(true))))
                    .expectNext(0L)
                    .verifyComplete();
        assertEquals(SqlTransactionSource.EXTERNAL, observedSource.get());
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return new BatchWriteRequest("insert into Users(id, name) values(?, ?)",
                                     2,
                                     List.of(String.class, String.class),
                                     SqlBindMarkerStyle.CANONICAL,
                                     Flux.just(new Object[]{"u1", "王"}, new Object[]{"u2", "李"}),
                                     options);
    }
}
