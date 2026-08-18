package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 用可控的 R2DBC 连接验证批量事务状态。这里不依赖真实数据库，专门覆盖很难稳定制造的超时和回滚失败。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class R2dbcBatchWriterTest {

    /** 业务执行时间不能提前消耗结果确定后才开始计算的资源清理预算。 */
    @Test
    void startsBatchCleanupBudgetWhenCleanupActuallyBegins() {
        StepVerifier.withVirtualTime(() -> {
            Connection connection = proxy(Connection.class, (ignored, method, ignoredArgs) ->
                    defaultValue(method));
            R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(
                    connection, Duration.ofMillis(50));
            return Mono.delay(Duration.ofSeconds(1))
                       .then(Mono.defer(() -> handle.cleanupDeadline()
                                                   .protect(Mono.delay(Duration.ofMillis(20)).then())));
        })
                    .thenAwait(Duration.ofMillis(1020))
                    .verifyComplete();
    }

    /** R2DBC 受保护批量更新必须把预读 owner 重新附加到实际业务 SQL。 */
    @Test
    void restrictsProtectedBatchUpdateToTheOwnerReadBeforeTheWrite() {
        String writeSql = "update Users set contact = ? where label = ?";
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest(writeSql, List.of(new byte[]{9}, "target")),
                new SqlRequest("select id from Users where label = ?", List.of("target")),
                List.of("id"), Map.of(), "id = ?",
                "delete from UserTokens where id = ? and field_tag = ?",
                "insert into UserTokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{2}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{new byte[]{9}, "target"}, work);
        BatchWriteRequest request = new BatchWriteRequest(
                writeSql, 2, List.of(byte[].class, String.class), SqlBindMarkerStyle.CANONICAL,
                Flux.fromIterable(List.<Object[]>of(row)), BatchWriteOptions.atomic(1),
                com.flying.orm.rdb.batch.BatchRowCountPolicy.EXACTLY_ONE);
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.ownerId = 1L;

        StepVerifier.create(R2dbcSqlExecutor.create(factory).writeProtectedBatch(request))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.COMMITTED)
                    .verifyComplete();

        assertTrue(factory.statementSql.contains(writeSql + " and ((id = ?))"));
    }

    /** R2DBC 批量侧索引 INSERT 返回零影响行时必须回滚同分片业务写。 */
    @Test
    void rollsBackProtectedBatchWhenContainsTokenInsertAffectsNoRow() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into Users(id, contact) values (?, ?)",
                               List.of(1L, new byte[]{9})),
                null, List.of("id"), Map.of("id", 1L), "id = ?",
                "delete from protected_customer_tokens where id = ? and field_tag = ?",
                "insert into protected_customer_tokens(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("contact", List.of(new byte[]{1}))));
        Object[] row = ProtectedBatchRows.extend(new Object[]{1L, new byte[]{9}}, work);
        BatchWriteRequest request = new BatchWriteRequest(
                work.writeRequest().sql(), 2, List.of(Long.class, byte[].class),
                SqlBindMarkerStyle.CANONICAL, Flux.fromIterable(List.<Object[]>of(row)),
                BatchWriteOptions.atomic(1));
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.zeroRowsSql = work.insertSql();

        StepVerifier.create(R2dbcSqlExecutor.create(factory).writeProtectedBatch(request))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException failure = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.ROLLED_BACK, failure.result().status());
                    })
                    .verify();

        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.closed.get());
    }

    /**
     * 已经开始提交后再超时，数据库是否提交成功无法判断，只能返回 UNKNOWN。
     */
    @Test
    void atomicTimeoutDuringCommitIsUnknown() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangCommit = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1)
                                                                       .withTimeout(Duration.ofMillis(10)))))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.UNKNOWN, batchError.result().status());
                    })
                    .verify();

        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /**
     * INDEPENDENT 分片的 COMMIT 回执超时同样不能伪装为普通失败；汇总入口必须保留 UNKNOWN 结果。
     */
    @Test
    void independentTimeoutDuringCommitIsUnknown() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangCommit = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.independent(1)
                                                                       .withTimeout(Duration.ofMillis(10)))))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.UNKNOWN, batchError.result().status());
                        assertEquals(BatchChunkResult.Status.UNKNOWN,
                                     batchError.result().chunks().getFirst().status());
                    })
                    .verify();

        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** BEGIN 回执超时前没有可确认的回滚，结果必须标记 UNKNOWN，并隔离连接而不是归还连接池。 */
    @Test
    void atomicTimeoutBeforeTransactionIsUnknownWithoutRollback() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangBegin = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1)
                                                                       .withTimeout(Duration.ofMillis(10)))))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.UNKNOWN, batchError.result().status());
                        assertEquals(BatchChunkResult.Status.UNKNOWN,
                                     batchError.result().chunks().getFirst().status());
                        assertNull(batchError.result().chunks().getFirst().recoveryToken());
                    })
                    .verify();

        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** 提交确认期间取消订阅时事务结果未知，连接必须物理淘汰，不能按普通关闭重新放回池中。 */
    @Test
    void atomicCancellationDuringCommitInvalidatesConnection() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangCommit = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations))
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                     cleanupObservations.getFirst().phase());
        assertFalse(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** 取消清理中的失效 fatal 必须原样出站，且同一物理连接只能执行一次失效。 */
    @Test
    void cancelDoesNotRepeatInvalidationWhenInvalidationFailsFatally() {
        AtomicInteger invalidations = new AtomicInteger();
        OutOfMemoryError fatal = new OutOfMemoryError("invalidation fatal");
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        R2dbcConnectionInvalidator invalidator = new R2dbcConnectionInvalidator() {
            @Override
            public Publisher<Void> close(Connection connection) {
                return Mono.empty();
            }

            @Override
            public Publisher<Void> invalidate(Connection connection) {
                invalidations.incrementAndGet();
                return Mono.error(fatal);
            }
        };
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                factory,
                SqlExecutionObserver.noop(),
                invalidator,
                R2dbcTransactionParticipant.none());
        R2dbcBatchConnectionHandle resource = new R2dbcBatchConnectionHandle(
                Mono.from(factory.create()).block());
        resource.markCommitting();

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> lifecycle.cancel(resource, "atomic").block());

        assertSame(fatal, observed);
        assertEquals(1, invalidations.get());
    }

    /** 行内 LOB 释放已经失败时，事务终态明确也不能把污染连接正常归池。 */
    @Test
    void confirmedOutcomeInvalidatesConnectionAfterCapturedLargeObjectDiscardFailure() {
        AtomicInteger invalidations = new AtomicInteger();
        IllegalStateException primary = new IllegalStateException("generated key read failed");
        IllegalStateException cleanup = new IllegalStateException("clob discard failed");
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                factory,
                SqlExecutionObserver.noop(),
                invalidator(invalidations),
                R2dbcTransactionParticipant.none());
        R2dbcBatchConnectionHandle resource = new R2dbcBatchConnectionHandle(
                Mono.from(factory.create()).block());
        Clob clob = new Clob() {
            @Override
            public Publisher<CharSequence> stream() {
                return Flux.empty();
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.error(cleanup);
            }
        };

        resource.largeObjects().discardCaptured(
                List.of(clob), SqlExecutionOptions.safeDefaults(), primary).block();
        resource.markActive();
        resource.markCommitted();
        lifecycle.closeAfterOutcome(resource).block();

        assertTrue(reaches(primary, cleanup));
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** 同一个分片流的每次订阅都必须从零开始编号，不能共享前一次或并发订阅的计数器。 */
    @Test
    void chunkIndexesAreIsolatedAcrossRepeatedAndConcurrentSubscriptions() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(new ControlledConnectionFactory()));
        Flux<Integer> indexes = chunks.chunks(request(BatchWriteOptions.atomic(1),
                                                       new Object[]{"u1"}, new Object[]{"u2"}))
                                     .map(R2dbcBatchWriterChunks.BatchChunk::chunkIndex);

        StepVerifier.create(indexes)
                    .expectNext(0, 1)
                    .verifyComplete();
        StepVerifier.create(indexes)
                    .expectNext(0, 1)
                    .verifyComplete();
        StepVerifier.create(Flux.merge(indexes.subscribeOn(Schedulers.parallel()),
                                      indexes.subscribeOn(Schedulers.parallel())).collectList())
                    .assertNext(actual -> assertEquals(List.of(0, 0, 1, 1),
                                                         actual.stream().sorted().toList()))
                    .verifyComplete();
    }

    /** 超大首行必须在进入整片缓冲前失败，不能继续向输入流索取第二行。 */
    @Test
    void rejectsAnOversizedRowBeforeBufferingTheRestOfItsChunk() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(new ControlledConnectionFactory()));
        Publisher<Object[]> rows = Flux.concat(
                Mono.just(new Object[]{new byte[1_024]}),
                Mono.error(new AssertionError("second row must not be consumed")));
        BatchWriteOptions options = BatchWriteOptions.atomic(2).withMemoryLimits(10L, 128L, 10);

        StepVerifier.create(chunks.chunks(request(options, rows)))
                    .expectError(BatchMemoryLimitExceededException.class)
                    .verify();
    }

    /** R2DBC 所有权边界压紧 ByteBuffer 后，tiny slice 应按实际可读载荷合法进入分片。 */
    @Test
    void acceptsCompactedByteBufferWithinChunkBudget() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(new ControlledConnectionFactory()));
        ByteBuffer buffer = ByteBuffer.allocate(1_024);
        buffer.position(buffer.capacity() - 1);
        BatchWriteOptions options = BatchWriteOptions.atomic(1).withMemoryLimits(1L, 128L, 1);

        StepVerifier.create(chunks.chunks(request(options, new Object[]{buffer})))
                    .assertNext(chunk -> assertEquals(
                            1, assertInstanceOf(ByteBuffer.class, chunk.rows().getFirst()[0]).capacity()))
                    .verifyComplete();
    }

    /** 分片流只在下游索取一个分片时拉取该分片的行数，不能预取后续分片。 */
    @Test
    void chunksDoNotRequestRowsBeyondTheDemandedChunk() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(new ControlledConnectionFactory()));
        AtomicLong requestedRows = new AtomicLong();
        AtomicInteger cancellations = new AtomicInteger();
        Publisher<Object[]> rows = new DemandTrackingRows(List.of(new Object[]{"u0"},
                                                                  new Object[]{"u1"},
                                                                  new Object[]{"u2"},
                                                                  new Object[]{"u3"}),
                                                        requestedRows,
                                                        cancellations);

        StepVerifier.create(chunks.chunks(request(BatchWriteOptions.atomic(2), rows)), 1)
                    .expectNextCount(1)
                    .thenCancel()
                    .verify();

        assertEquals(2L, requestedRows.get());
        assertEquals(1, cancellations.get());
    }

    /** 上游复用参数数组及其嵌套可变值时，每次 onNext 都要立即完成深快照。 */
    @Test
    void snapshotsEachRowBeforeBufferingWhenPublisherReusesArray() {
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(
                R2dbcBindMarkers.from(new ControlledConnectionFactory()));
        byte[] binary = new byte[1];
        StringBuilder text = new StringBuilder();
        Object[] nested = new Object[]{binary, text};
        Object[] reused = new Object[]{nested};
        Publisher<Object[]> rows = Flux.range(0, 2).map(index -> {
            binary[0] = index.byteValue();
            text.setLength(0);
            text.append("u").append(index);
            return reused;
        });

        StepVerifier.create(chunks.chunks(request(BatchWriteOptions.atomic(2), rows)))
                    .assertNext(chunk -> {
                        Object[] first = assertInstanceOf(Object[].class, chunk.rows().get(0)[0]);
                        Object[] second = assertInstanceOf(Object[].class, chunk.rows().get(1)[0]);
                        assertArrayEquals(new byte[]{0}, assertInstanceOf(byte[].class, first[0]));
                        assertArrayEquals(new byte[]{1}, assertInstanceOf(byte[].class, second[0]));
                        assertEquals("u0", first[1]);
                        assertEquals("u1", second[1]);
                    })
                    .verifyComplete();
    }

    /** 已确认提交的原子批量不能因为随后关闭连接失败而被改写成失败。 */
    @Test
    void atomicCommitRemainsSuccessfulWhenConnectionCloseFails() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.failClose = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations))
                .withObserver(new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只关心事务结果确定后的清理故障。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                cleanupObservations.add(observation);
            }
        });

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1))))
                    .expectNextMatches(result -> result.status() == BatchWriteResult.Status.COMMITTED
                            && result.affectedRows() == 1L)
                    .verifyComplete();

        assertEquals(1, factory.closed.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                     cleanupObservations.getFirst().operation());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
    }

    /**
     * 回执模式已经生成了恢复令牌。即使业务失败后的回滚确认也失败，这个令牌仍然是上层追查结果的依据。
     */
    @Test
    void rollbackFailureKeepsReceiptRecoveryToken() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.failUserWrite = true;
        factory.failRollback = true;
        factory.failClose = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1).withReceipt("rollback-op"))))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.UNKNOWN, batchError.result().status());
                        assertNotNull(batchError.result().chunks().getFirst().recoveryToken());
                    })
                    .verify();

        // 回执查询 close 失败和 rollback UNKNOWN 的事务连接都必须物理淘汰。
        assertEquals(2, invalidations.get());
        assertEquals(1, factory.closed.get());
    }

    /**
     * 独立分片的订阅被取消时，当前活动事务要先回滚再释放连接。
     */
    @Test
    void cancellingIndependentChunkRollsBackActiveTransaction() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangUserWrite = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        StepVerifier.create(executor.writeBatchChunks(request(BatchWriteOptions.independent(1))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.closed.get());
    }

    /** 独立分片在提交确认期间取消时也必须淘汰未知状态连接。 */
    @Test
    void independentCancellationDuringCommitInvalidatesConnection() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangCommit = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations))
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.writeBatchChunks(request(BatchWriteOptions.independent(1))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                     cleanupObservations.getFirst().phase());
        assertFalse(cleanupObservations.getFirst().outcomeConfirmed());
    }

    /** 原子批量取消后的 rollback 失败必须发布未确认清理事实并物理失效连接。 */
    @Test
    void atomicCancellationRollbackFailureIsObservedAndInvalidated() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangUserWrite = true;
        factory.failRollback = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations))
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
        assertUnconfirmedBatchCleanup(cleanupObservations);
    }

    /** 独立分片取消后的 rollback 失败同样不能吞掉，必须淘汰当前分片连接。 */
    @Test
    void independentCancellationRollbackFailureIsObservedAndInvalidated() {
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangUserWrite = true;
        factory.failRollback = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations))
                .withObserver(cleanupObserver(cleanupObservations));

        StepVerifier.create(executor.writeBatchChunks(request(BatchWriteOptions.independent(1))))
                    .thenAwait(Duration.ofMillis(10))
                    .thenCancel()
                    .verify();

        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
        assertUnconfirmedBatchCleanup(cleanupObservations);
    }

    /**
     * 并发分片可能后发先至。汇总失败时必须沿用真正失败分片的位置，不能根据已完成结果猜编号。
     */
    @Test
    void independentSummaryKeepsActualFailedChunkPosition() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.hangFirstUserWrite = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        BatchWriteOptions options = BatchWriteOptions.independent(1, 2).withTimeout(Duration.ofMillis(20));

        StepVerifier.create(executor.writeBatch(request(options,
                                                        new Object[]{"u1"},
                                                        new Object[]{"u2"})))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        BatchChunkResult failed = batchError.result().chunks().stream()
                                                            .filter(chunk -> chunk.status()
                                                                    == BatchChunkResult.Status.FAILED)
                                                            .findFirst()
                                                            .orElseThrow();
                        assertEquals(0, failed.chunkIndex());
                        assertEquals(0L, failed.startOffset());
                    })
                    .verify();
    }

    /** 批量输入等待由 Publisher 或上层控制，SQL 兜底时间不覆盖连接获取前的输入阶段。 */
    @Test
    void independentInputWaitingIsLeftToPublisherAndUpperLayer() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        StepVerifier.withVirtualTime(() -> {
            Publisher<Object[]> rows = Flux.concat(Mono.just(new Object[]{"u1"}),
                                                    Mono.delay(Duration.ofSeconds(8))
                                                        .map(ignored -> new Object[]{"u2"}),
                                                    Mono.never());
            BatchWriteRequest request = request(BatchWriteOptions.independent(1)
                                                                 .withTimeout(Duration.ofSeconds(10)),
                                                rows);
            return executor.writeBatch(request);
        })
                    .thenAwait(Duration.ofSeconds(11))
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));
        assertEquals(2, factory.commitAttempts.get());
    }

    /**
     * 返回的 Mono 是冷 Publisher。重复订阅时，每次都必须有自己独立的汇总状态。
     */
    @Test
    void independentSummaryDoesNotShareStateAcrossSubscriptions() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        Mono<BatchWriteResult> operation = executor.writeBatch(request(BatchWriteOptions.independent(1)));

        StepVerifier.create(operation)
                    .expectNextMatches(result -> result.chunks().size() == 1 && result.inputCount() == 1)
                    .verifyComplete();
        StepVerifier.create(operation)
                    .expectNextMatches(result -> result.chunks().size() == 1 && result.inputCount() == 1)
                    .verifyComplete();
    }

    /** 持续输入可以跨越 SQL 兜底时长，只要每个已获连接的独立分片都在自己的时限内完成。 */
    @Test
    void continuouslyEmittingInputCanOutlivePerTransactionFallback() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        StepVerifier.withVirtualTime(() -> {
            Publisher<Object[]> rows = Flux.interval(Duration.ZERO, Duration.ofSeconds(8))
                                                 .take(3)
                                                 .map(index -> new Object[]{"u" + index});
            BatchWriteRequest request = request(BatchWriteOptions.independent(1)
                                                                 .withTimeout(Duration.ofSeconds(10)),
                                                rows);
            return executor.writeBatch(request);
        })
                    .thenAwait(Duration.ofSeconds(17))
                    .assertNext(result -> {
                        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
                        assertEquals(3, result.inputCount());
                    })
                    .verifyComplete();
        assertEquals(3, factory.commitAttempts.get());
    }

    /** 外部事务解析与输入等待不消耗 ORM 的 SQL 兜底时间，连接可用后才开始事务执行计时。 */
    @Test
    void batchTimeoutStartsAfterTransactionResolutionAndInputConsumption() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        AtomicInteger transactionLookups = new AtomicInteger();
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withTransactionParticipant(() -> {
                    transactionLookups.incrementAndGet();
                    return Mono.delay(Duration.ofMillis(60)).then(Mono.empty());
                });
        StepVerifier.withVirtualTime(() -> {
            Publisher<Object[]> rows = Mono.delay(Duration.ofMillis(60))
                                           .map(ignored -> new Object[]{"u1"});
            BatchWriteRequest request = request(BatchWriteOptions.atomic(1)
                                                                 .withTimeout(Duration.ofMillis(100)),
                                                rows);
            return executor.writeBatch(request);
        })
                    .thenAwait(Duration.ofMillis(121))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED, result.status()))
                    .verifyComplete();
        assertEquals(1, transactionLookups.get());
        assertEquals(1, factory.commitAttempts.get());
    }

    /** 回执 SQL 完成后，下一次连接池排队仍由连接池或上层控制，不继承 ORM 的 SQL 兜底时间。 */
    @Test
    void receiptLookupLeavesFollowupConnectionWaitingToPool() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.receiptLookupDelay = Duration.ofSeconds(8);
        factory.hangAfterFirstConnection = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        BatchWriteRequest request = request(BatchWriteOptions.atomic(1)
                                                             .withReceipt("deadline-op")
                                                             .withTimeout(Duration.ofSeconds(10)));

        StepVerifier.withVirtualTime(() -> executor.writeBatch(request))
                    .thenAwait(Duration.ofSeconds(11))
                    .thenCancel()
                    .verify(Duration.ofSeconds(1));
        assertEquals(2, factory.connections.get());
    }

    /** 已提交回执的 payload 重放只消费输入，不应把批量 SQL 兜底时限错误套到 Publisher 等待阶段。 */
    @Test
    void atomicReceiptReplayLeavesInputWaitingToPublisher() {
        Object[] row = new Object[]{"u1"};
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.preexistingReceiptPayload = new BatchPayloadHasher().hashRows(List.<Object[]>of(row));
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);
        BatchWriteOptions options = BatchWriteOptions.atomic(1)
                                                     .withReceipt("existing-receipt")
                                                     .withTimeout(Duration.ofMillis(10));

        StepVerifier.withVirtualTime(() -> executor.writeBatch(request(
                        options,
                        Mono.delay(Duration.ofMillis(100)).map(ignored -> row))))
                    .thenAwait(Duration.ofMillis(101))
                    .assertNext(result -> assertEquals(BatchWriteResult.Status.COMMITTED, result.status()))
                    .verifyComplete();

        assertEquals(0, factory.commitAttempts.get());
    }

    @Test
    void atomicBeginFailureDoesNotAttemptRollback() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.failBegin = true;
        factory.failRollback = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatch(request(BatchWriteOptions.atomic(1))))
                    .expectErrorSatisfies(error -> {
                        BatchWriteException batchError = assertInstanceOf(BatchWriteException.class, error);
                        assertEquals(BatchWriteResult.Status.UNKNOWN, batchError.result().status());
                        assertEquals(BatchChunkResult.Status.UNKNOWN,
                                     batchError.result().chunks().getFirst().status());
                        assertNull(batchError.result().chunks().getFirst().recoveryToken());
                    })
                    .verify();

        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    @Test
    void independentBeginFailureDoesNotAttemptRollback() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.failBegin = true;
        factory.failRollback = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        StepVerifier.create(executor.writeBatchChunks(request(BatchWriteOptions.independent(1))))
                    .assertNext(result -> {
                        assertEquals(BatchChunkResult.Status.UNKNOWN, result.status());
                        assertNull(result.recoveryToken());
                    })
                    .verifyComplete();

        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /**
     * 驱动把虚拟机级错误作为 onError 信号交给批量写入时，ATOMIC 必须先确认 rollback，随后仍原样暴露该 fatal。
     */
    @Test
    void atomicFatalOperationIsPropagatedAfterConfirmedRollback() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("atomic operation fatal");
        factory.userWriteError = fatal;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> executor.writeBatch(request(BatchWriteOptions.atomic(1)))
                                                                .block());

        assertSame(fatal, observed);
        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.closed.get());
    }

    /**
     * rollback 回执丢失时，内部 UNKNOWN 结果必须仍保留操作 fatal，最外层恢复该 fatal 而不能降级为普通批量异常。
     */
    @Test
    void atomicRollbackFailureRetainsFatalOperationInUnknownContext() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("atomic operation fatal");
        factory.userWriteError = fatal;
        factory.rollbackError = new IllegalStateException("atomic rollback acknowledgement lost");
        R2dbcBatchWriter writer = batchWriter(factory);

        BatchWriteException error = assertThrows(BatchWriteException.class,
                                                  () -> writer.write(request(BatchWriteOptions.atomic(1))).block());

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertContainsThrowableIdentity(error, fatal);
        assertSame(fatal, ReactiveSqlExecutionProtection.translate(error));
        assertEquals(1, factory.rollbacks.get());
    }

    /**
     * INDEPENDENT 分片遇到 fatal 也只能在 rollback 已确认后向外传播，不能把已回滚结果伪装成成功信号。
     */
    @Test
    void independentFatalOperationIsPropagatedAfterConfirmedRollback() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("independent operation fatal");
        factory.userWriteError = fatal;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                                                  () -> executor.writeBatchChunks(
                                                                  request(BatchWriteOptions.independent(1)))
                                                                .next()
                                                                .block());

        assertSame(fatal, observed);
        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.closed.get());
    }

    /**
     * INDEPENDENT rollback 失败时，UNKNOWN BatchWriteException 既保留原 fatal，也不能被后续汇总改写为 FAILED。
     */
    @Test
    void independentRollbackFailureRetainsFatalOperationInUnknownContext() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("independent operation fatal");
        factory.userWriteError = fatal;
        factory.rollbackError = new IllegalStateException("independent rollback acknowledgement lost");
        R2dbcBatchWriter writer = batchWriter(factory);

        BatchWriteException error = assertThrows(
                BatchWriteException.class,
                () -> writer.write(request(BatchWriteOptions.independent(1))).block());

        assertEquals(BatchWriteResult.Status.UNKNOWN, error.result().status());
        assertContainsThrowableIdentity(error, fatal);
        assertSame(fatal, ReactiveSqlExecutionProtection.translate(error));
        assertEquals(1, factory.rollbacks.get());
    }

    /**
     * 普通业务异常仍沿用既有的独立分片失败结果；fatal 专项不能改变这个可恢复路径。
     */
    @Test
    void independentOrdinaryFailureStillEmitsFailedChunkAfterConfirmedRollback() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.failUserWrite = true;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory);

        BatchChunkResult result = executor.writeBatchChunks(request(BatchWriteOptions.independent(1))).next().block();

        assertEquals(BatchChunkResult.Status.FAILED, result.status());
        assertEquals(1, factory.rollbacks.get());
    }

    /** BEGIN 未确认时的 fatal 必须先触发 fail-closed cleanup，再从公共入口按同一对象传播。 */
    @Test
    void independentBeginFatalIsPropagatedAfterUncertainConnectionIsInvalidated() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("independent begin fatal");
        factory.beginError = fatal;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.writeBatchChunks(request(BatchWriteOptions.independent(1))).next().block());

        assertSame(fatal, observed);
        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** COMMIT 已发出后的 fatal 不能回滚猜测，必须 UNKNOWN 并隔离连接后才原样向外传播。 */
    @Test
    void independentCommitFatalIsPropagatedAfterUncertainConnectionIsInvalidated() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("independent commit fatal");
        factory.commitError = fatal;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.writeBatchChunks(request(BatchWriteOptions.independent(1))).next().block());

        assertSame(fatal, observed);
        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** 普通操作失败而 rollback 发出 fatal 时，rollback 的未知回执优先，原操作错误仍保留在内部 UNKNOWN 结果里。 */
    @Test
    void independentRollbackFatalIsPropagatedAfterUncertainConnectionIsInvalidated() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        IllegalStateException operation = new IllegalStateException("independent operation failed");
        OutOfMemoryError fatal = new OutOfMemoryError("independent rollback fatal");
        factory.userWriteError = operation;
        factory.rollbackError = fatal;
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.writeBatchChunks(request(BatchWriteOptions.independent(1))).next().block());

        assertSame(fatal, observed);
        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** 普通 COMMIT 回执错误仍按既有规则发出 UNKNOWN chunk，而不是进入 fatal 专用异常路径。 */
    @Test
    void independentOrdinaryCommitFailureStillEmitsUnknownChunk() {
        AtomicInteger invalidations = new AtomicInteger();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.commitError = new IllegalStateException("independent commit acknowledgement lost");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(invalidator(invalidations));

        BatchChunkResult result = executor.writeBatchChunks(request(BatchWriteOptions.independent(1))).next().block();

        assertEquals(BatchChunkResult.Status.UNKNOWN, result.status());
        assertEquals(0, factory.rollbacks.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, factory.closed.get());
    }

    /** 未知原子提交必须先释放旧事务连接，再在 confirmTimeout 内用完整回执确认提交事实。 */
    @Test
    void atomicReceiptConfirmationRunsAfterUnknownTransactionConnectionCleanup() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.commitError = new IllegalStateException("atomic commit acknowledgement lost");
        factory.committedReceiptAfterCommitFailure = true;

        BatchWriteResult result = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(closingInvalidator())
                .writeBatch(request(BatchWriteOptions.atomic(1)
                                                       .withReceipt("confirm-atomic", Duration.ofSeconds(1))))
                .block();

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, factory.confirmReceiptReads.get());
        assertTrue(factory.confirmationStartedAfterCleanup);
    }

    /** INDEPENDENT 分片也只能在旧事务连接清理完成后，把严格匹配的回执提升为 COMMITTED。 */
    @Test
    void independentReceiptConfirmationPromotesUnknownChunkAfterCleanup() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.commitError = new IllegalStateException("independent commit acknowledgement lost");
        factory.committedReceiptAfterCommitFailure = true;

        BatchChunkResult result = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(closingInvalidator())
                .writeBatchChunks(request(BatchWriteOptions.independent(1)
                                                              .withReceipt("confirm-independent",
                                                                           Duration.ofSeconds(1))))
                .single()
                .block();

        assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
        assertEquals(1, factory.confirmReceiptReads.get());
        assertTrue(factory.confirmationStartedAfterCleanup);
    }

    /** 同一 operationId 的并发 ATOMIC 请求在预留冲突后必须重读首个请求已提交的回执。 */
    @Test
    void atomicReceiptReservationConflictReplaysCommittedFactAfterRollback() {
        Object[] row = new Object[]{"u1"};
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.duplicateReceiptReserve = true;
        factory.receiptPayloadAfterReservationConflict = new BatchPayloadHasher()
                .hashRows(List.<Object[]>of(row));

        BatchWriteResult result = R2dbcSqlExecutor.create(factory)
                .writeBatch(request(BatchWriteOptions.atomic(1).withReceipt("concurrent-atomic"),
                                    Flux.<Object[]>just(row)))
                .block();

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.reservationReplayReads.get());
        assertTrue(factory.reservationReplayStartedAfterCleanup);
    }

    /** INDEPENDENT 分片的预留冲突同样要在本分片回滚和连接归还后重读已提交回执。 */
    @Test
    void independentReceiptReservationConflictReplaysCommittedFactAfterRollback() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.duplicateReceiptReserve = true;
        factory.receiptPayloadAfterReservationConflict = "unused-by-exact-token-read";

        BatchChunkResult result = R2dbcSqlExecutor.create(factory)
                .writeBatchChunks(request(BatchWriteOptions.independent(1)
                                                           .withReceipt("concurrent-independent")))
                .single()
                .block();

        assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
        assertEquals(1, factory.rollbacks.get());
        assertEquals(1, factory.reservationReplayReads.get());
        assertTrue(factory.reservationReplayStartedAfterCleanup);
    }

    /** confirmTimeout 为零表示明确关闭主动确认，未知提交必须保持 UNKNOWN。 */
    @Test
    void zeroReceiptConfirmationTimeoutKeepsAtomicCommitUnknown() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        factory.commitError = new IllegalStateException("atomic commit acknowledgement lost");
        factory.committedReceiptAfterCommitFailure = true;

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> R2dbcSqlExecutor.create(factory)
                        .withConnectionInvalidator(closingInvalidator())
                        .writeBatch(request(BatchWriteOptions.atomic(1)
                                                               .withReceipt("confirm-disabled", Duration.ZERO)))
                        .block());

        assertEquals(BatchWriteResult.Status.UNKNOWN, failure.result().status());
        assertEquals(0, factory.confirmReceiptReads.get());
    }

    /** 原子提交失败图中的 JVM 致命错误必须原样传播，不能被随后可命中的回执改写为已提交。 */
    @Test
    void atomicReceiptConfirmationCannotHideCommitVirtualMachineError() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("atomic commit fatal");
        factory.commitError = new IllegalStateException("driver wrapper", fatal);
        factory.committedReceiptAfterCommitFailure = true;

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> R2dbcSqlExecutor.create(factory)
                        .withConnectionInvalidator(closingInvalidator())
                        .writeBatch(request(BatchWriteOptions.atomic(1)
                                                               .withReceipt("fatal-confirm-atomic",
                                                                            Duration.ofSeconds(1))))
                        .block());

        assertSame(fatal, observed);
        assertEquals(0, factory.confirmReceiptReads.get());
    }

    /** 独立分片提交失败图中的 JVM 致命错误同样必须在查询回执前原样传播。 */
    @Test
    void independentReceiptConfirmationCannotHideCommitVirtualMachineError() {
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError fatal = new OutOfMemoryError("independent commit fatal");
        factory.commitError = new IllegalStateException("driver wrapper", fatal);
        factory.committedReceiptAfterCommitFailure = true;

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> R2dbcSqlExecutor.create(factory)
                        .withConnectionInvalidator(closingInvalidator())
                        .writeBatchChunks(request(BatchWriteOptions.independent(1)
                                                                     .withReceipt("fatal-confirm-independent",
                                                                                  Duration.ofSeconds(1))))
                        .collectList()
                        .block());

        assertSame(fatal, observed);
        assertEquals(0, factory.confirmReceiptReads.get());
    }

    /** 已确认原子提交后的 close/invalidate 双 fatal 不能被吞成 COMMITTED，也不能形成 Throwable 环。 */
    @Test
    void atomicCompletedBatchPropagatesPrimaryCleanupFatalWithoutThrowableCycle() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError closeFatal = new OutOfMemoryError("atomic close fatal");
        OutOfMemoryError invalidationFatal = new OutOfMemoryError("atomic invalidation fatal");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations, closeFatal, invalidationFatal))
                .withObserver(cleanupObserver(cleanupObservations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.writeBatch(request(BatchWriteOptions.atomic(1))).block());

        assertSame(closeFatal, observed);
        assertEquals(1, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     cleanupObservations.getFirst().phase());
        assertFalse(reaches(invalidationFatal, closeFatal));
    }

    /** 已确认原子提交的 close fatal 在 invalidate 成功后仍必须取代 COMMITTED 结果。 */
    @Test
    void atomicCompletedBatchPropagatesCloseFatalWhenInvalidationSucceeds() {
        AtomicInteger closeAttempts = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        List<ResourceCleanupObservation> cleanupObservations = new ArrayList<>();
        ControlledConnectionFactory factory = new ControlledConnectionFactory();
        OutOfMemoryError closeFatal = new OutOfMemoryError("atomic close fatal");
        ReactiveSqlExecutor executor = R2dbcSqlExecutor.create(factory)
                .withConnectionInvalidator(cleanupFailingInvalidator(
                        closeAttempts, invalidations, closeFatal, null))
                .withObserver(cleanupObserver(cleanupObservations));

        OutOfMemoryError observed = assertThrows(
                OutOfMemoryError.class,
                () -> executor.writeBatch(request(BatchWriteOptions.atomic(1))).block());

        assertSame(closeFatal, observed);
        assertEquals(1, closeAttempts.get());
        assertEquals(1, invalidations.get());
        assertEquals(1, cleanupObservations.size());
    }

    private static BatchWriteRequest request(BatchWriteOptions options) {
        return request(options, new Object[]{"u1"});
    }

    private static BatchWriteRequest request(BatchWriteOptions options, Object[]... rows) {
        return request(options, Flux.fromArray(rows));
    }

    private static BatchWriteRequest request(BatchWriteOptions options, Publisher<Object[]> rows) {
        return new BatchWriteRequest("insert into Users(id) values(?)",
                                     1,
                                     List.of(String.class),
                                     SqlBindMarkerStyle.CANONICAL,
                                     rows,
                                     options);
    }

    private static R2dbcBatchWriter batchWriter(ControlledConnectionFactory factory) {
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(factory);
        R2dbcConnectionInvalidator connectionInvalidator = invalidator(new AtomicInteger());
        return new R2dbcBatchWriter(factory,
                                    new BatchReceiptStore(factory,
                                                          bindMarkers,
                                                          SqlExecutionObserver.noop(),
                                                          connectionInvalidator),
                                    bindMarkers,
                                    SqlExecutionObserver.noop(),
                                    BatchExecutionObserver.noop(),
                                    connectionInvalidator,
                                    R2dbcTransactionParticipant.none());
    }

    private static void assertContainsThrowableIdentity(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        throw new AssertionError("expected throwable graph to retain the supplied fatal");
    }

    private static R2dbcConnectionInvalidator invalidator(AtomicInteger invalidations) {
        return new R2dbcConnectionInvalidator() {
            @Override
            public Publisher<Void> close(Connection connection) {
                return connection.close();
            }

            @Override
            public Publisher<Void> invalidate(Connection connection) {
                invalidations.incrementAndGet();
                return Mono.empty();
            }
        };
    }

    private static R2dbcConnectionInvalidator cleanupFailingInvalidator(AtomicInteger closeAttempts,
                                                                          AtomicInteger invalidations,
                                                                          Throwable closeFailure,
                                                                          Throwable invalidationFailure) {
        return R2dbcConnectionInvalidator.of(connection -> {
            closeAttempts.incrementAndGet();
            return rawError(closeFailure);
        }, connection -> {
            invalidations.incrementAndGet();
            return invalidationFailure == null ? Mono.empty() : rawError(invalidationFailure);
        });
    }

    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    private static <T> Publisher<T> rawError(Throwable error) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {

            private boolean terminated;

            @Override
            public void request(long ignored) {
                if (!terminated) {
                    terminated = true;
                    subscriber.onError(error);
                }
            }

            @Override
            public void cancel() {
                terminated = true;
            }
        });
    }

    private static SqlExecutionObserver cleanupObserver(List<ResourceCleanupObservation> observations) {
        return new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 聚焦批量取消后的资源清理事实。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                observations.add(observation);
            }
        };
    }

    private static R2dbcConnectionInvalidator closingInvalidator() {
        return R2dbcConnectionInvalidator.of(Connection::close, Connection::close);
    }

    private static void assertUnconfirmedBatchCleanup(List<ResourceCleanupObservation> observations) {
        assertEquals(1, observations.size());
        ResourceCleanupObservation observation = observations.getFirst();
        assertEquals(SqlExecutionOperation.CHUNKED_BATCH_WRITE, observation.operation());
        assertEquals(ResourceCleanupObservation.Phase.TRANSACTION_ROLLBACK, observation.phase());
        assertFalse(observation.outcomeConfirmed());
    }

    private static final class ControlledConnectionFactory implements ConnectionFactory {

        private final AtomicInteger rollbacks = new AtomicInteger();
        private final AtomicInteger closed = new AtomicInteger();
        private final AtomicInteger userExecutions = new AtomicInteger();
        private final AtomicInteger connections = new AtomicInteger();
        private final AtomicInteger activeConnections = new AtomicInteger();
        private final AtomicInteger confirmReceiptReads = new AtomicInteger();
        private final AtomicInteger reservationReplayReads = new AtomicInteger();
        private final AtomicInteger commitAttempts = new AtomicInteger();
        private final List<String> statementSql = new ArrayList<>();

        private boolean hangCommit;
        private boolean hangBegin;
        private boolean failRollback;
        private boolean failBegin;
        private boolean failUserWrite;
        private boolean hangUserWrite;
        private boolean hangFirstUserWrite;
        private boolean hangAfterFirstConnection;
        private boolean failClose;
        private Throwable beginError;
        private Throwable commitError;
        private Throwable rollbackError;
        private Throwable userWriteError;
        private String zeroRowsSql;
        private Long ownerId;
        private Duration receiptLookupDelay = Duration.ZERO;
        private boolean committedReceiptAfterCommitFailure;
        private String preexistingReceiptPayload;
        private boolean confirmationStartedAfterCleanup;
        private boolean duplicateReceiptReserve;
        private String receiptPayloadAfterReservationConflict;
        private boolean reservationReplayAvailable;
        private boolean reservationReplayStartedAfterCleanup;

        @Override
        public Publisher<? extends Connection> create() {
            if (hangAfterFirstConnection && connections.getAndIncrement() > 0) {
                return Mono.never();
            }
            return Mono.just(connection());
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return () -> "recording";
        }

        private Connection connection() {
            activeConnections.incrementAndGet();
            AtomicInteger closeState = new AtomicInteger();
            return proxy(Connection.class, (ignored, method, args) -> switch (method.getName()) {
                case "beginTransaction" -> beginError != null ? errorOnRequest(beginError)
                        : hangBegin ? Mono.never()
                        : failBegin
                        ? Mono.error(new IllegalStateException("transaction begin failed"))
                        : Mono.empty();
                case "commitTransaction" -> {
                    commitAttempts.incrementAndGet();
                    yield commitError != null ? errorOnRequest(commitError)
                            : hangCommit ? Mono.never() : Mono.empty();
                }
                case "rollbackTransaction" -> {
                    rollbacks.incrementAndGet();
                    yield rollbackError != null ? errorOnRequest(rollbackError)
                            : failRollback ? Mono.error(new IllegalStateException("rollback acknowledgement lost"))
                            : Mono.empty();
                }
                case "createStatement" -> {
                    statementSql.add((String) args[0]);
                    yield statement((String) args[0]);
                }
                case "close" -> {
                    closed.incrementAndGet();
                    if (!failClose && closeState.compareAndSet(0, 1)) {
                        activeConnections.decrementAndGet();
                    }
                    yield failClose ? Mono.error(new IllegalStateException("connection close failed")) : Mono.empty();
                }
                case "validate" -> Mono.just(true);
                case "isAutoCommit" -> false;
                default -> defaultValue(method);
            });
        }

        private Statement statement(String sql) {
            return proxy(Statement.class, new InvocationHandler() {

                private Statement self;

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    if (self == null) {
                        self = (Statement) proxy;
                    }
                    return switch (method.getName()) {
                        case "bind", "bindNull", "add", "fetchSize", "returnGeneratedValues" -> self;
                        case "execute" -> execute(sql);
                        default -> defaultValue(method);
                    };
                }
            });
        }

        private Publisher<? extends Result> execute(String sql) {
            boolean userWrite = sql.contains("Users");
            if (userWrite && userWriteError != null) {
                return errorOnRequest(userWriteError);
            }
            if (userWrite && failUserWrite) {
                return Flux.error(new IllegalStateException("user write failed"));
            }
            if (userWrite && hangUserWrite) {
                return Flux.never();
            }
            if (userWrite && hangFirstUserWrite && userExecutions.getAndIncrement() == 0) {
                return Flux.never();
            }
            if (sql.startsWith("select ") && ownerId != null) {
                return Flux.just(ownerResult(ownerId));
            }
            if (sql.startsWith("select row_count, affected_rows")
                    && reservationReplayAvailable) {
                reservationReplayReads.incrementAndGet();
                reservationReplayStartedAfterCleanup = activeConnections.get() == 1;
                return Flux.just(receiptResult(1L, 1L));
            }
            if (sql.startsWith("select row_count, affected_rows")
                    && committedReceiptAfterCommitFailure
                    && commitAttempts.get() > 0) {
                confirmReceiptReads.incrementAndGet();
                confirmationStartedAfterCleanup = activeConnections.get() == 1;
                return Flux.just(receiptResult(1L, 1L));
            }
            if (sql.startsWith("select payload_hash") && reservationReplayAvailable) {
                reservationReplayReads.incrementAndGet();
                reservationReplayStartedAfterCleanup = activeConnections.get() == 1;
                return Flux.just(receiptResult(preexistingReceiptPayload, 1L, 1L));
            }
            if (sql.startsWith("select payload_hash") && preexistingReceiptPayload != null) {
                return Flux.just(receiptResult(preexistingReceiptPayload, 1L, 1L));
            }
            if (sql.startsWith("insert into ") && sql.contains("status, created_at")
                    && duplicateReceiptReserve) {
                duplicateReceiptReserve = false;
                reservationReplayAvailable = true;
                preexistingReceiptPayload = receiptPayloadAfterReservationConflict;
                return errorOnRequest(new RdbException(
                        RdbErrorKind.DUPLICATE_KEY,
                        "database duplicate key conflict",
                        "23505",
                        null,
                        new IllegalStateException("duplicate receipt reservation")));
            }
            Flux<Result> execution = Flux.just(result(
                    sql.startsWith("select "), zeroRowsSql != null && zeroRowsSql.equals(sql) ? 0L : 1L));
            return sql.startsWith("select ") && !receiptLookupDelay.isZero()
                    ? execution.delaySubscription(receiptLookupDelay)
                    : execution;
        }

        private Result result(boolean emptyRows, long rowsUpdated) {
            return proxy(Result.class, (ignored, method, args) -> switch (method.getName()) {
                case "getRowsUpdated" -> Mono.just(rowsUpdated);
                case "map" -> emptyRows ? Flux.empty() : Flux.error(new AssertionError("unexpected row mapping"));
                default -> defaultValue(method);
            });
        }

        @SuppressWarnings("unchecked")
        private Result receiptResult(long rowCount, long affectedRows) {
            io.r2dbc.spi.RowMetadata metadata = proxy(
                    io.r2dbc.spi.RowMetadata.class,
                    (ignored, method, args) -> defaultValue(method));
            io.r2dbc.spi.Row row = proxy(
                    io.r2dbc.spi.Row.class,
                    (ignored, method, args) -> {
                        if (!method.getName().equals("get") || !(args[0] instanceof Integer index)) {
                            return defaultValue(method);
                        }
                        return index == 0 ? rowCount : affectedRows;
                    });
            return proxy(Result.class, (ignored, method, args) -> {
                if (!method.getName().equals("map")) {
                    return defaultValue(method);
                }
                java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object> mapper =
                        (java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object>) args[0];
                return Flux.just(mapper.apply(row, metadata));
            });
        }

        @SuppressWarnings("unchecked")
        private Result receiptResult(String payloadHash, long rowCount, long affectedRows) {
            io.r2dbc.spi.RowMetadata metadata = proxy(
                    io.r2dbc.spi.RowMetadata.class,
                    (ignored, method, args) -> defaultValue(method));
            io.r2dbc.spi.Row row = proxy(
                    io.r2dbc.spi.Row.class,
                    (ignored, method, args) -> {
                        if (!method.getName().equals("get") || !(args[0] instanceof Integer index)) {
                            return defaultValue(method);
                        }
                        return switch (index) {
                            case 0 -> payloadHash;
                            case 1 -> rowCount;
                            default -> affectedRows;
                        };
                    });
            return proxy(Result.class, (ignored, method, args) -> {
                if (!method.getName().equals("map")) {
                    return defaultValue(method);
                }
                java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object> mapper =
                        (java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object>) args[0];
                return Flux.just(mapper.apply(row, metadata));
            });
        }

        @SuppressWarnings("unchecked")
        private Result ownerResult(long id) {
            io.r2dbc.spi.ColumnMetadata column = proxy(
                    io.r2dbc.spi.ColumnMetadata.class,
                    (ignored, method, args) -> method.getName().equals("getName")
                            ? "id" : defaultValue(method));
            io.r2dbc.spi.RowMetadata metadata = proxy(
                    io.r2dbc.spi.RowMetadata.class,
                    (ignored, method, args) -> method.getName().equals("getColumnMetadatas")
                            ? List.of(column) : defaultValue(method));
            io.r2dbc.spi.Row row = proxy(
                    io.r2dbc.spi.Row.class,
                    (ignored, method, args) -> method.getName().equals("get") ? id : defaultValue(method));
            return proxy(Result.class, (ignored, method, args) -> {
                if (!method.getName().equals("map")) {
                    return defaultValue(method);
                }
                java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object> mapper =
                        (java.util.function.BiFunction<io.r2dbc.spi.Row, io.r2dbc.spi.RowMetadata, Object>) args[0];
                return Flux.just(mapper.apply(row, metadata));
            });
        }

        private static <T> Publisher<T> errorOnRequest(Throwable error) {
            return rawError(error);
        }
    }

    /** 严格按 request(n) 供给元素的测试输入，用来验证分片算子的真实拉取量。 */
    private static final class DemandTrackingRows implements Publisher<Object[]> {

        private final List<Object[]> rows;

        private final AtomicLong requested;

        private final AtomicInteger cancellations;

        private DemandTrackingRows(List<Object[]> rows,
                                    AtomicLong requested,
                                    AtomicInteger cancellations) {
            this.rows = rows;
            this.requested = requested;
            this.cancellations = cancellations;
        }

        @Override
        public void subscribe(Subscriber<? super Object[]> subscriber) {
            subscriber.onSubscribe(new Subscription() {

                private long outstanding;

                private int index;

                private boolean emitting;

                private boolean cancelled;

                @Override
                public synchronized void request(long count) {
                    requested.addAndGet(count);
                    outstanding = count >= Long.MAX_VALUE - outstanding ? Long.MAX_VALUE : outstanding + count;
                    if (emitting || cancelled) {
                        return;
                    }
                    emitting = true;
                    while (outstanding > 0 && index < rows.size() && !cancelled) {
                        outstanding--;
                        subscriber.onNext(rows.get(index++));
                    }
                    if (index == rows.size() && !cancelled) {
                        subscriber.onComplete();
                    }
                    emitting = false;
                }

                @Override
                public synchronized void cancel() {
                    cancelled = true;
                    cancellations.incrementAndGet();
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }

    private static Object defaultValue(Method method) {
        Class<?> returnType = method.getReturnType();
        if (Publisher.class.isAssignableFrom(returnType)) {
            return Mono.empty();
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        return null;
    }
}
