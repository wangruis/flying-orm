package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 JDBC 批量回滚异常聚合保留已有因果方向且不产生 Throwable 环。 */
class JdbcBatchSupportTest {

    /** 批量参数超过安全深度时，即使配置最大 long 预算也必须在 onNext 所有权边界拒绝。 */
    @Test
    void maximumBufferedLimitRejectsRowsWhoseMemoryEstimateIsUnknown() {
        Object value = "leaf";
        for (int depth = 0; depth < 66; depth++) {
            value = List.of(value);
        }
        BatchWriteOptions options = BatchWriteOptions.atomic(1)
                .withMemoryLimits(1, Long.MAX_VALUE, 1);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into events(payload) values (?)",
                1,
                List.of(Object.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{value}),
                options);

        try (JdbcBatchRows rows = new JdbcBatchRows(
                request.rows(), request.parameterCount(), options.maxBufferedBytes())) {
            assertThrows(IllegalArgumentException.class,
                         () -> JdbcBatchSupport.readChunk(
                                 rows,
                                 request,
                                 0L,
                                 0,
                                 JdbcBatchSupport.BatchDeadline.start(options.timeout())));
        }
    }

    /** JDBC 所有权边界压紧 ByteBuffer 后，tiny slice 应按实际可读载荷合法进入分片。 */
    @Test
    void acceptsCompactedByteBufferWithinChunkBudget() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(1_024);
        buffer.position(buffer.capacity() - 1);
        BatchWriteOptions options = BatchWriteOptions.atomic(1).withMemoryLimits(1, 128L, 1);
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into events(payload) values (?)",
                1,
                List.of(ByteBuffer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{buffer}),
                options);

        try (JdbcBatchRows rows = new JdbcBatchRows(
                request.rows(), request.parameterCount(), options.maxBufferedBytes())) {
            List<Object[]> chunk = JdbcBatchSupport.readChunk(
                    rows, request, 0L, 0, JdbcBatchSupport.BatchDeadline.start(options.timeout()));
            ByteBuffer owned = assertInstanceOf(ByteBuffer.class, chunk.getFirst()[0]);
            assertEquals(1, owned.capacity());
            assertEquals(1, chunk.size());
        }
    }

    /** onNext 快照失败必须记录终态并取消上游，即使批量未配置超时也不能永久等待。 */
    @Test
    void snapshotFailureCancelsPublisherAndWakesUnlimitedWait() throws Exception {
        AtomicInteger cancellations = new AtomicInteger();
        List<Object> cycle = new java.util.ArrayList<>();
        cycle.add(cycle);
        Publisher<Object[]> publisher = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long ignored) { subscriber.onNext(new Object[]{cycle}); }
            @Override public void cancel() { cancellations.incrementAndGet(); }
        });

        try (JdbcBatchRows rows = new JdbcBatchRows(publisher, 1, 1_024L)) {
            assertThrows(IllegalArgumentException.class, () -> rows.next(Duration.ZERO));
        }
        assertEquals(1, cancellations.get());
    }

    /** rollback 失败已以 cause 引用主失败时，不能反向把 rollback error suppress 到主失败上。 */
    @Test
    void doesNotCreateCycleWhenRollbackFailureAlreadyCausesNormalPrimary() {
        IllegalStateException primary = new IllegalStateException("operation failed");
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failed", primary);

        JdbcBatchSupport.RollbackOutcome outcome = JdbcBatchSupport.RollbackOutcome.failed(primary, rollbackFailure);

        assertFalse(outcome.confirmed());
        assertSame(null, outcome.cleanupFatal());
        assertSame(primary, rollbackFailure.getCause());
        assertFalse(reaches(primary, rollbackFailure));
    }

    /** primary VME 优先传播时也不能把已反向引用它的 cleanup error 再挂回 VME。 */
    @Test
    void doesNotCreateCycleWhenRollbackFailureAlreadyCausesPrimaryVirtualMachineError() {
        OutOfMemoryError primary = new OutOfMemoryError("operation fatal");
        IllegalStateException rollbackFailure = new IllegalStateException("rollback failed", primary);

        JdbcBatchSupport.RollbackOutcome outcome = JdbcBatchSupport.RollbackOutcome.failed(primary, rollbackFailure);

        assertFalse(outcome.confirmed());
        assertSame(primary, outcome.cleanupFatal());
        assertSame(primary, rollbackFailure.getCause());
        assertFalse(reaches(primary, rollbackFailure));
    }

    /** reset 的 VME 已以 cause 指向操作 VME 时，恢复阶段不能反向补成异常环。 */
    @Test
    void doesNotCreateCycleWhenRestoreFatalAlreadyCausesOperationFatal() {
        OutOfMemoryError operationFatal = new OutOfMemoryError("operation fatal");
        OutOfMemoryError restoreFatal = new OutOfMemoryError("restore fatal");
        restoreFatal.initCause(operationFatal);

        OutOfMemoryError thrown = org.junit.jupiter.api.Assertions.assertThrows(
                OutOfMemoryError.class,
                () -> JdbcBatchSupport.rethrowRestoreVirtualMachineError(restoreFatal, operationFatal));

        assertSame(operationFatal, thrown);
        assertSame(operationFatal, restoreFatal.getCause());
        assertFalse(reaches(operationFatal, restoreFatal));
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
}
