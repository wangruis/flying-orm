package com.flying.orm.testkit.fault;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证故障按调用序号稳定触发，测试不需要真的断数据库连接。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ReactiveSqlFaultInjectorTest {

    @Test
    void failsOnlyTheConfiguredInvocation() {
        RecordingExecutor delegate = new RecordingExecutor();
        ReactiveSqlFaultInjector injector = ReactiveSqlFaultInjector.builder(delegate)
                                                                     .fail(ReactiveSqlFaultInjector.Operation.UPDATE,
                                                                           2,
                                                                           RdbFaults.deadlock())
                                                                     .build();
        SqlRequest request = new SqlRequest("update device set name = ?", List.of("sensor"));

        assertEquals(1L, injector.rowsUpdated(request).block());
        RdbException error = assertThrows(RdbException.class, () -> injector.rowsUpdated(request).block());
        assertEquals(RdbErrorKind.DEADLOCK, error.kind());
        assertEquals(1L, injector.rowsUpdated(request).block());

        assertEquals(3, injector.invocations(ReactiveSqlFaultInjector.Operation.UPDATE));
        assertEquals(2, delegate.updates.get());
    }

    @Test
    void hangingQueryIsCancelledByExecutionTimeout() {
        ReactiveSqlFaultInjector injector = ReactiveSqlFaultInjector.builder(new RecordingExecutor())
                                                                     .hang(ReactiveSqlFaultInjector.Operation.QUERY, 1)
                                                                     .build();

        assertThrows(SqlExecutionTimeoutException.class,
                     () -> injector.query(new SqlRequest("select id from device", List.of()),
                                          SqlExecutionOptions.timeout(Duration.ofMillis(10)))
                                   .blockLast(Duration.ofSeconds(1)));

        assertEquals(1, injector.cancellations(ReactiveSqlFaultInjector.Operation.QUERY));
    }

    @Test
    void returnsUnknownBatchAndScriptedRecoveryWithoutCallingDelegate() {
        RecordingExecutor delegate = new RecordingExecutor();
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken(
                "operation-1", 0, "flying_batch_receipt", "plan", "payload", 2L, null);
        BatchWriteResult unknown = BatchWriteResult.from(
                BatchWriteOptions.Mode.INDEPENDENT,
                List.of(BatchChunkResult.unknown(0, 0, 2, RdbFaults.connectionInterrupted(), token)));
        ReactiveSqlFaultInjector injector = ReactiveSqlFaultInjector.builder(delegate)
                                                                     .returnBatch(1, unknown)
                                                                     .returnRecovery(1,
                                                                                     BatchResolution.committed(token))
                                                                     .build();
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into device(id) values(?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(new Object[]{"1"}, new Object[]{"2"}),
                BatchWriteOptions.independent(2, 1));

        assertEquals(BatchWriteResult.Status.UNKNOWN, injector.writeBatch(request).block().status());
        assertEquals(BatchResolution.committed(token), injector.resolveUnknown(token).block());

        assertEquals(0, delegate.batchWrites.get());
        assertEquals(0, delegate.recoveries.get());
    }

    @Test
    void exposesStableKindsForCommonInjectedDatabaseFaults() {
        assertEquals(RdbErrorKind.CONNECTION, RdbFaults.connectionInterrupted().kind());
        assertEquals(RdbErrorKind.TIMEOUT, RdbFaults.timeout().kind());
        assertEquals(RdbErrorKind.DEADLOCK, RdbFaults.deadlock().kind());
        assertEquals(RdbErrorKind.LOCK_TIMEOUT, RdbFaults.lockTimeout().kind());
        assertEquals(RdbErrorKind.CANCELLED, RdbFaults.cancelled().kind());
    }

    private static final class RecordingExecutor implements ReactiveSqlExecutor {

        private final AtomicInteger updates = new AtomicInteger();
        private final AtomicInteger batchWrites = new AtomicInteger();
        private final AtomicInteger recoveries = new AtomicInteger();

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            return Flux.just(DynamicRow.copyOf(Map.of("id", "1")));
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            updates.incrementAndGet();
            return Mono.just(1L);
        }

        @Override
        public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
            batchWrites.incrementAndGet();
            return Mono.just(BatchWriteResult.empty(request.options().mode()));
        }

        @Override
        public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
            recoveries.incrementAndGet();
            return Mono.just(BatchResolution.unknown(token));
        }
    }
}
