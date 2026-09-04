package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;

import java.util.Objects;

/**
 * JDBC 批量写入的观测边界。
 *
 * <p>批量执行过程中不保存 rows，也不把业务参数放进观测对象。只有整批有了明确结果，
 * 这里才按结果里的分片顺序发 CHUNK，再发一次 SUMMARY。普通批量入口和分片入口
 * 共享同一条执行路径，因此不会重复执行或重复发事件。</p>
 */
final class JdbcBatchExecutionObservationSupport {

    private static final BatchContext DISABLED = new BatchContext(null, null, null);

    private final BatchExecutionObserver observer;
    private final SqlExecutionObserver cleanupObserver;

    private JdbcBatchExecutionObservationSupport(BatchExecutionObserver observer,
                                                 SqlExecutionObserver cleanupObserver) {
        BatchExecutionObserver safeObserver = observer == null ? null : BatchExecutionObserver.composite(
                BatchExecutionObserver.noop(), observer);
        this.observer = safeObserver == null || !safeObserver.enabled() ? null : safeObserver;
        this.cleanupObserver = cleanupObserver == null || !cleanupObserver.enabled() ? null : cleanupObserver;
    }

    static JdbcBatchExecutionObservationSupport create(BatchExecutionObserver observer,
                                                       SqlExecutionObserver cleanupObserver) {
        return new JdbcBatchExecutionObservationSupport(observer, cleanupObserver);
    }

    BatchContext begin(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        if (observer == null && cleanupObserver == null) {
            return DISABLED;
        }
        BatchExecutionObservation.BatchWriteRequestView view = observer == null
                ? null
                : new BatchExecutionObservation.BatchWriteRequestView(
                        request.sql(), request.options().mode(), request.parameterCount(), SqlExecutionBackend.JDBC);
        return new BatchContext(observer, cleanupObserver, view);
    }

    static final class BatchContext {

        private final BatchExecutionObserver observer;
        private final SqlExecutionObserver cleanupObserver;
        private final BatchExecutionObservation.BatchWriteRequestView request;
        private final long startedAt;
        private SqlTransactionSource transactionSource = SqlTransactionSource.INTERNAL;

        private BatchContext(BatchExecutionObserver observer,
                             SqlExecutionObserver cleanupObserver,
                             BatchExecutionObservation.BatchWriteRequestView request) {
            this.observer = observer;
            this.cleanupObserver = cleanupObserver;
            this.request = request;
            this.startedAt = observer == null ? 0L : System.nanoTime();
        }

        void transactionSource(SqlTransactionSource source) {
            if (observer == null) {
                return;
            }
            transactionSource = Objects.requireNonNull(source, "batch transaction source must not be null");
        }

        void completed(BatchWriteResult result) {
            if (observer == null) {
                return;
            }
            BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
            long duration = elapsedNanos();
            for (BatchChunkResult chunk : safeResult.chunks()) {
                notifyObserver(BatchExecutionObservation.chunk(request, chunk, duration));
            }
            notifyObserver(BatchExecutionObservation.summary(request, safeResult, duration));
        }

        /** 外部事务结束后只补最终汇总，已经发过的 ENLISTED 分片不重复发送。 */
        void finalized(BatchWriteResult result) {
            if (observer == null) {
                return;
            }
            notifyObserver(BatchExecutionObservation.summary(
                    request, Objects.requireNonNull(result, "final batch result must not be null"), elapsedNanos()));
        }

        void failed(Throwable error) {
            if (observer == null) {
                return;
            }
            Throwable safeError = Objects.requireNonNull(error, "batch write error must not be null");
            long duration = elapsedNanos();
            if (safeError instanceof BatchWriteException batchError) {
                BatchWriteResult result = batchError.result();
                for (BatchChunkResult chunk : result.chunks()) {
                    notifyObserver(BatchExecutionObservation.chunk(request, chunk, duration));
                }
                notifyObserver(BatchExecutionObservation.summary(request, result, duration));
                return;
            }
            notifyObserver(BatchExecutionObservation.failedSummary(request, duration, safeError));
        }

        void evidence(BatchExecutionEvidence evidence) {
            if (observer != null) {
                observer.onExecutionEvidence(Objects.requireNonNull(
                        evidence, "batch execution evidence must not be null"));
            }
        }

        void cleanupFailure(Throwable error) {
            if (cleanupObserver == null) {
                return;
            }
            cleanupObserver.onResourceCleanup(new ResourceCleanupObservation(
                    SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                    ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                    true,
                    Objects.requireNonNull(error, "batch cleanup failure must not be null")));
        }

        private void notifyObserver(BatchExecutionObservation observation) {
            observer.onExecution(observation, transactionSource);
        }

        private long elapsedNanos() {
            return Math.max(0L, System.nanoTime() - startedAt);
        }
    }
}
