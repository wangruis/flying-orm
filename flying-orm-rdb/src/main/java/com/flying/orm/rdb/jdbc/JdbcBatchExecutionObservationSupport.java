package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
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

    private final BatchExecutionObserver observer;

    private JdbcBatchExecutionObservationSupport(BatchExecutionObserver observer) {
        this.observer = observer == null
                ? null
                : BatchExecutionObserver.composite(BatchExecutionObserver.noop(), observer);
    }

    static JdbcBatchExecutionObservationSupport create(BatchExecutionObserver observer) {
        return new JdbcBatchExecutionObservationSupport(observer);
    }

    BatchContext begin(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return new BatchContext(new BatchExecutionObservation.BatchWriteRequestView(
                request.sql(), request.options().mode(), request.parameterCount(), SqlExecutionBackend.JDBC));
    }

    final class BatchContext {

        private final BatchExecutionObservation.BatchWriteRequestView request;
        private final long startedAt = System.nanoTime();
        private SqlTransactionSource transactionSource = SqlTransactionSource.INTERNAL;

        private BatchContext(BatchExecutionObservation.BatchWriteRequestView request) {
            this.request = request;
        }

        void transactionSource(SqlTransactionSource source) {
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

        private void notifyObserver(BatchExecutionObservation observation) {
            try {
                observer.onExecution(observation, transactionSource);
            } catch (RuntimeException ignored) {
                // 观测是旁路能力，日志或指标代码出错不能改变已经确定的批量结果。
            }
        }

        private long elapsedNanos() {
            return Math.max(0L, System.nanoTime() - startedAt);
        }
    }
}
