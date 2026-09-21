package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;

/** 可选批量观测只持有累计标量事实，不保留参数及历史缓冲。 */
final class JdbcBatchExecutionObservationSupport {
    private static final BatchContext DISABLED = new BatchContext(null, null);
    private final BatchExecutionObserver observer;

    private JdbcBatchExecutionObservationSupport(BatchExecutionObserver observer) {
        BatchExecutionObserver safe = observer == null ? null
                : BatchExecutionObserver.composite(BatchExecutionObserver.noop(), observer);
        this.observer = safe == null || !safe.enabled() ? null : safe;
    }

    static JdbcBatchExecutionObservationSupport create(BatchExecutionObserver observer) {
        return new JdbcBatchExecutionObservationSupport(observer);
    }

    BatchContext begin(BatchWriteRequest request) {
        return observer == null ? DISABLED : new BatchContext(observer,
                new BatchExecutionObservation.BatchWriteRequestView(
                        request.sql(), request.parameterCount(), SqlExecutionBackend.JDBC));
    }

    static final class BatchContext {
        private final BatchExecutionObserver observer;
        private final BatchExecutionObservation.BatchWriteRequestView request;
        private final long startedAt;

        private BatchContext(BatchExecutionObserver observer,
                             BatchExecutionObservation.BatchWriteRequestView request) {
            this.observer = observer;
            this.request = request;
            this.startedAt = observer == null ? 0 : System.nanoTime();
        }

        boolean enabled() { return observer != null; }

        void progress(BatchExecutionEvidence evidence) {
            if (observer != null) observer.onExecution(
                    BatchExecutionObservation.progress(request, evidence, elapsedNanos()));
        }

        void completed(BatchExecutionEvidence evidence) {
            if (observer == null) return;
            observer.onExecution(BatchExecutionObservation.summary(request, evidence, elapsedNanos()));
            observer.onExecutionEvidence(evidence);
        }

        private long elapsedNanos() { return Math.max(0, System.nanoTime() - startedAt); }
    }
}
