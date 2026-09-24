package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;

import java.time.Duration;
import java.util.Objects;

/**
 * 批量调用的累计 SQL 执行事实；不暴露内部缓冲边界，不保存输入或实体。
 *
 * @author wangr
 * @version v4.1.0
 */
public record BatchExecutionObservation(BatchExecutionEventType eventType,
                                        BatchWriteRequestView request,
                                        BatchExecutionState state,
                                        long inputCount,
                                        BatchAffectedRows affectedRows,
                                        long successfulCount,
                                        long failedCount,
                                        long conflictCount,
                                        long durationNanos,
                                        SqlFailureCategory failureCategory,
                                        BatchExecutionEvidence.Failure failure) {

    public BatchExecutionObservation {
        eventType = Objects.requireNonNull(eventType, "batch event type must not be null");
        request = Objects.requireNonNull(request, "batch request view must not be null");
        state = Objects.requireNonNull(state, "batch execution state must not be null");
        affectedRows = Objects.requireNonNull(affectedRows, "batch affected rows must not be null");
        failureCategory = Objects.requireNonNull(failureCategory, "batch failure category must not be null");
        if (inputCount < 0 || successfulCount < 0 || failedCount < 0 || conflictCount < 0 || durationNanos < 0) {
            throw new IllegalArgumentException("batch observation counts and duration must not be negative");
        }
        if (successfulCount > inputCount || failedCount > inputCount - successfulCount
                || conflictCount > failedCount) {
            throw new IllegalArgumentException("batch observation outcomes must fit accepted input");
        }
    }

    public static BatchExecutionObservation progress(BatchWriteRequestView request,
                                                      BatchExecutionEvidence evidence,
                                                      long durationNanos) {
        return from(BatchExecutionEventType.PROGRESS, request, evidence, durationNanos);
    }

    public static BatchExecutionObservation summary(BatchWriteRequestView request,
                                                     BatchExecutionEvidence evidence,
                                                     long durationNanos) {
        return from(BatchExecutionEventType.SUMMARY, request, evidence, durationNanos);
    }

    private static BatchExecutionObservation from(BatchExecutionEventType eventType,
                                                  BatchWriteRequestView request,
                                                  BatchExecutionEvidence evidence,
                                                  long durationNanos) {
        BatchExecutionEvidence facts = Objects.requireNonNull(evidence, "batch execution evidence must not be null");
        SqlFailureCategory category = !facts.conflicts().isEmpty() ? SqlFailureCategory.OPTIMISTIC_LOCK
                : facts.failure() == null ? SqlFailureCategory.NONE
                : SqlFailureCategory.fromKind(facts.failure().kind());
        return new BatchExecutionObservation(eventType, request, facts.state(), facts.inputCount(),
                facts.affectedRows(), facts.successfulCount(), facts.failedCount(), facts.conflicts().size(),
                durationNanos, category, facts.failure());
    }

    /** 仅用于还未形成任何 SQL 事实的入口失败；已有部分结果时使用 summary。 */
    public static BatchExecutionObservation failedSummary(BatchWriteRequestView request,
                                                           long durationNanos,
                                                           Throwable error) {
        Throwable failure = Objects.requireNonNull(error, "batch error must not be null");
        SqlFailureCategory category = SqlFailureCategory.classify(failure);
        BatchExecutionState state = switch (category) {
            case CANCELLED -> BatchExecutionState.CANCELLED;
            case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
            default -> BatchExecutionState.FAILED;
        };
        return new BatchExecutionObservation(BatchExecutionEventType.SUMMARY, request, state, 0,
                BatchAffectedRows.unknown(), 0, 0, 0, durationNanos, category,
                BatchExecutionEvidence.Failure.from(failure));
    }

    public SqlExecutionBackend backend() { return request.backend(); }
    public SqlStatementType statementType() { return SqlStatementType.fromSql(request.sql()); }
    public String sql() { return request.sql(); }
    public int parameterCount() { return request.parameterCount(); }
    public Duration duration() { return Duration.ofNanos(durationNanos); }
    public SqlExecutionResultKind resultKind() { return SqlExecutionResultKind.fromBatch(state, failureCategory); }

    /** 只保存无参数值的请求描述。 */
    public record BatchWriteRequestView(String sql, int parameterCount, SqlExecutionBackend backend) {
        public BatchWriteRequestView(String sql, int parameterCount) {
            this(sql, parameterCount, SqlExecutionBackend.UNKNOWN);
        }

        public BatchWriteRequestView {
            sql = Objects.requireNonNull(sql, "batch sql must not be null");
            backend = Objects.requireNonNull(backend, "batch execution backend must not be null");
            if (parameterCount < 0) throw new IllegalArgumentException("batch parameter count must not be negative");
        }
    }
}
