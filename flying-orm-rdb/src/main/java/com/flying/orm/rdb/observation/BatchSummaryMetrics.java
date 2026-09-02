package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.util.Objects;

/**
 * 流式批量观测的具名汇总值，避免多个同类型计数在调用链中依赖位置传递。
 */
record BatchSummaryMetrics(BatchWriteResult.Status status,
                           long inputCount,
                           long affectedRows,
                           long conflictCount,
                           long chunkCount,
                           long successfulChunkCount,
                           long failedChunkCount,
                           BatchChunkResult.Failure firstFailure,
                           BatchChunkResult.RecoveryToken firstRecoveryToken) {

    BatchSummaryMetrics {
        status = Objects.requireNonNull(status, "batch summary status must not be null");
        requireNonNegative("batch summary input count", inputCount);
        requireNonNegative("batch summary affected rows", affectedRows);
        requireNonNegative("batch conflict count", conflictCount);
        requireNonNegative("batch summary chunk count", chunkCount);
        requireNonNegative("batch summary successful chunk count", successfulChunkCount);
        requireNonNegative("batch summary failed chunk count", failedChunkCount);
        if (successfulChunkCount > chunkCount || failedChunkCount != chunkCount - successfulChunkCount) {
            throw new IllegalArgumentException(
                    "successful and failed chunks must exactly partition total chunks");
        }
    }

    static BatchSummaryMetrics from(BatchWriteResult result) {
        BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
        long chunkCount = safeResult.chunks().size();
        long successfulChunkCount = BatchObservationClassification.successfulChunkCount(safeResult);
        return new BatchSummaryMetrics(
                safeResult.status(),
                safeResult.inputCount(),
                safeResult.affectedRows(),
                safeResult.conflictCount(),
                chunkCount,
                successfulChunkCount,
                chunkCount - successfulChunkCount,
                BatchObservationClassification.firstFailure(safeResult),
                BatchObservationClassification.firstRecoveryToken(safeResult));
    }

    SqlFailureCategory failureCategory() {
        if (conflictCount > 0) {
            return SqlFailureCategory.OPTIMISTIC_LOCK;
        }
        return firstFailure == null
                ? BatchObservationClassification.category(status)
                : BatchObservationClassification.category(firstFailure);
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
