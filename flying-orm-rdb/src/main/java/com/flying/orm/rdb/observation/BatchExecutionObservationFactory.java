package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.util.Objects;

/**
 * 集中组装批量观测事件，公共事件模型只保留清晰的入口和字段校验。
 *
 * <p>这个类没有可变状态。完整结果会扫描一次分片列表计算成功数量；流式结果直接使用订阅内累加值，
 * 两条路径都不会为了写日志再复制分片或业务参数。</p>
 */
final class BatchExecutionObservationFactory {

    private BatchExecutionObservationFactory() {
    }

    static BatchExecutionObservation chunk(BatchExecutionObservation.BatchWriteRequestView request,
                                           BatchChunkResult chunk,
                                           long durationNanos) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        BatchChunkResult safeChunk = Objects.requireNonNull(chunk, "batch chunk result must not be null");
        return BatchExecutionObservation.Chunk.fromValidated(safeRequest, safeChunk, durationNanos);
    }

    static BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request,
                                             BatchWriteResult result,
                                             long durationNanos) {
        return summary(request, BatchSummaryMetrics.from(result), durationNanos);
    }

    static BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request,
                                             BatchSummaryMetrics metrics,
                                             long durationNanos) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        BatchSummaryMetrics safeMetrics = Objects.requireNonNull(metrics, "batch summary metrics must not be null");
        return new BatchExecutionObservation.Summary(safeRequest,
                                                     safeMetrics.status(),
                                                     safeMetrics.inputCount(),
                                                     safeMetrics.affectedRows(),
                                                     safeMetrics.chunkCount(),
                                                     safeMetrics.successfulChunkCount(),
                                                     safeMetrics.failedChunkCount(),
                                                     durationNanos,
                                                     safeMetrics.failureCategory(),
                                                     safeMetrics.firstFailure(),
                                                     safeMetrics.firstRecoveryToken());
    }

    static BatchExecutionObservation failedSummary(BatchExecutionObservation.BatchWriteRequestView request,
                                                   long durationNanos,
                                                   Throwable error) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        Throwable safeError = Objects.requireNonNull(error, "batch write error must not be null");
        return new BatchExecutionObservation.Summary(safeRequest,
                                                     BatchWriteResult.Status.UNKNOWN,
                                                     0,
                                                     0,
                                                     0,
                                                     0,
                                                     0,
                                                     durationNanos,
                                                     SqlFailureCategory.classify(safeError),
                                                     BatchChunkResult.Failure.from(safeError),
                                                     null);
    }

    static BatchExecutionObservation recovery(BatchResolution resolution,
                                              long durationNanos,
                                              Throwable error) {
        BatchResolution safeResolution = Objects.requireNonNull(resolution,
                                                                "batch resolution must not be null");
        return new BatchExecutionObservation.Recovery(
                safeResolution.status(),
                durationNanos,
                BatchObservationClassification.recoveryCategory(safeResolution, error),
                error == null ? null : BatchChunkResult.Failure.from(error),
                safeResolution.token());
    }
}
