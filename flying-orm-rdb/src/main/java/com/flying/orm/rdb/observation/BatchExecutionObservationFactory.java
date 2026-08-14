package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.util.Objects;

/**
 * 集中组装批量观测事件，公共 record 只保留清晰的入口和字段校验。
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
        boolean successful = BatchObservationClassification.isSuccessful(safeChunk.status());
        return new BatchExecutionObservation(BatchExecutionEventType.CHUNK,
                                             safeRequest.backend(),
                                             SqlStatementType.fromSql(safeRequest.sql()),
                                             safeRequest.sql(),
                                             safeRequest.mode(),
                                             null,
                                             safeChunk.status(),
                                             null,
                                             safeChunk.chunkIndex(),
                                             safeChunk.startOffset(),
                                             safeChunk.inputCount(),
                                             safeChunk.affectedRows(),
                                             1,
                                             successful ? 1 : 0,
                                             successful ? 0 : 1,
                                             safeRequest.parameterCount(),
                                             durationNanos,
                                             BatchObservationClassification.category(safeChunk),
                                             safeChunk.failure(),
                                             safeChunk.recoveryToken());
    }

    static BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request,
                                             BatchWriteResult result,
                                             long durationNanos) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        BatchWriteResult safeResult = Objects.requireNonNull(result, "batch write result must not be null");
        long chunkCount = safeResult.chunks().size();
        long successfulChunkCount = BatchObservationClassification.successfulChunkCount(safeResult);
        return new BatchExecutionObservation(BatchExecutionEventType.SUMMARY,
                                             safeRequest.backend(),
                                             SqlStatementType.fromSql(safeRequest.sql()),
                                             safeRequest.sql(),
                                             safeResult.mode(),
                                             safeResult.status(),
                                             null,
                                             null,
                                             BatchExecutionObservation.NO_CHUNK,
                                             0,
                                             safeResult.inputCount(),
                                             safeResult.affectedRows(),
                                             chunkCount,
                                             successfulChunkCount,
                                             chunkCount - successfulChunkCount,
                                             safeRequest.parameterCount(),
                                             durationNanos,
                                             BatchObservationClassification.summaryCategory(safeResult),
                                             BatchObservationClassification.firstFailure(safeResult),
                                             BatchObservationClassification.firstRecoveryToken(safeResult));
    }

    static BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request,
                                             BatchWriteResult.Status status,
                                             long inputCount,
                                             long affectedRows,
                                             long conflictCount,
                                             long chunkCount,
                                             long successfulChunkCount,
                                             long failedChunkCount,
                                             BatchChunkResult.Failure firstFailure,
                                             BatchChunkResult.RecoveryToken firstRecoveryToken,
                                             long durationNanos) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        BatchWriteResult.Status safeStatus = Objects.requireNonNull(status,
                                                                    "batch summary status must not be null");
        if (conflictCount < 0) {
            throw new IllegalArgumentException("batch conflict count must not be negative");
        }
        SqlFailureCategory category = conflictCount > 0
                ? SqlFailureCategory.OPTIMISTIC_LOCK
                : firstFailure == null
                        ? BatchObservationClassification.category(safeStatus)
                        : BatchObservationClassification.category(firstFailure);
        return new BatchExecutionObservation(BatchExecutionEventType.SUMMARY,
                                             safeRequest.backend(),
                                             SqlStatementType.fromSql(safeRequest.sql()),
                                             safeRequest.sql(),
                                             safeRequest.mode(),
                                             safeStatus,
                                             null,
                                             null,
                                             BatchExecutionObservation.NO_CHUNK,
                                             0,
                                             inputCount,
                                             affectedRows,
                                             chunkCount,
                                             successfulChunkCount,
                                             failedChunkCount,
                                             safeRequest.parameterCount(),
                                             durationNanos,
                                             category,
                                             firstFailure,
                                             firstRecoveryToken);
    }

    static BatchExecutionObservation failedSummary(BatchExecutionObservation.BatchWriteRequestView request,
                                                   long durationNanos,
                                                   Throwable error) {
        BatchExecutionObservation.BatchWriteRequestView safeRequest = Objects.requireNonNull(
                request, "batch request view must not be null");
        Throwable safeError = Objects.requireNonNull(error, "batch write error must not be null");
        return new BatchExecutionObservation(BatchExecutionEventType.SUMMARY,
                                             safeRequest.backend(),
                                             SqlStatementType.fromSql(safeRequest.sql()),
                                             safeRequest.sql(),
                                             safeRequest.mode(),
                                             BatchWriteResult.Status.UNKNOWN,
                                             null,
                                             null,
                                             BatchExecutionObservation.NO_CHUNK,
                                             0,
                                             0,
                                             0,
                                             0,
                                             0,
                                             0,
                                             safeRequest.parameterCount(),
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
        return new BatchExecutionObservation(BatchExecutionEventType.RECOVERY,
                                             SqlExecutionBackend.R2DBC,
                                             SqlStatementType.UNKNOWN,
                                             "",
                                             null,
                                             null,
                                             null,
                                             safeResolution.status(),
                                             safeResolution.token().chunkIndex(),
                                             0,
                                             0,
                                             0,
                                             0,
                                             0,
                                             0,
                                             0,
                                             durationNanos,
                                             BatchObservationClassification.recoveryCategory(safeResolution, error),
                                             error == null ? null : BatchChunkResult.Failure.from(error),
                                             safeResolution.token());
    }
}
