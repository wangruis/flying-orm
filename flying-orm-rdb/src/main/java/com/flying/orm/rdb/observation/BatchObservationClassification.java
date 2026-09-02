package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.util.Objects;

/**
 * 批量观测模型的状态归类和统计工具。
 *
 * <p>这些规则只服务于同包内的事件工厂，不作为公共扩展点。把它们从公共事件模型中拿出来，
 * 是为了让 {@link BatchExecutionObservation} 只负责表达和校验事件，不继续混入统计遍历职责。</p>
 */
final class BatchObservationClassification {

    private BatchObservationClassification() {
    }

    static SqlFailureCategory category(BatchChunkResult chunk) {
        return switch (chunk.status()) {
            case COMMITTED, ENLISTED, ROLLED_BACK -> SqlFailureCategory.NONE;
            case CONFLICTED -> SqlFailureCategory.OPTIMISTIC_LOCK;
            case FAILED, UNKNOWN -> category(chunk.failure());
        };
    }

    static SqlFailureCategory summaryCategory(BatchWriteResult result) {
        if (result.conflictCount() > 0) {
            return SqlFailureCategory.OPTIMISTIC_LOCK;
        }
        BatchChunkResult.Failure failure = firstFailure(result);
        return failure == null ? category(result.status()) : category(failure);
    }

    static SqlFailureCategory category(BatchWriteResult.Status status) {
        return switch (status) {
            case COMMITTED, ENLISTED, ROLLED_BACK -> SqlFailureCategory.NONE;
            case PARTIAL, UNKNOWN -> SqlFailureCategory.UNKNOWN;
        };
    }

    static SqlFailureCategory category(BatchChunkResult.Failure failure) {
        return failure == null
                ? SqlFailureCategory.UNKNOWN
                : SqlFailureCategory.fromKind(failure.kind());
    }

    static SqlFailureCategory recoveryCategory(BatchResolution resolution, Throwable error) {
        if (error != null) {
            return SqlFailureCategory.classify(error);
        }
        return resolution.status() == BatchResolution.Status.COMMITTED
                ? SqlFailureCategory.NONE
                : SqlFailureCategory.UNKNOWN;
    }

    static BatchChunkResult.Failure firstFailure(BatchWriteResult result) {
        return result.chunks()
                     .stream()
                     .map(BatchChunkResult::failure)
                     .filter(Objects::nonNull)
                     .findFirst()
                     .orElse(null);
    }

    static BatchChunkResult.RecoveryToken firstRecoveryToken(BatchWriteResult result) {
        return result.chunks()
                     .stream()
                     .map(BatchChunkResult::recoveryToken)
                     .filter(Objects::nonNull)
                     .findFirst()
                     .orElse(null);
    }

    static long successfulChunkCount(BatchWriteResult result) {
        return result.chunks().stream().filter(chunk -> isSuccessful(chunk.status())).count();
    }

    static boolean isSuccessful(BatchChunkResult.Status status) {
        return status == BatchChunkResult.Status.COMMITTED || status == BatchChunkResult.Status.ENLISTED;
    }
}
