package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.BatchExecutionObservation;

import java.util.Objects;

/**
 * 流式批量观测只记汇总值，不保留每个分片。分片再多，观测本身占用的内存也保持固定。
 *
 * <p>实例按一次订阅创建，由 Reactor 的串行 onNext 规则驱动，不在线程之间共享，因此无需锁或原子变量。
 * 如果未来改成并行消费同一个 accumulator，必须先重新设计同步策略，不能直接复用当前实现。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
final class BatchObservationAccumulator {

    private final BatchWriteOptions.Mode mode;

    private long inputCount;
    private long affectedRows;
    private long conflictCount;
    private long chunkCount;
    private long successfulChunkCount;
    private long failedChunkCount;
    private boolean allCommitted = true;
    private boolean allEnlisted = true;
    private boolean anyEnlisted;
    private boolean unknown;
    private BatchChunkResult.Failure firstFailure;
    private BatchChunkResult.RecoveryToken firstRecoveryToken;

    BatchObservationAccumulator(BatchWriteOptions.Mode mode) {
        this.mode = Objects.requireNonNull(mode, "batch mode must not be null");
    }

    void add(BatchChunkResult chunk) {
        BatchChunkResult safeChunk = Objects.requireNonNull(chunk, "batch chunk result must not be null");
        chunkCount = addExact(chunkCount, 1L);
        if (safeChunk.status() == BatchChunkResult.Status.COMMITTED
                || safeChunk.status() == BatchChunkResult.Status.ENLISTED) {
            successfulChunkCount = addExact(successfulChunkCount, 1L);
        } else {
            failedChunkCount = addExact(failedChunkCount, 1L);
        }
        inputCount = addExact(inputCount, safeChunk.inputCount());
        // 只有明确提交的分片才能计入 affectedRows；回滚、失败和 UNKNOWN 都不能虚报成功行数。
        if (safeChunk.status() == BatchChunkResult.Status.COMMITTED) {
            affectedRows = addExact(affectedRows, safeChunk.affectedRows());
        } else {
            allCommitted = false;
        }
        if (safeChunk.status() != BatchChunkResult.Status.ENLISTED) {
            allEnlisted = false;
        }
        anyEnlisted |= safeChunk.status() == BatchChunkResult.Status.ENLISTED;
        unknown |= safeChunk.status() == BatchChunkResult.Status.UNKNOWN;
        conflictCount = addExact(conflictCount, safeChunk.conflicts().size());
        if (firstFailure == null) {
            firstFailure = safeChunk.failure();
        }
        if (firstRecoveryToken == null) {
            firstRecoveryToken = safeChunk.recoveryToken();
        }
    }

    long inputCount() {
        return inputCount;
    }

    long affectedRows() {
        return affectedRows;
    }

    BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request,
                                      long durationNanos) {
        return BatchExecutionObservation.summary(request,
                                                 status(),
                                                 inputCount,
                                                 affectedRows,
                                                 conflictCount,
                                                 chunkCount,
                                                 successfulChunkCount,
                                                 failedChunkCount,
                                                 firstFailure,
                                                 firstRecoveryToken,
                                                 durationNanos);
    }

    BatchExecutionObservation failedSummary(BatchExecutionObservation.BatchWriteRequestView request,
                                            long durationNanos,
                                            Throwable error) {
        BatchChunkResult.Failure failure = firstFailure == null
                ? BatchChunkResult.Failure.from(Objects.requireNonNull(
                        error, "batch observation error must not be null"))
                : firstFailure;
        return BatchExecutionObservation.summary(request,
                                                 BatchWriteResult.Status.UNKNOWN,
                                                 inputCount,
                                                 affectedRows,
                                                 conflictCount,
                                                 chunkCount,
                                                 successfulChunkCount,
                                                 failedChunkCount,
                                                 failure,
                                                 firstRecoveryToken,
                                                 durationNanos);
    }

    BatchWriteResult.Status status() {
        // UNKNOWN 优先级最高，因为任何一个不确定分片都会让整批最终事实不完整。
        if (unknown) {
            return BatchWriteResult.Status.UNKNOWN;
        }
        if (allCommitted) {
            return BatchWriteResult.Status.COMMITTED;
        }
        if (anyEnlisted) {
            return allEnlisted ? BatchWriteResult.Status.ENLISTED : BatchWriteResult.Status.UNKNOWN;
        }
        return mode == BatchWriteOptions.Mode.ATOMIC
                ? BatchWriteResult.Status.ROLLED_BACK
                : BatchWriteResult.Status.PARTIAL;
    }

    private static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }
}
