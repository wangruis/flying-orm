package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;

import java.util.Objects;

/**
 * 携带输入分片位置或精确失败结果的响应式批量内部异常。
 *
 * <p>驱动异常不知道它来自哪个输入分片。写入器通过该类型恢复稳定的
 * {@link BatchChunkResult}，不向使用方公开新的异常契约。</p>
 */
final class R2dbcBatchChunkWriteFailure extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final R2dbcBatchWriterChunks.BatchChunk chunk;

    private final BatchChunkResult exactResult;

    R2dbcBatchChunkWriteFailure(R2dbcBatchWriterChunks.BatchChunk chunk, Throwable cause) {
        super(cause);
        this.chunk = Objects.requireNonNull(chunk, "batch failure chunk must not be null");
        this.exactResult = null;
    }

    static R2dbcBatchChunkWriteFailure unknown(R2dbcBatchWriterChunks.BatchChunk chunk,
                                                Throwable cause,
                                                BatchChunkResult.RecoveryToken recoveryToken) {
        return exact(unknownResult(chunk, cause, recoveryToken), cause);
    }

    static BatchChunkResult unknownResult(R2dbcBatchWriterChunks.BatchChunk chunk,
                                          Throwable cause,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        R2dbcBatchWriterChunks.BatchChunk safeChunk = Objects.requireNonNull(chunk,
                "batch failure chunk must not be null");
        return unknownResult(safeChunk.chunkIndex(),
                safeChunk.startOffset(),
                safeChunk.rows().size(),
                cause,
                recoveryToken);
    }

    static BatchChunkResult unknownResult(BatchChunkResult executed,
                                          Throwable cause,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        BatchChunkResult safeExecuted = Objects.requireNonNull(executed,
                "executed batch chunk must not be null");
        return unknownResult(safeExecuted.chunkIndex(),
                safeExecuted.startOffset(),
                safeExecuted.inputCount(),
                cause,
                recoveryToken);
    }

    static R2dbcBatchChunkWriteFailure exact(BatchChunkResult exactResult, Throwable cause) {
        return new R2dbcBatchChunkWriteFailure(exactResult, cause);
    }

    private R2dbcBatchChunkWriteFailure(BatchChunkResult exactResult, Throwable cause) {
        super(cause);
        this.chunk = null;
        this.exactResult = Objects.requireNonNull(exactResult, "batch failure result must not be null");
    }

    R2dbcBatchWriterChunks.BatchChunk chunk() {
        return chunk;
    }

    BatchChunkResult exactResult() {
        return exactResult;
    }

    private static BatchChunkResult unknownResult(int chunkIndex,
                                                   long startOffset,
                                                   int inputCount,
                                                   Throwable cause,
                                                   BatchChunkResult.RecoveryToken recoveryToken) {
        return recoveryToken == null
                ? BatchChunkResult.unknown(chunkIndex, startOffset, inputCount, cause)
                : BatchChunkResult.unknown(chunkIndex, startOffset, inputCount, cause, recoveryToken);
    }
}
