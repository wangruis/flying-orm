package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowConflict;

import java.util.List;
import java.util.Objects;

/**
 * 批量分片在响应式链路中传递失败时使用的内部包装类型。
 *
 * <p>驱动异常本身不知道它来自哪一个输入分片。这里保留分片位置，写入器才能把异常变成稳定的
 * {@code BatchChunkResult}，而不是丢失失败行范围。两个类型都只在 reactive 包内协作，
 * 不构成对使用方的公共异常 API。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
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

    /**
     * 为 NEW 或 COMMITTING 等无法确认事务结局的分片构造 UNKNOWN 失败。
     *
     * <p>结果必须仍作为 error 信号传递，不能把超时改成正常 onNext；外层据此保留 writeChunks 和
     * write 的既有终止契约，同时能从包装中恢复精确的 UNKNOWN 与恢复令牌。</p>
     *
     * @param chunk 当前失败分片
     * @param cause 导致结局不可确认的错误
     * @param recoveryToken 可用时用于后续恢复查询的令牌
     * @return 携带精确 UNKNOWN 结果的内部失败
     */
    static R2dbcBatchChunkWriteFailure unknown(R2dbcBatchWriterChunks.BatchChunk chunk,
                                                Throwable cause,
                                                BatchChunkResult.RecoveryToken recoveryToken) {
        return exact(unknownResult(chunk, cause, recoveryToken), cause);
    }

    /**
     * 按尚未提交的分片位置生成 UNKNOWN，避免各事务分支重复实现恢复令牌规则。
     *
     * @param chunk 当前失败分片
     * @param cause 导致结局不可确认的错误
     * @param recoveryToken 可用时用于后续恢复查询的令牌
     * @return 不带已提交影响行数的 UNKNOWN 分片结果
     */
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

    /**
     * 将已执行但提交事实未确认的分片转换为 UNKNOWN，影响行数不再作为提交事实返回。
     *
     * @param executed 已执行但未确认提交的分片结果
     * @param cause 导致结局不可确认的错误
     * @param recoveryToken 可用时用于后续恢复查询的令牌
     * @return 保留输入范围的 UNKNOWN 分片结果
     */
    static BatchChunkResult unknownResult(BatchChunkResult executed,
                                          Throwable cause,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        BatchChunkResult safeExecuted = Objects.requireNonNull(executed, "executed batch chunk must not be null");
        return unknownResult(safeExecuted.chunkIndex(),
                             safeExecuted.startOffset(),
                             safeExecuted.inputCount(),
                             cause,
                             recoveryToken);
    }

    /**
     * 传递已经按事务阶段确定的分片结果，同时保持分片流的 error termination 契约。
     *
     * @param exactResult 已确定的 FAILED 或 UNKNOWN 分片结果
     * @param cause 原始执行错误
     * @return 携带精确结果的内部失败
     */
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

final class R2dbcBatchChunkConflictFailure extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final R2dbcBatchWriterChunks.BatchChunk chunk;

    private final List<BatchRowConflict> conflicts;

    R2dbcBatchChunkConflictFailure(R2dbcBatchWriterChunks.BatchChunk chunk, List<BatchRowConflict> conflicts) {
        super(new BatchOptimisticLockException(conflicts));
        this.chunk = java.util.Objects.requireNonNull(chunk, "batch conflict chunk must not be null");
        this.conflicts = List.copyOf(java.util.Objects.requireNonNull(conflicts, "batch row conflicts must not be null"));
    }

    R2dbcBatchWriterChunks.BatchChunk chunk() {
        return chunk;
    }

    List<BatchRowConflict> conflicts() {
        return conflicts;
    }
}
