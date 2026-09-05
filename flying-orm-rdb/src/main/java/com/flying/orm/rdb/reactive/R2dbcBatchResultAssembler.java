package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 负责把批量写入过程中的中间结果整理成最终结果。
 *
 * <p>这个类只处理结果，不负责执行 SQL、提交事务或回滚事务。事务层只需要告诉它
 * 哪些分片已经执行过以及这次失败是什么，它就能按统一规则生成结果，避免 ATOMIC
 * 和 INDEPENDENT 各写一套容易不一致的拼装逻辑。</p>
 *
 * <p>chunkIndex 和 startOffset 都来自输入流，不能根据完成顺序重新计算。INDEPENDENT
 * 模式可能并发完成，ATOMIC 模式也可能在某个分片执行后失败，所以结果必须保留输入位置，
 * 调用方才能准确知道哪一段成功、失败、冲突、回滚或未知。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchResultAssembler {

    /**
     * 按输入分片编号排序。
     *
     * <p>不能直接使用列表当前顺序，因为 INDEPENDENT 模式的完成顺序受并发和数据库响应
     * 速度影响。返回不可变列表，避免结果组装完成后被意外修改。</p>
     */
    List<BatchChunkResult> sortedChunks(List<BatchChunkResult> chunks) {
        return chunks.stream()
                     .sorted(Comparator.comparingInt(BatchChunkResult::chunkIndex))
                     .toList();
    }

    /**
     * 给 INDEPENDENT 模式补上导致整批流程结束的最后一个结果。
     *
     * <p>已经完成的分片保留原结果；如果异常带有具体分片，就使用异常中的真实分片位置。
     * 只有拿不到具体分片时，才根据已收集结果推算下一个分片位置。行数上限异常还要保留
     * 它提供的溢出偏移，这样上层能知道输入从哪里被截断。</p>
     */
    List<BatchChunkResult> withGlobalFailure(List<BatchChunkResult> completed, Throwable error) {
        List<BatchChunkResult> results = new ArrayList<>(sortedChunks(completed));
        if (error instanceof BatchWriteException failure
                && failure.result().mode() == BatchWriteOptions.Mode.INDEPENDENT) {
            results.addAll(failure.result().chunks());
            return sortedChunks(results);
        }
        if (error instanceof R2dbcBatchChunkWriteFailure failure) {
            if (failure.exactResult() != null) {
                results.add(failure.exactResult());
                return sortedChunks(results);
            }
            R2dbcBatchWriterChunks.BatchChunk chunk = failure.chunk();
            results.add(BatchChunkResult.failed(chunk.chunkIndex(),
                                                chunk.startOffset(),
                                                chunk.rows().size(),
                                                failureCause(failure)));
            return sortedChunks(results);
        }
        if (error instanceof R2dbcBatchChunkConflictFailure conflict) {
            R2dbcBatchWriterChunks.BatchChunk chunk = conflict.chunk();
            results.add(BatchChunkResult.conflicted(chunk.chunkIndex(),
                                                    chunk.startOffset(),
                                                    chunk.rows().size(),
                                                    conflict.conflicts()));
            return sortedChunks(results);
        }
        int nextChunkIndex = results.stream()
                                    .mapToInt(BatchChunkResult::chunkIndex)
                                    .max()
                                    .orElse(-1) + 1;
        if (error instanceof R2dbcBatchRowLimitExceededException rowLimit) {
            results.add(BatchChunkResult.failed(nextChunkIndex,
                                                rowLimit.exceededOffset(),
                                                1,
                                                rowLimit));
        } else {
            long nextOffset = R2dbcExecutionCounts.sum(results.stream()
                                                               .mapToLong(BatchChunkResult::inputCount));
            results.add(BatchChunkResult.failed(nextChunkIndex, nextOffset, 0, failureCause(error)));
        }
        return results;
    }

    /**
     * 生成 ATOMIC 回滚后的明细。
     *
     * <p>已经执行成功但尚未提交的分片，在回滚成功后必须改成 ROLLED_BACK，不能继续
     * 返回 COMMITTED。当前触发失败的分片仍保留具体失败或乐观锁冲突信息；如果失败发生
     * 在分片生成阶段，则使用已完成分片数量作为下一个分片编号，保持原执行器的位置规则。</p>
     */
    List<BatchChunkResult> rolledBackChunks(List<BatchChunkResult> committed, Throwable error) {
        List<BatchChunkResult> results = new ArrayList<>(rolledBackExecutedChunks(committed));
        if (error instanceof R2dbcBatchChunkConflictFailure conflict) {
            R2dbcBatchWriterChunks.BatchChunk chunk = conflict.chunk();
            results.add(BatchChunkResult.conflicted(chunk.chunkIndex(),
                                                    chunk.startOffset(),
                                                    chunk.rows().size(),
                                                    conflict.conflicts()));
        } else if (error instanceof R2dbcBatchChunkWriteFailure failure
                && !containsChunk(committed, failure.chunk())) {
            R2dbcBatchWriterChunks.BatchChunk chunk = failure.chunk();
            results.add(BatchChunkResult.failed(chunk.chunkIndex(),
                                                chunk.startOffset(),
                                                chunk.rows().size(),
                                                failure.getCause()));
        } else if (error instanceof R2dbcBatchRowLimitExceededException rowLimit) {
            results.add(BatchChunkResult.failed(committed.size(),
                                                rowLimit.exceededOffset(),
                                                1,
                                                rowLimit));
        } else {
            long nextOffset = R2dbcExecutionCounts.sum(committed.stream()
                                                                   .mapToLong(BatchChunkResult::inputCount));
            results.add(BatchChunkResult.failed(committed.size(), nextOffset, 0, failureCause(error)));
        }
        return results;
    }

    /** 外部事务确认回滚时，只转换已经执行过的分片，不额外伪造一个失败分片。 */
    List<BatchChunkResult> rolledBackExecutedChunks(List<BatchChunkResult> executed) {
        return executed.stream()
                       .map(chunk -> BatchChunkResult.rolledBack(chunk.chunkIndex(),
                                                                 chunk.startOffset(),
                                                                 chunk.inputCount()))
                       .toList();
    }

    /**
     * 从内部的分片失败包装中取出真正的数据库异常。
     *
     * <p>包装异常只用来携带分片位置和冲突明细，不应该把包装层暴露成数据库失败原因。
     * 其它异常直接原样返回。</p>
     */
    Throwable failureCause(Throwable error) {
        if (error instanceof R2dbcBatchRowSourceFailure failure) {
            return failure.getCause();
        }
        if (error instanceof R2dbcBatchChunkWriteFailure failure) {
            return failure.getCause();
        }
        if (error instanceof R2dbcBatchChunkConflictFailure conflict) {
            return conflict.getCause();
        }
        return error;
    }

    /**
     * 把外部事务中已经执行完成的内部结果改成 ENLISTED。
     *
     * <p>分片位置和输入数量仍可安全返回，但底层临时记录的影响行数不能暴露成已提交数量，
     * 因为外层事务之后仍可能回滚。</p>
     */
    List<BatchChunkResult> enlistedChunks(List<BatchChunkResult> executed) {
        return executed.stream()
                       .map(chunk -> new BatchChunkResult(chunk.chunkIndex(),
                                                          chunk.startOffset(),
                                                          chunk.inputCount(),
                                                          0,
                                                          BatchChunkResult.Status.ENLISTED,
                                                          null,
                                                          null,
                                                          List.of()))
                       .toList();
    }

    /** 外部事务内执行失败或超时时，事务还没有结束，只能返回不带已提交行数的 UNKNOWN。 */
    BatchWriteResult externalUnknown(List<BatchChunkResult> completed,
                                     Throwable error,
                                     BatchChunkResult.RecoveryToken recoveryToken) {
        R2dbcTransactionParticipationException pending = new R2dbcTransactionParticipationException(
                R2dbcTransactionParticipationException.Reason.OUTCOME_CONTROLLED_BY_EXTERNAL_TRANSACTION);
        R2dbcBatchWriterChunks.BatchChunk activeChunk = failureChunk(error);
        Throwable locatedPending = activeChunk == null
                ? pending : new R2dbcBatchChunkWriteFailure(activeChunk, pending);
        List<BatchChunkResult> unknown = unknownChunks(completed, locatedPending, recoveryToken);
        if (unknown.isEmpty()) {
            return new BatchWriteResult(BatchWriteOptions.Mode.ATOMIC,
                                        BatchWriteResult.Status.UNKNOWN,
                                        0,
                                        0,
                                        List.of());
        }
        return BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, unknown);
    }

    private static R2dbcBatchWriterChunks.BatchChunk failureChunk(Throwable error) {
        if (error instanceof R2dbcBatchChunkWriteFailure failure) {
            return failure.chunk();
        }
        if (error instanceof R2dbcBatchChunkConflictFailure conflict) {
            return conflict.chunk();
        }
        return null;
    }

    /**
     * 生成没有恢复令牌的 UNKNOWN 结果。
     *
     * <p>提交或回滚结果无法确认时，不能把已执行分片标成 FAILED。空列表表示还没有形成
     * 分片，此时返回一个零行 UNKNOWN 占位结果；非空列表则逐个保留原分片位置和输入行数。</p>
     */
    List<BatchChunkResult> unknownChunks(List<BatchChunkResult> committed, Throwable error) {
        return unknownChunks(committed, error, null);
    }

    /**
     * 生成 UNKNOWN 结果，并在调用方已经拿到恢复令牌时把令牌带回去。
     *
     * <p>恢复令牌只描述恢复所需的事实，不会改变 UNKNOWN 的状态含义。调用方必须先确认
     * 数据库中的提交事实，不能仅凭连接关闭成功就把 UNKNOWN 改成成功或失败。</p>
     */
    List<BatchChunkResult> unknownChunks(List<BatchChunkResult> committed,
                                         Throwable error,
                                         BatchChunkResult.RecoveryToken recoveryToken) {
        R2dbcBatchWriterChunks.BatchChunk activeChunk = error instanceof R2dbcBatchChunkWriteFailure failure
                ? failure.chunk() : null;
        Throwable cause = failureCause(error);
        if (committed.isEmpty() && activeChunk == null) {
            return List.of(recoveryToken == null
                                   ? BatchChunkResult.unknown(0, 0, 0, cause)
                                   : BatchChunkResult.unknown(0, 0, 0, cause, recoveryToken));
        }
        List<BatchChunkResult> results = new ArrayList<>(committed.size() + (activeChunk == null ? 0 : 1));
        for (BatchChunkResult chunk : committed) {
            results.add(recoveryToken == null
                                ? BatchChunkResult.unknown(chunk.chunkIndex(),
                                                           chunk.startOffset(),
                                                           chunk.inputCount(),
                                                           cause)
                                : BatchChunkResult.unknown(chunk.chunkIndex(),
                                                           chunk.startOffset(),
                                                           chunk.inputCount(),
                                                           cause,
                                                           recoveryToken));
        }
        if (activeChunk != null && !containsChunk(committed, activeChunk)) {
            results.add(recoveryToken == null
                                ? BatchChunkResult.unknown(activeChunk.chunkIndex(),
                                                           activeChunk.startOffset(),
                                                           activeChunk.rows().size(),
                                                           cause)
                                : BatchChunkResult.unknown(activeChunk.chunkIndex(),
                                                           activeChunk.startOffset(),
                                                           activeChunk.rows().size(),
                                                           cause,
                                                           recoveryToken));
        }
        return results;
    }

    /**
     * 截止时间可能正好落在“完成结果已记录、活动标记尚未清除”的交接瞬间。
     * 这里只在失败结果组装的冷路径按稳定分片位置去重，避免同一输入被报告两次。
     */
    private static boolean containsChunk(List<BatchChunkResult> completed,
                                         R2dbcBatchWriterChunks.BatchChunk candidate) {
        return completed.stream().anyMatch(chunk -> chunk.chunkIndex() == candidate.chunkIndex()
                && chunk.startOffset() == candidate.startOffset());
    }

    /** 补记已经在执行边界完成快照、但尚未形成完整分片的输入行；这里只保留数量，不保留参数值。 */
    List<BatchChunkResult> accountAcceptedRows(List<BatchChunkResult> chunks,
                                               long acceptedRows,
                                               Throwable error) {
        List<BatchChunkResult> accounted = new ArrayList<>(chunks);
        long represented = R2dbcExecutionCounts.sum(
                accounted.stream().mapToLong(BatchChunkResult::inputCount));
        if (acceptedRows <= represented) {
            return accounted;
        }
        int chunkIndex;
        long startOffset;
        if (!accounted.isEmpty()) {
            BatchChunkResult last = accounted.getLast();
            if (last.status() == BatchChunkResult.Status.FAILED && last.inputCount() == 0) {
                accounted.removeLast();
                chunkIndex = last.chunkIndex();
                startOffset = last.startOffset();
            } else {
                chunkIndex = last.chunkIndex() + 1;
                startOffset = represented;
            }
        } else {
            chunkIndex = 0;
            startOffset = 0L;
        }
        int missing = Math.toIntExact(acceptedRows - represented);
        accounted.add(BatchChunkResult.failed(chunkIndex, startOffset, missing, failureCause(error)));
        return accounted;
    }

    BatchWriteResult accountAcceptedRows(BatchWriteResult result,
                                          long acceptedRows,
                                          Throwable error) {
        return BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC,
                accountAcceptedRows(result.chunks(), acceptedRows, error));
    }

    BatchWriteException failureBeforeTransaction(String message,
                                                  Throwable error,
                                                  long acceptedRows) {
        if (error instanceof BatchWriteException batchFailure) {
            return batchFailure;
        }
        Throwable source = failureCause(error);
        BatchChunkResult failed = BatchChunkResult.failed(
                0, 0, Math.toIntExact(acceptedRows), source);
        return new BatchWriteException(message, source,
                BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(failed)));
    }
}
