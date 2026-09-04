package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.util.List;
import java.util.Objects;

/**
 * 一次批量调用已经形成的 SQL 执行事实与独立事务事实。
 *
 * <p>{@link #affectedRows()} 统计驱动已证明的执行行数，不把 PENDING_EXTERNAL 猜成已提交。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class BatchExecutionEvidence {

    private final BatchWriteOptions.Mode mode;
    private final BatchExecutionState state;
    private final BatchCommitFact commitFact;
    private final long inputCount;
    private final BatchAffectedRows affectedRows;
    private final List<BatchChunkExecutionFact> chunks;

    private BatchExecutionEvidence(BatchWriteOptions.Mode mode,
                                   BatchExecutionState state,
                                   BatchCommitFact commitFact,
                                   List<BatchChunkExecutionFact> chunks) {
        this.mode = Objects.requireNonNull(mode, "batch evidence mode must not be null");
        this.state = Objects.requireNonNull(state, "batch evidence state must not be null");
        this.commitFact = Objects.requireNonNull(commitFact, "batch commit fact must not be null");
        this.chunks = List.copyOf(Objects.requireNonNull(chunks, "batch evidence chunks must not be null"));
        inputCount = sumInput(this.chunks);
        affectedRows = sumAffected(this.chunks);
    }

    public static BatchExecutionEvidence of(BatchWriteOptions.Mode mode,
                                            BatchExecutionState state,
                                            BatchCommitFact commitFact,
                                            List<BatchChunkExecutionFact> chunks) {
        return new BatchExecutionEvidence(mode, state, commitFact, chunks);
    }

    public BatchWriteOptions.Mode mode() {
        return mode;
    }

    public BatchExecutionState state() {
        return state;
    }

    public BatchCommitFact commitFact() {
        return commitFact;
    }

    public long inputCount() {
        return inputCount;
    }

    public BatchAffectedRows affectedRows() {
        return affectedRows;
    }

    public List<BatchChunkExecutionFact> chunks() {
        return chunks;
    }

    private static long sumInput(List<BatchChunkExecutionFact> chunks) {
        long result = 0L;
        try {
            for (BatchChunkExecutionFact chunk : chunks) {
                result = Math.addExact(result, chunk.inputCount());
            }
            return result;
        } catch (ArithmeticException overflow) {
            throw countOverflow(overflow);
        }
    }

    private static BatchAffectedRows sumAffected(List<BatchChunkExecutionFact> chunks) {
        long result = 0L;
        try {
            for (BatchChunkExecutionFact chunk : chunks) {
                if (!chunk.affectedRows().isKnown()) {
                    return BatchAffectedRows.unknown();
                }
                result = Math.addExact(result, chunk.affectedRows().value());
            }
            return BatchAffectedRows.known(result);
        } catch (ArithmeticException overflow) {
            throw countOverflow(overflow);
        }
    }

    private static RdbException countOverflow(ArithmeticException overflow) {
        return new RdbException(RdbErrorKind.UNKNOWN,
                                "database execution count exceeds supported range",
                                null,
                                null,
                                overflow);
    }
}
