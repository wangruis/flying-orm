package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionState;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * JDBC 分片 evidence 的状态收集与失败归类；legacy 分片执行不会创建这些对象。
 */
final class JdbcBatchEvidenceSupport {

    private JdbcBatchEvidenceSupport() {
    }

    static BatchExecutionState failureState(Throwable failure, boolean hasSuccess) {
        if (hasSuccess) {
            return BatchExecutionState.PARTIAL;
        }
        return switch (BatchChunkResult.Failure.from(failure).kind()) {
            case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
            case CANCELLED -> BatchExecutionState.CANCELLED;
            default -> BatchExecutionState.FAILED;
        };
    }

    /** 只有主表批处理的返回点才能把驱动更新计数归入业务输入位置。 */
    static int[] executeBusinessBatch(PreparedStatement statement,
                                      Counts evidence,
                                      long startOffset,
                                      int inputCount) throws SQLException {
        try {
            return statement.executeBatch();
        } catch (BatchUpdateException failure) {
            if (evidence != null) {
                evidence.record(failure, startOffset, inputCount);
            }
            throw failure;
        }
    }

    record Outcome(BatchChunkExecutionFact fact,
                   Throwable failure,
                   boolean databaseWorkAttempted) {

        Outcome {
            Objects.requireNonNull(fact, "batch chunk execution fact must not be null");
        }

        boolean successful() {
            return failure == null;
        }
    }

    /** 单分片收集器的生命周期不会越过 executeBatchEvidence 调用。 */
    static final class Counts {

        private final long startOffset;
        private final int inputCount;
        private final BitSet successfulOffsets;
        private final BitSet failedOffsets;
        private long affectedRows;
        private boolean affectedRowsKnown = true;
        private boolean databaseWorkAttempted;

        Counts(long startOffset, int inputCount) {
            this.startOffset = startOffset;
            this.inputCount = inputCount;
            successfulOffsets = new BitSet(inputCount);
            failedOffsets = new BitSet(inputCount);
        }

        void record(long inputOffset, long count) {
            int relativeOffset = Math.toIntExact(inputOffset - startOffset);
            if (count == Statement.EXECUTE_FAILED) {
                failedOffsets.set(relativeOffset);
                return;
            }
            successfulOffsets.set(relativeOffset);
            if (count == Statement.SUCCESS_NO_INFO || count < 0L) {
                affectedRowsKnown = false;
            } else {
                affectedRows = JdbcBatchChunkExecutor.addExact(affectedRows, count);
            }
        }

        void record(BatchUpdateException failure, long executionStartOffset, int executionInputCount) {
            long[] counts = failure.getLargeUpdateCounts();
            if (counts == null) {
                int[] narrowCounts = failure.getUpdateCounts();
                counts = narrowCounts == null ? new long[0]
                        : java.util.Arrays.stream(narrowCounts).asLongStream().toArray();
            }
            int reported = Math.min(counts.length, executionInputCount);
            for (int index = 0; index < reported; index++) {
                record(executionStartOffset + index, counts[index]);
            }
            if (reported < executionInputCount || counts.length > executionInputCount) {
                affectedRowsKnown = false;
            }
        }

        void unknownAffectedRows() {
            affectedRowsKnown = false;
        }

        void markDatabaseWorkAttempted() {
            databaseWorkAttempted = true;
        }

        boolean databaseWorkAttempted() {
            return databaseWorkAttempted;
        }

        boolean hasSuccess() {
            return !successfulOffsets.isEmpty();
        }

        BatchChunkExecutionFact fact(int chunkIndex,
                                     BatchExecutionState state,
                                     Throwable failure) {
            BatchAffectedRows affected = affectedRowsKnown
                    ? BatchAffectedRows.known(affectedRows) : BatchAffectedRows.unknown();
            if (state == BatchExecutionState.SUCCESS
                    && successfulOffsets.cardinality() == inputCount
                    && failedOffsets.isEmpty()) {
                return BatchChunkExecutionFact.allSuccessful(
                        chunkIndex, startOffset, inputCount, affected);
            }
            return BatchChunkExecutionFact.of(
                    chunkIndex,
                    startOffset,
                    inputCount,
                    offsets(successfulOffsets),
                    offsets(failedOffsets),
                    state,
                    affected,
                    failure == null ? null : BatchChunkResult.Failure.from(failure));
        }

        private List<Long> offsets(BitSet compressed) {
            if (compressed.isEmpty()) {
                return List.of();
            }
            return compressed.stream().mapToObj(relative -> startOffset + relative).toList();
        }
    }

}
