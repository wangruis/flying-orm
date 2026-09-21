package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** 单个有界执行窗口的驱动计数；结束后立即归入唯一事实累计器。 */
final class JdbcBatchEvidenceSupport {
    private JdbcBatchEvidenceSupport() {}

    static BatchExecutionState failureState(Throwable failure, boolean hasSuccess, boolean attempted) {
        if (hasSuccess) return BatchExecutionState.PARTIAL;
        return switch (BatchExecutionEvidence.Failure.from(failure).kind()) {
            case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
            case CANCELLED -> BatchExecutionState.CANCELLED;
            case CONNECTION, UNKNOWN -> attempted ? BatchExecutionState.UNKNOWN : BatchExecutionState.FAILED;
            default -> BatchExecutionState.FAILED;
        };
    }

    static int[] executeBusinessBatch(PreparedStatement statement, Counts evidence,
                                     long offset, int size) throws SQLException {
        try {
            int[] result = statement.executeBatch();
            evidence.record(result, offset, size);
            return result;
        } catch (BatchUpdateException failure) {
            evidence.record(failure, offset, size);
            throw failure;
        }
    }

    static final class Counts {
        private final long startOffset;
        private final int inputCount;
        private final BatchRowCountPolicy policy;
        private final BitSet successes = new BitSet();
        private final BitSet failures = new BitSet();
        private final List<BatchRowConflict> conflicts = new ArrayList<>();
        private long affectedRows;
        private boolean known = true;
        private boolean attempted;
        private boolean unverifiable;
        private int completionLimit;
        private BitSet incompleteRows;

        Counts(long offset, int size, BatchRowCountPolicy policy) {
            this.startOffset = offset;
            this.inputCount = size;
            this.policy = policy;
        }

        void record(long offset, long count) {
            int relative = Math.toIntExact(offset - startOffset);
            if (count >= 0) affectedRows = JdbcBatchChunkExecutor.addExact(affectedRows, count);
            else if (count != Statement.EXECUTE_FAILED) known = false;
            if (count == Statement.EXECUTE_FAILED) {
                known = false;
                failures.set(relative);
            } else if (count == Statement.SUCCESS_NO_INFO) {
                successes.set(relative);
                if (policy == BatchRowCountPolicy.EXACTLY_ONE) {
                    unverifiable = true;
                    excludeCompletion(relative);
                }
            } else if (count < 0) {
                unverifiable = true;
            } else if (policy == BatchRowCountPolicy.EXACTLY_ONE && count != 1) {
                failures.set(relative);
                conflicts.add(BatchRowConflict.exactlyOne(offset, count));
            } else {
                successes.set(relative);
            }
        }

        void record(int[] counts, long offset, int size) {
            int reported = counts == null ? 0 : Math.min(size, counts.length);
            for (int i = 0; i < reported; i++) record(offset + i, counts[i]);
            if (counts == null || counts.length != size) known = false;
        }

        void record(BatchUpdateException failure, long offset, int size) {
            long[] counts = failure.getLargeUpdateCounts();
            if (counts == null) {
                record(failure.getUpdateCounts(), offset, size);
                return;
            }
            for (int i = 0; i < Math.min(size, counts.length); i++) record(offset + i, counts[i]);
            if (counts.length != size) known = false;
        }

        void requireSuccessful() throws SQLException {
            requireNoFailures();
            if (successes.cardinality() != inputCount)
                throw new SQLException("jdbc driver returned incomplete batch update counts", "HY000");
        }

        void requireNoFailures() throws SQLException {
            if (!conflicts.isEmpty()) throw new BatchOptimisticLockException(conflicts);
            if (!failures.isEmpty()) throw new SQLException("jdbc driver reported a failed batch item", "HY000");
            if (unverifiable) throw new SQLException("jdbc driver cannot verify exact batch row counts", "0A000");
        }

        void markDatabaseWorkAttempted() { attempted = true; }
        boolean databaseWorkAttempted() { return attempted; }
        void completedRows(int count) { completionLimit = count; }

        void completedPlainRows(List<ProtectedBatchRows.RowView> rows) {
            completionLimit = inputCount;
            for (int index = 0; index < rows.size(); index++) {
                if (rows.get(index).work() != null) excludeCompletion(index);
            }
        }

        boolean isCompletedRow(int index) {
            return index < completionLimit && successes.get(index)
                    && (incompleteRows == null || !incompleteRows.get(index));
        }

        private void excludeCompletion(int index) {
            if (incompleteRows == null) incompleteRows = new BitSet();
            incompleteRows.set(index);
        }

        void finish(BatchExecutionEvidence.Accumulator evidence, boolean success) {
            if (attempted && successes.cardinality() + failures.cardinality() < inputCount) known = false;
            BatchAffectedRows rows = known ? BatchAffectedRows.known(affectedRows) : BatchAffectedRows.unknown();
            if (success && successes.cardinality() == inputCount && failures.isEmpty()) {
                evidence.succeeded(inputCount, rows);
            } else {
                evidence.terminal(inputCount, successes, failures, rows, conflicts);
            }
        }
    }
}
