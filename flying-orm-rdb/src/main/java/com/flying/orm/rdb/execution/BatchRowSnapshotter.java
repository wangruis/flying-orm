package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.internal.InternalApi;
import java.util.Objects;

@InternalApi
public final class BatchRowSnapshotter {
    private BatchRowSnapshotter() {
    }

    public static Object[] snapshot(Object[] row) {
        return Objects.requireNonNull(row, "batch row must not be null");
    }

    public static Object[] snapshot(Object[] row,
                                    int parameterCount,
                                    long maxBufferedBytes,
                                    String limitName) {
        return snapshotView(row, parameterCount, maxBufferedBytes, limitName).row();
    }

    public static Snapshot snapshotAndEstimate(Object[] row,
                                               int parameterCount,
                                               long maxBufferedBytes,
                                               String limitName) {
        ProtectedBatchRows.RowView rowView = snapshotView(
                row, parameterCount, maxBufferedBytes, limitName);
        return new Snapshot(rowView.row(), rowView.estimatedBytes());
    }

    /** Validates one owned batch row once and keeps the decoded view for the full execution chain. */
    public static ProtectedBatchRows.RowView snapshotView(Object[] row,
                                                           int parameterCount,
                                                           long maxBufferedBytes,
                                                           String limitName) {
        Object[] safeRow = requireArguments(row, maxBufferedBytes, limitName);
        ProtectedBatchRows.RowView rowView = ProtectedBatchRows.decode(safeRow, parameterCount);
        long estimatedBytes = ProtectedBatchRows.estimateRowBytes(rowView);
        requireWithinBudget(estimatedBytes, maxBufferedBytes, limitName);
        // BatchWriteRequest 明确把整行所有权交给执行器；这里只校验形状并计算预算，避免热路径重复复制大字段。
        return rowView.withEstimatedBytes(estimatedBytes);
    }

    private static Object[] requireArguments(Object[] row, long maxBufferedBytes, String limitName) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        if (maxBufferedBytes <= 0L) {
            throw new IllegalArgumentException("batch snapshot byte limit must be positive");
        }
        Objects.requireNonNull(limitName, "batch snapshot limit name must not be null");
        return safeRow;
    }

    private static void requireWithinBudget(long actual, long limit, String limitName) {
        if (actual == Long.MAX_VALUE || actual > limit) {
            throw new BatchMemoryLimitExceededException(limitName, limit, actual);
        }
    }

    public record Snapshot(Object[] row, long estimatedBytes) { }
}
