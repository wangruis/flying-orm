package com.flying.orm.rdb.batch;

/**
 * Input-buffer and accepted-row limits for one sequential batch execution.
 *
 * @author wangr
 * @version v4.1.0
 */
public record BatchWriteOptions(int bufferSize, long maxRows, long maxBufferedBytes, long maxRowBytes) {
    public static final int DEFAULT_BUFFER_SIZE = 500;
    public static final long DEFAULT_MAX_ROWS = 100_000L;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 32L * 1024 * 1024;

    public BatchWriteOptions {
        if (bufferSize <= 0) throw new IllegalArgumentException("batch buffer size must be greater than zero");
        if (maxRows < 0) throw new IllegalArgumentException("batch max rows must not be negative");
        if (maxBufferedBytes <= 0) throw new IllegalArgumentException("batch max buffered bytes must be greater than zero");
        if (maxRowBytes <= 0 || maxRowBytes > maxBufferedBytes) {
            throw new IllegalArgumentException("batch max row bytes must fit within the input buffer budget");
        }
    }

    public static BatchWriteOptions defaults() { return of(DEFAULT_BUFFER_SIZE); }

    public static BatchWriteOptions of(int bufferSize) {
        return new BatchWriteOptions(bufferSize, DEFAULT_MAX_ROWS, DEFAULT_MAX_BUFFERED_BYTES,
                                     defaultMaxRowBytes(bufferSize, DEFAULT_MAX_BUFFERED_BYTES));
    }

    /** Zero allows unlimited accepted rows; input buffers remain bounded. */
    public BatchWriteOptions withMaxRows(long maxRows) {
        return new BatchWriteOptions(bufferSize, maxRows, maxBufferedBytes, maxRowBytes);
    }

    /** Recalculates the default per-row reservation for the new buffer budget. */
    public BatchWriteOptions withMemoryLimits(long maxRows, long maxBufferedBytes) {
        return new BatchWriteOptions(bufferSize, maxRows, maxBufferedBytes,
                                     defaultMaxRowBytes(bufferSize, maxBufferedBytes));
    }

    public BatchWriteOptions withMaxRowBytes(long maxRowBytes) {
        return new BatchWriteOptions(bufferSize, maxRows, maxBufferedBytes, maxRowBytes);
    }

    private static long defaultMaxRowBytes(int bufferSize, long maxBufferedBytes) {
        return Math.max(1L, maxBufferedBytes / (bufferSize == 1 ? 1 : 2));
    }
}
