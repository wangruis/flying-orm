package com.flying.orm.rdb.batch;

import java.util.Objects;

/**
 * Process-level bounds on one batch input buffer. Total rows belong to the request options.
 *
 * @author wangr
 * @version v4.1.0
 */
public record BatchMemoryLimits(int maxBufferSize, long maxBufferedBytes) {
    public static final int DEFAULT_MAX_BUFFER_SIZE = 10_000;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 256L * 1024 * 1024;

    public BatchMemoryLimits {
        if (maxBufferSize <= 0) throw new IllegalArgumentException("batch hard max buffer size must be greater than zero");
        if (maxBufferedBytes <= 0) throw new IllegalArgumentException("batch max buffered bytes must be greater than zero");
    }

    public static BatchMemoryLimits defaults() {
        return new BatchMemoryLimits(DEFAULT_MAX_BUFFER_SIZE, DEFAULT_MAX_BUFFERED_BYTES);
    }

    public void check(BatchWriteOptions options) {
        Objects.requireNonNull(options, "batch write options must not be null");
        check("buffer size", maxBufferSize, options.bufferSize());
        check("buffered bytes", maxBufferedBytes, options.maxBufferedBytes());
    }

    private static void check(String name, long limit, long actual) {
        if (actual > limit) throw new BatchMemoryLimitExceededException(name, limit, actual);
    }
}
