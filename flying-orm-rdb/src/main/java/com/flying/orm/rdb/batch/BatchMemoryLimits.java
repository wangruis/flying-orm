package com.flying.orm.rdb.batch;

/**
 * 一次批量任务允许占用的进程级硬边界。
 *
 * <p>分片、并发、缓冲和结果明细必须有界。总行数只由 {@link BatchWriteOptions#maxRows()} 表达，
 * 避免进程级配置和单次请求维护两套相互冲突的任务规模政策。五参数构造器及 {@link #maxRows()}
 * 为公开 ABI 兼容而保留，其值不再参与 {@link #check(BatchWriteOptions)}。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record BatchMemoryLimits(int maxChunkSize,
                                int maxConcurrency,
                                long maxRows,
                                long maxBufferedBytes,
                                int maxResultChunks) {

    public static final int DEFAULT_MAX_CHUNK_SIZE = 10_000;
    public static final int DEFAULT_MAX_CONCURRENCY = 32;
    public static final long DEFAULT_MAX_ROWS = 10_000_000L;
    public static final long DEFAULT_MAX_BUFFERED_BYTES = 256L * 1024 * 1024;
    public static final int DEFAULT_MAX_RESULT_CHUNKS = 65_536;

    public BatchMemoryLimits {
        if (maxChunkSize <= 0) {
            throw new IllegalArgumentException("batch hard max chunk size must be greater than zero");
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("batch hard max concurrency must be greater than zero");
        }
        if (maxRows <= 0) {
            throw new IllegalArgumentException("batch max rows must be greater than zero");
        }
        if (maxBufferedBytes <= 0) {
            throw new IllegalArgumentException("batch max buffered bytes must be greater than zero");
        }
        if (maxResultChunks <= 0) {
            throw new IllegalArgumentException("batch max result chunks must be greater than zero");
        }
    }

    public static BatchMemoryLimits defaults() {
        return new BatchMemoryLimits(DEFAULT_MAX_CHUNK_SIZE,
                                     DEFAULT_MAX_CONCURRENCY,
                                     DEFAULT_MAX_ROWS,
                                     DEFAULT_MAX_BUFFERED_BYTES,
                                     DEFAULT_MAX_RESULT_CHUNKS);
    }

    /**
     * 在订阅输入和申请连接之前检查单次配置。单次配置可以比这里更保守，但不能临时放大进程级的
     * 分片、并发、缓冲或结果明细边界。总行数由单次配置独立决定。
     */
    public void check(BatchWriteOptions options) {
        BatchWriteOptions safeOptions = java.util.Objects.requireNonNull(options,
                                                                          "batch write options must not be null");
        check("chunk size", maxChunkSize, safeOptions.chunkSize());
        check("concurrency", maxConcurrency, safeOptions.concurrency());
        check("buffered bytes", maxBufferedBytes, safeOptions.maxBufferedBytes());
        check("result chunks", maxResultChunks, safeOptions.maxResultChunks());
    }

    private static void check(String name, long limit, long actual) {
        if (actual > limit) {
            throw new BatchMemoryLimitExceededException(name, limit, actual);
        }
    }
}
