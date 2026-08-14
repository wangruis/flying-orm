package com.flying.orm.rdb.batch;

/**
 * 一次批量任务允许占用的硬边界。
 *
 * <p>这些值都必须是正数，不再用 0 表示无限。批量数据常由外部 Publisher 提供，输入规模可能直到运行时
 * 才知道，所以执行器必须边消费边检查，超过任一边界就取消上游并在拿更多连接前失败。</p>
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
     * 在订阅输入和申请连接之前检查单次配置。单次配置可以比这里更保守，但不能临时把整个进程的
     * 内存和并发边界放大。
     */
    public void check(BatchWriteOptions options) {
        BatchWriteOptions safeOptions = java.util.Objects.requireNonNull(options,
                                                                          "batch write options must not be null");
        check("chunk size", maxChunkSize, safeOptions.chunkSize());
        check("concurrency", maxConcurrency, safeOptions.concurrency());
        check("rows", maxRows, safeOptions.maxRows());
        check("buffered bytes", maxBufferedBytes, safeOptions.maxBufferedBytes());
        check("result chunks", maxResultChunks, safeOptions.maxResultChunks());
    }

    private static void check(String name, long limit, long actual) {
        if (actual > limit) {
            throw new BatchMemoryLimitExceededException(name, limit, actual);
        }
    }
}
