package com.flying.orm.rdb.reactive;

/**
 * 批量输入行数超过请求上限时抛出，并保留第一条越界输入的全局偏移。
 *
 * <p>它只表示输入行数越界。结果分片数和缓冲字节数的保护沿用
 * {@code BatchMemoryLimitExceededException}，这样上层可以区分“输入太多”和“内存预算不足”。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchRowLimitExceededException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final long exceededOffset;

    R2dbcBatchRowLimitExceededException(long offset, long maxRows) {
        super("batch write row count exceeds max rows: maxRows=" + maxRows + ", overflowOffset=" + offset);
        this.exceededOffset = offset;
    }

    /**
     * @return 第一条未被接受的输入行的零基偏移
     */
    long exceededOffset() {
        return exceededOffset;
    }
}
