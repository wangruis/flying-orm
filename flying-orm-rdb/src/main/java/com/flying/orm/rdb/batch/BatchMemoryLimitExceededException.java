package com.flying.orm.rdb.batch;

/** 当批量输入超过行数、缓冲字节或结果分片硬上限时抛出。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class BatchMemoryLimitExceededException extends RuntimeException {

    private final String limitName;
    private final long limit;
    private final long actual;

    public BatchMemoryLimitExceededException(String limitName, long limit, long actual) {
        super("batch " + limitName + " limit exceeded: limit=" + limit + ", actual=" + actual);
        this.limitName = limitName;
        this.limit = limit;
        this.actual = actual;
    }

    public String limitName() { return limitName; }
    public long limit() { return limit; }
    public long actual() { return actual; }
}
