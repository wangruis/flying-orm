package com.flying.orm.rdb.cache;

/**
 * 框架无关的缓存快照。上层按自己的监控体系读取即可，主项目不创建后台线程，也不主动上报。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record OrmCacheSnapshot(long estimatedSize,
                               long estimatedWeight,
                               long maximumWeight,
                               long hitCount,
                               long missCount,
                               double hitRate,
                               long loadSuccessCount,
                               long loadFailureCount,
                               long totalLoadTimeNanos,
                               long evictionCount,
                               long evictionWeight,
                               long rejectedOversizedCount) {

    /** @return 命中与未命中的总请求次数。统计关闭时为零。 */
    public long requestCount() {
        return saturatedAdd(hitCount, missCount);
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
