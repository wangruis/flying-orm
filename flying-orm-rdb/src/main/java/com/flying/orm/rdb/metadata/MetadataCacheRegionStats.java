package com.flying.orm.rdb.metadata;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.concurrent.atomic.LongAdder;

/**
 * 单个元数据缓存区域的高并发统计计数器。
 *
 * <p>LongAdder 避免高频命中时争用同一把锁。快照允许各字段存在很小的时间差，语义与 Caffeine
 * 自己的监控统计一致，适合看趋势，不用于业务记账。</p>
 */
final class MetadataCacheRegionStats {

    private final boolean enabled;
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder successCount = new LongAdder();
    private final LongAdder failureCount = new LongAdder();
    private final LongAdder totalLoadTimeNanos = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder evictionWeight = new LongAdder();

    MetadataCacheRegionStats(boolean enabled) {
        this.enabled = enabled;
    }

    void hit() {
        if (enabled) {
            hitCount.increment();
        }
    }

    void miss() {
        if (enabled) {
            missCount.increment();
        }
    }

    void success(long elapsedNanos) {
        if (enabled) {
            successCount.increment();
            totalLoadTimeNanos.add(Math.max(0L, elapsedNanos));
        }
    }

    void failure(long elapsedNanos) {
        if (enabled) {
            failureCount.increment();
            totalLoadTimeNanos.add(Math.max(0L, elapsedNanos));
        }
    }

    void eviction(int weight) {
        if (enabled) {
            evictionCount.increment();
            evictionWeight.add(weight);
        }
    }

    MetadataCacheSnapshot.Region snapshot(long entries) {
        return new MetadataCacheSnapshot.Region(entries,
                                                hitCount.sum(),
                                                missCount.sum(),
                                                successCount.sum(),
                                                failureCount.sum(),
                                                totalLoadTimeNanos.sum(),
                                                evictionCount.sum(),
                                                evictionWeight.sum());
    }

    CacheStats caffeineStats() {
        return CacheStats.of(hitCount.sum(),
                             missCount.sum(),
                             successCount.sum(),
                             failureCount.sum(),
                             totalLoadTimeNanos.sum(),
                             evictionCount.sum(),
                             evictionWeight.sum());
    }
}
