package com.flying.orm.rdb.metadata;

import java.util.Objects;

/**
 * 元数据缓存某一时刻的只读统计。这里刻意只放 Java 基础类型，不把 Caffeine 或具体指标框架的类型
 * 暴露给上层；普通 Java 服务可以直接记录日志，有指标系统的服务也可以直接采集同一份数据。
 *
 * <p>forms 和 tables 共享同一个总权重边界，但分别记录命中和加载统计。{@link #combined()} 汇总两类条目和请求，
 * 既能守住整体内存上限，也方便按元数据类型定位命中问题。</p>
 *
 * @param forms  动态表单元数据缓存统计
 * @param tables 数据库表元数据缓存统计
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record MetadataCacheSnapshot(Region forms, Region tables) {

    public MetadataCacheSnapshot {
        forms = Objects.requireNonNull(forms, "form metadata cache snapshot must not be null");
        tables = Objects.requireNonNull(tables, "table metadata cache snapshot must not be null");
    }

    /** @return 两块缓存逐项相加后的整体统计 */
    public Region combined() {
        return forms.plus(tables);
    }

    /**
     * 单块缓存的统计值。所有计数从当前 reader 实例创建后累计，entries 是生成快照时的近似缓存条数。
     *
     * @param entries           当前条目数
     * @param hitCount          命中次数
     * @param missCount         未命中次数
     * @param loadSuccessCount  加载成功次数
     * @param loadFailureCount  加载失败次数
     * @param totalLoadTimeNanos 所有加载累计耗时，单位纳秒
     * @param evictionCount     因容量或过期被驱逐的条目数
     * @param evictionWeight    被驱逐条目的累计权重；当前按条目计权时通常等于 evictionCount
     */
    public record Region(long entries,
                         long hitCount,
                         long missCount,
                         long loadSuccessCount,
                         long loadFailureCount,
                         long totalLoadTimeNanos,
                         long evictionCount,
                         long evictionWeight) {

        public Region {
            requireNonNegative(entries, "metadata cache entries");
            requireNonNegative(hitCount, "metadata cache hit count");
            requireNonNegative(missCount, "metadata cache miss count");
            requireNonNegative(loadSuccessCount, "metadata cache load success count");
            requireNonNegative(loadFailureCount, "metadata cache load failure count");
            requireNonNegative(totalLoadTimeNanos, "metadata cache total load time");
            requireNonNegative(evictionCount, "metadata cache eviction count");
            requireNonNegative(evictionWeight, "metadata cache eviction weight");
        }

        /** @return 命中和未命中的总请求次数 */
        public long requestCount() {
            return saturatedAdd(hitCount, missCount);
        }

        /** @return 加载成功和失败的总次数 */
        public long loadCount() {
            return saturatedAdd(loadSuccessCount, loadFailureCount);
        }

        /**
         * 命中率没有请求时返回 1，避免健康检查刚启动就显示成 NaN。真正发生请求后按命中数计算。
         */
        public double hitRate() {
            if (hitCount == 0L && missCount == 0L) {
                return 1D;
            }
            return (double) hitCount / ((double) hitCount + (double) missCount);
        }

        Region plus(Region other) {
            Region safeOther = Objects.requireNonNull(other, "metadata cache region snapshot must not be null");
            return new Region(saturatedAdd(entries, safeOther.entries),
                              saturatedAdd(hitCount, safeOther.hitCount),
                              saturatedAdd(missCount, safeOther.missCount),
                              saturatedAdd(loadSuccessCount, safeOther.loadSuccessCount),
                              saturatedAdd(loadFailureCount, safeOther.loadFailureCount),
                              saturatedAdd(totalLoadTimeNanos, safeOther.totalLoadTimeNanos),
                              saturatedAdd(evictionCount, safeOther.evictionCount),
                              saturatedAdd(evictionWeight, safeOther.evictionWeight));
        }

        private static long saturatedAdd(long left, long right) {
            return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
        }

        private static void requireNonNegative(long value, String name) {
            if (value < 0) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
        }
    }
}
