package com.flying.orm.rdb.cache;

import java.time.Duration;
import java.util.Objects;

/**
 * 一个缓存区域的完整边界。这里的 weight 是稳定的逻辑重量，不是 JVM 对象字节数；它用来让不同机器
 * 得到一致的准入和淘汰行为，也避免在热路径做昂贵的对象大小扫描。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record CacheRegionPolicy(boolean enabled,
                                long maximumWeight,
                                int maximumEntryWeight,
                                Duration expireAfterAccess,
                                boolean recordStats) {

    public CacheRegionPolicy {
        if (maximumWeight <= 0 || maximumWeight >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("cache maximum weight must be between 1 and Integer.MAX_VALUE - 1");
        }
        if (maximumEntryWeight <= 0 || maximumEntryWeight > maximumWeight) {
            throw new IllegalArgumentException("cache maximum entry weight must be positive and not exceed total weight");
        }
        expireAfterAccess = Objects.requireNonNull(expireAfterAccess,
                                                   "cache expire after access must not be null");
        if (expireAfterAccess.isZero() || expireAfterAccess.isNegative()) {
            throw new IllegalArgumentException("cache expire after access must be positive");
        }
    }

    public static CacheRegionPolicy metadataDefaults() {
        return new CacheRegionPolicy(true, 16_384, 1_024, Duration.ofMinutes(5), true);
    }

    public static CacheRegionPolicy sqlPlanDefaults() {
        return new CacheRegionPolicy(true, 32_768, 2_048, Duration.ofMinutes(10), true);
    }

    public static CacheRegionPolicy conditionPlanDefaults() {
        return new CacheRegionPolicy(true, 16_384, 1_024, Duration.ofMinutes(10), true);
    }

    public static CacheRegionPolicy entityMappingDefaults() {
        return new CacheRegionPolicy(true, 16_384, 1_024, Duration.ofHours(1), true);
    }

    /** 关闭区域时仍保留合法边界，避免以后打开同一配置时意外得到无限缓存。 */
    public static CacheRegionPolicy disabled() {
        return new CacheRegionPolicy(false, 1, 1, Duration.ofMinutes(1), false);
    }
}
