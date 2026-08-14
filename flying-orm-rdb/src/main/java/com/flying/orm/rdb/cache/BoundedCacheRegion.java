package com.flying.orm.rdb.cache;

import com.flying.orm.rdb.internal.InternalApi;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntBiFunction;

/**
 * Caffeine 有界缓存的薄封装。加载使用 Caffeine 的原子 get，同一个 key 高并发首次访问只执行一次
 * loader。单条对象过大时仍返回本次结果，但不会把它留在缓存中。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@InternalApi
public final class BoundedCacheRegion<K, V> {

    private final CacheRegionPolicy policy;
    private final Cache<K, WeightedValue<V>> cache;
    private final ToIntBiFunction<K, V> weigher;
    private final LongAdder rejectedOversized = new LongAdder();

    @SuppressWarnings("unchecked")
    private BoundedCacheRegion(CacheRegionPolicy policy, ToIntBiFunction<K, V> weigher) {
        this.policy = Objects.requireNonNull(policy, "cache region policy must not be null");
        this.weigher = Objects.requireNonNull(weigher, "cache region weigher must not be null");
        Caffeine<K, WeightedValue<V>> builder = (Caffeine<K, WeightedValue<V>>) (Caffeine<?, ?>) Caffeine.newBuilder();
        builder.maximumWeight(policy.maximumWeight())
               .weigher((key, value) -> value.caffeineWeight());
        builder.expireAfterAccess(policy.expireAfterAccess());
        if (policy.recordStats()) {
            builder.recordStats();
        }
        this.cache = builder.build();
    }

    public static <K, V> BoundedCacheRegion<K, V> create(CacheRegionPolicy policy,
                                                          ToIntBiFunction<K, V> weigher) {
        return new BoundedCacheRegion<>(policy, weigher);
    }

    public V get(K key, Function<? super K, ? extends V> loader) {
        K safeKey = Objects.requireNonNull(key, "cache key must not be null");
        Function<? super K, ? extends V> safeLoader = Objects.requireNonNull(loader,
                                                                              "cache loader must not be null");
        if (!policy.enabled()) {
            return Objects.requireNonNull(safeLoader.apply(safeKey), "cache loader must not return null");
        }
        WeightedValue<V> value = cache.get(safeKey, currentKey -> weighted(currentKey, safeLoader.apply(currentKey)));
        return Objects.requireNonNull(value, "cache loader must not return null").value();
    }

    public V getIfPresent(K key) {
        if (!policy.enabled()) {
            return null;
        }
        WeightedValue<V> value = cache.getIfPresent(Objects.requireNonNull(key, "cache key must not be null"));
        return value == null ? null : value.value();
    }

    /** @return true 表示对象已进入缓存，false 表示区域关闭或对象过大。 */
    public boolean put(K key, V value) {
        K safeKey = Objects.requireNonNull(key, "cache key must not be null");
        V safeValue = Objects.requireNonNull(value, "cache value must not be null");
        if (!policy.enabled()) {
            return false;
        }
        WeightedValue<V> weighted = weighted(safeKey, safeValue);
        cache.put(safeKey, weighted);
        return !weighted.oversized();
    }

    public void invalidate(K key) {
        cache.invalidate(Objects.requireNonNull(key, "cache key must not be null"));
    }

    public void invalidateIf(Predicate<? super K> predicate) {
        cache.asMap().keySet().removeIf(Objects.requireNonNull(predicate, "cache predicate must not be null"));
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }

    public OrmCacheSnapshot snapshot() {
        cache.cleanUp();
        CacheStats stats = cache.stats();
        long weight = cache.policy().eviction()
                           .map(eviction -> eviction.weightedSize().orElse(0L))
                           .orElse(0L);
        return new OrmCacheSnapshot(cache.estimatedSize(), weight, policy.maximumWeight(),
                                    stats.hitCount(), stats.missCount(), stats.hitRate(),
                                    stats.loadSuccessCount(), stats.loadFailureCount(), stats.totalLoadTime(),
                                    stats.evictionCount(), stats.evictionWeight(), rejectedOversized.sum());
    }

    private WeightedValue<V> weighted(K key, V value) {
        V safeValue = Objects.requireNonNull(value, "cache loader must not return null");
        int weight = Math.max(1, weigher.applyAsInt(key, safeValue));
        boolean oversized = weight > policy.maximumEntryWeight();
        if (oversized) {
            rejectedOversized.increment();
        }
        // weight 大于总上限的条目由 Caffeine 返回给本次调用但不保留，不需要永久旁路 Map。
        int caffeineWeight = oversized ? Math.toIntExact(policy.maximumWeight() + 1L) : weight;
        return new WeightedValue<>(safeValue, caffeineWeight, oversized);
    }

    private record WeightedValue<V>(V value, int caffeineWeight, boolean oversized) {
    }
}
