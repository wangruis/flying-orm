package com.flying.orm.rdb.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BoundedCacheRegionTest {

    @Test
    void entityMappingDefaultsAdmitARealisticWideEntityPlan() {
        AtomicInteger loads = new AtomicInteger();
        BoundedCacheRegion<String, Integer> region = BoundedCacheRegion.create(
                CacheRegionPolicy.entityMappingDefaults(), (key, weight) -> weight);

        assertEquals(128, region.get("wide-entity", ignored -> {
            loads.incrementAndGet();
            return 128;
        }));
        assertEquals(128, region.get("wide-entity", ignored -> {
            loads.incrementAndGet();
            return 128;
        }));
        assertEquals(1, loads.get());
    }

    @Test
    void safeDefaultsEnableBoundedValueFreeConditionPlans() {
        OrmCachePolicy policy = OrmCachePolicy.safeDefaults();
        assertTrue(policy.metadata().enabled());
        assertTrue(policy.sqlPlans().enabled());
        assertTrue(policy.conditionPlans().enabled());
    }

    @Test
    void rejectsOversizedEntriesAndReportsHitsWithoutRetainingThem() {
        CacheRegionPolicy policy = new CacheRegionPolicy(true, 8, 5, Duration.ofMinutes(1), true);
        BoundedCacheRegion<String, String> cache = BoundedCacheRegion.create(policy, (key, value) -> value.length());
        AtomicInteger loads = new AtomicInteger();

        assertEquals("small", cache.get("a", key -> {
            loads.incrementAndGet();
            return "small";
        }));
        assertEquals("small", cache.get("a", key -> "other"));
        assertEquals("oversized", cache.get("b", key -> "oversized"));

        assertEquals(1, loads.get());
        assertEquals(1, cache.snapshot().estimatedSize());
        assertEquals(1, cache.snapshot().rejectedOversizedCount());
        assertTrue(cache.snapshot().hitCount() >= 1);
    }

    @Test
    void disabledRegionLoadsValuesButNeverRetainsThem() {
        BoundedCacheRegion<String, String> cache = BoundedCacheRegion.create(
                CacheRegionPolicy.disabled(), (key, value) -> 1);
        assertEquals("value", cache.get("a", key -> "value"));
        assertNull(cache.getIfPresent("a"));
        assertEquals(0, cache.snapshot().estimatedSize());
    }
}
