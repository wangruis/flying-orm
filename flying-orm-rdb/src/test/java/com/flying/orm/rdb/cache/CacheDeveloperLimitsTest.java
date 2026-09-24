package com.flying.orm.rdb.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CacheDeveloperLimitsTest {
    @Test
    void totalCacheWeightUsesItsDeclaredLongRange() {
        CacheRegionPolicy policy = new CacheRegionPolicy(true, Long.MAX_VALUE, Integer.MAX_VALUE,
                Duration.ofMinutes(1), true);
        BoundedCacheRegion<String, String> cache = BoundedCacheRegion.create(policy, (key, value) -> Integer.MAX_VALUE);
        assertTrue(cache.put("one", "first"));
        assertTrue(cache.put("two", "second"));
        assertEquals(2L * Integer.MAX_VALUE, cache.snapshot().estimatedWeight());
        assertEquals("first", cache.getIfPresent("one"));
    }

    @Test
    void developerEntryBudgetStillRejectsRetentionWithALargeTotalBudget() {
        for (long maximum : new long[]{100, (long) Integer.MAX_VALUE + 1, Long.MAX_VALUE}) {
            CacheRegionPolicy policy = new CacheRegionPolicy(true, maximum, 3, Duration.ofMinutes(1), true);
            BoundedCacheRegion<String, String> cache = BoundedCacheRegion.create(policy, (key, value) -> value.length());
            AtomicInteger loads = new AtomicInteger();
            for (int repeat = 0; repeat < 2; repeat++) {
                assertEquals("large", cache.get("key", key -> { loads.incrementAndGet(); return "large"; }));
                assertNull(cache.getIfPresent("key"));
            }
            assertEquals(2, loads.get());
            assertTrue(cache.put("key", "ok"));
            assertFalse(cache.put("key", "large"));
            assertNull(cache.getIfPresent("key"));
            assertEquals(0, cache.snapshot().estimatedSize());
            assertEquals(3, cache.snapshot().rejectedOversizedCount());
        }
    }
}
