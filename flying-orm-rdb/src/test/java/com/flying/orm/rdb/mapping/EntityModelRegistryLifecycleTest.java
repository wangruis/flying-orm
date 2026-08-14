package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityModelRegistryLifecycleTest {

    @Test
    void closeClearsEntriesAndStatisticsAndPreventsLaterRetention() {
        EntityModelRegistry registry = registry();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        registry.metadata(User.class);
        registry.entityValues(User.class);
        registry.rowMapper(User.class, codecs);
        registry.rowMapper(User.class, codecs);
        assertTrue(registry.estimatedMappings() > 0L);
        assertTrue(registry.stats().requestCount() > 0L);

        registry.close();
        registry.close();

        assertEquals(0L, registry.estimatedMappings());
        assertEquals(0L, registry.stats().requestCount());
        assertNotSame(registry.metadata(User.class), registry.metadata(User.class));
        assertNotSame(registry.entityValues(User.class), registry.entityValues(User.class));
        assertNotSame(registry.rowMapper(User.class, codecs), registry.rowMapper(User.class, codecs));
        assertEquals(0L, registry.estimatedMappings());
    }

    @Test
    void concurrentFirstLoadSharesOnePlanAndConcurrentCloseLeavesNothingRetained() throws Exception {
        EntityModelRegistry registry = registry();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        CountDownLatch start = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<RowMapper<User>>> loads = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                loads.add(executor.submit(() -> {
                    start.await();
                    return registry.rowMapper(User.class, codecs);
                }));
            }
            start.countDown();
            RowMapper<User> first = loads.getFirst().get();
            for (Future<RowMapper<User>> load : loads) {
                assertSame(first, load.get());
            }
        }

        CountDownLatch closeStart = new CountDownLatch(1);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> races = new ArrayList<>();
            for (int i = 0; i < 64; i++) {
                races.add(executor.submit(() -> {
                    closeStart.await();
                    registry.rowMapper(User.class, codecs);
                    return null;
                }));
            }
            Future<?> close = executor.submit(() -> {
                closeStart.await();
                registry.close();
                return null;
            });
            closeStart.countDown();
            close.get();
            for (Future<?> race : races) {
                race.get();
            }
        }

        assertEquals(0L, registry.estimatedMappings());
        assertEquals(0L, registry.stats().requestCount());
    }

    private static EntityModelRegistry registry() {
        return EntityModelRegistry.create(new CacheRegionPolicy(
                true, 256, 128, Duration.ofMinutes(5), true));
    }

    private record User(long id, String name) {
    }
}
