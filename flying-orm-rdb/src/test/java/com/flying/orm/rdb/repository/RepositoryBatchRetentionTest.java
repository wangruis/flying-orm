package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import org.reactivestreams.Subscription;

import static org.junit.jupiter.api.Assertions.*;

class RepositoryBatchRetentionTest {
    @Test
    void mappedRetentionFollowsDemandAndCancelsOnMappingFailure() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryBatchRetention<Row> retained = new RepositoryBatchRetention<>(
                    RepositoryEntityIdSupport.create(models.metadata(Row.class), models), false, Long.MAX_VALUE,
                    "retainedBytes", (entity, row) -> fail("unlimited budget must not estimate"));
            Row first = new Row();
            Row second = new Row();
            List<Long> requests = new ArrayList<>();
            List<Map<String, Object>> received = new ArrayList<>();
            AtomicInteger cancelled = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            IllegalArgumentException expected = new IllegalArgumentException("mapping failed");
            BaseSubscriber<Map<String, Object>> consumer = new BaseSubscriber<>() {
                protected void hookOnSubscribe(Subscription subscription) { request(1); }
                protected void hookOnNext(Map<String, Object> row) { received.add(row); }
                protected void hookOnError(Throwable error) { failure.set(error); }
            };
            retained.map(Flux.just(first, second).doOnRequest(requests::add)
                    .doOnCancel(cancelled::incrementAndGet), entity -> {
                        if (entity == second) throw expected;
                        return Map.<String, Object>of("value", 1L);
                    }).subscribe(consumer);
            assertEquals(List.of(1L), requests);
            assertEquals(List.of(Map.of("value", 1L)), received);
            assertSame(first, retained.complete(0));
            consumer.request(1);
            assertSame(expected, failure.get());
            assertEquals(1, cancelled.get());
            assertNull(retained.complete(1));
            retained.clear();
        }
    }

    @Test
    void mappedRetentionRestartsOffsetsPerSubscriptionAndRejectsNullRows() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryBatchRetention<Row> retained = new RepositoryBatchRetention<>(
                    RepositoryEntityIdSupport.create(models.metadata(Row.class), models), false, Long.MAX_VALUE,
                    "retainedBytes", (entity, row) -> 1);
            Row entity = new Row();
            Flux<String> mapped = retained.map(Flux.just(entity), ignored -> "value");
            for (int subscription = 0; subscription < 2; subscription++) {
                assertEquals(List.of("value"), mapped.collectList().block());
                assertSame(entity, retained.complete(0));
            }
            assertThrows(NullPointerException.class,
                    () -> retained.map(Flux.just(entity), ignored -> null).collectList().block());
            assertNull(retained.complete(0));
        }
    }

    @Test
    void sharedRetentionReleasesBudgetAndNeverRevertsSuccessfulKeys() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            RepositoryEntityIdSupport<Row> ids = RepositoryEntityIdSupport.create(models.metadata(Row.class), models);
            RepositoryBatchRetention<Row> retained = new RepositoryBatchRetention<>(ids, true, 10,
                    "lifecycleRetainedBytes", (entity, row) -> 6);
            Row first = new Row();
            retained.remember(0, first, Map.of());
            assertThrows(BatchMemoryLimitExceededException.class, () -> retained.remember(1, new Row(), Map.of()));
            retained.generatedKeys().accept(0, DynamicRow.copyOf(Map.of("id", 7L)));
            assertSame(first, retained.complete(0));
            assertNull(retained.complete(0));
            retained.remember(1, new Row(), Map.of());
            assertThrows(MappingException.class, () -> retained.complete(1));
            retained.clear();
            assertEquals(7L, first.id);
            assertThrows(MappingException.class, () -> retained.generatedKeys().accept(0, DynamicRow.copyOf(Map.of("id", 8L))));
        }
    }

    @Test
    void unlimitedRetentionDoesNotEstimateOrReadExtraFields() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            AtomicInteger estimates = new AtomicInteger();
            RepositoryBatchRetention<Row> retained = new RepositoryBatchRetention<>(
                    RepositoryEntityIdSupport.create(models.metadata(Row.class), models), false, Long.MAX_VALUE,
                    "combinedRetainedBytes", (entity, row) -> estimates.incrementAndGet());
            Row row = new Row();
            retained.remember(0, row, Map.of());
            assertSame(row, retained.complete(0));
            assertEquals(0, estimates.get());
            assertFalse(retained.generatedKeys().required());
        }
    }

    private static final class Row {
        @TableId(type = IdType.AUTO)
        private Long id;
    }
}
