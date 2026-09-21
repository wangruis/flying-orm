package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchLifecycleCommitHandoffTest {

    @Test
    void appliedGeneratedKeysSurviveAbortWithoutAnyTransactionHandoff() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<GeneratedEntity> metadata = models.metadata(GeneratedEntity.class);
            BatchLifecycleTracker<GeneratedEntity> tracker = new BatchLifecycleTracker<>(
                    new EntityLifecycleDispatcher<>(metadata, null), EntityLifecyclePhase.POST_PERSIST,
                    RepositoryEntityIdSupport.create(metadata, models), true, 4_096L,
                    (entity, row) -> new BatchLifecycleTracker.RetainedMemory(1L, 1L));
            List<GeneratedEntity> entities = List.of(new GeneratedEntity(), new GeneratedEntity());
            Flux.from(tracker.rows(Flux.fromIterable(entities), ignored -> Map.of(),
                                   EntityLifecyclePhase.PRE_PERSIST))
                .collectList().block(Duration.ofSeconds(2));
            tracker.generatedKeys().accept(0L, DynamicRow.copyOf(Map.of("id", 1_001L)));
            tracker.generatedKeys().accept(1L, DynamicRow.copyOf(Map.of("id", 1_002L)));
            tracker.abort();
            assertEquals(List.of(1_001L, 1_002L), entities.stream().map(GeneratedEntity::getId).toList());
        }
    }

    private static final class GeneratedEntity {
        @TableId(type = IdType.AUTO)
        private Long id;

        private Long getId() {
            return id;
        }
    }
}
