package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchLifecycleCommitHandoffTest {

    @Test
    void appliedGeneratedKeysSurviveClearWithoutAnyTransactionHandoff() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<GeneratedEntity> metadata = models.metadata(GeneratedEntity.class);
            RepositoryBatchRetention<GeneratedEntity> tracker = new RepositoryBatchRetention<>(
                    RepositoryEntityIdSupport.create(metadata, models), true, 4_096L, "retainedBytes",
                    (entity, row) -> 1L);
            List<GeneratedEntity> entities = List.of(new GeneratedEntity(), new GeneratedEntity());
            for (int index = 0; index < entities.size(); index++) tracker.remember(index, entities.get(index), Map.of());
            tracker.generatedKeys().accept(0L, DynamicRow.copyOf(Map.of("id", 1_001L)));
            tracker.generatedKeys().accept(1L, DynamicRow.copyOf(Map.of("id", 1_002L)));
            tracker.clear();
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
