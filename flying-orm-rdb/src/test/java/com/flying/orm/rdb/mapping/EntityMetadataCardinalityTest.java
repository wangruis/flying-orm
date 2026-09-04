package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityMetadataCardinalityTest {

    @Test
    void rejectsMultipleVersionFields() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertThrows(MappingException.class, () -> models.metadata(MultipleVersions.class));
        }
    }

    @Test
    void rejectsMultipleLogicDeleteFields() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertThrows(MappingException.class, () -> models.metadata(MultipleLogicDeletes.class));
        }
    }

    @TableName("multiple_versions")
    private static final class MultipleVersions {
        @Version
        private Long firstVersion;
        @Version
        private Long secondVersion;
    }

    @TableName("multiple_logic_deletes")
    private static final class MultipleLogicDeletes {
        @TableLogic
        private Integer firstDeleted;
        @TableLogic
        private Integer secondDeleted;
    }
}
