package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EntitySchemaSyncSupportCaseSensitiveTableTest {

    @Test
    void keepsCaseDistinctPhysicalTablesAsSeparateSynchronizationTargets() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            List<EntitySchemaTarget> targets = EntitySchemaSyncSupport.targets(
                    models, List.of(UpperCaseTable.class, LowerCaseTable.class));

            assertEquals(List.of("CustomerData", "customerdata"),
                         targets.stream().map(target -> target.metadata().table()).toList());
        }
    }

    @TableName("CustomerData")
    private static final class UpperCaseTable {
        @TableId
        private Long id;
    }

    @TableName("customerdata")
    private static final class LowerCaseTable {
        @TableId
        private Long id;
    }
}
