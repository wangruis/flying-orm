package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableCatalog;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntitySchemaSyncSupportCaseSensitiveTableTest {

    @Test
    void keepsCatalogDistinctPhysicalTablesAsSeparateTargets() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            List<EntitySchemaTarget> targets = EntitySchemaSyncSupport.targets(
                    models, List.of(FirstCatalog.class, SecondCatalog.class));
            assertEquals(List.of(RelationIdentity.of("first", "dbo", "customers"),
                                 RelationIdentity.of("second", "dbo", "customers")),
                         targets.stream().map(target -> target.descriptor().table().identity()).toList());
        }
    }

    @Test
    void keepsDottedSegmentsDistinctButRejectsSameSegmentedIdentity() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertEquals(2, EntitySchemaSyncSupport.targets(models,
                    List.of(DottedSchema.class, DottedTable.class)).size());
            assertThrows(IllegalArgumentException.class, () -> EntitySchemaSyncSupport.targets(models,
                    List.of(FirstCatalog.class, SameFirstCatalog.class)));
            assertEquals(1, EntitySchemaSyncSupport.targets(models,
                    List.of(FirstCatalog.class, FirstCatalog.class)).size());
        }
    }

    @TableCatalog("first")
    @TableName(value = "customers", schema = "dbo")
    private static final class FirstCatalog { @TableId private Long id; }

    @TableCatalog("second")
    @TableName(value = "customers", schema = "dbo")
    private static final class SecondCatalog { @TableId private Long id; }

    @TableCatalog("first")
    @TableName(value = "customers", schema = "dbo")
    private static final class SameFirstCatalog { @TableId private Long id; }

    @TableName(value = "customers", schema = "a.b")
    private static final class DottedSchema { @TableId private Long id; }

    @TableName(value = "b.customers", schema = "a")
    private static final class DottedTable { @TableId private Long id; }

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
