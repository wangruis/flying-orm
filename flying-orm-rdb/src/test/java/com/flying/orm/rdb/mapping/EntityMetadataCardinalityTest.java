package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.annotation.OrderBy;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityMetadataCardinalityTest {

    @Test
    void defaultSortsAreFrozenOncePerEntityMetadata() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<OrderedEntity> metadata = models.metadata(OrderedEntity.class);
            QuerySpec first = EntityQueryDefaults.query(metadata.toDynamicForm(),
                    metadata, ConditionGroup.and().build(), FieldScope.unrestricted());
            QuerySpec second = EntityQueryDefaults.query(metadata.toDynamicForm(),
                    metadata, ConditionGroup.and().build(), FieldScope.unrestricted());
            assertEquals(List.of(PageSort.desc("rank"), PageSort.asc("id")), first.sorts());
            assertSame(first.sorts(), second.sorts(),
                    "query creation must reuse the entity's frozen annotation sort plan");
            assertThrows(UnsupportedOperationException.class, () -> first.sorts().clear());
        }
    }

    @TableName("ordered_entities")
    private static final class OrderedEntity {
        @OrderBy(sort = 2)
        private Long id;
        @OrderBy(sort = 1, asc = false)
        private Integer rank;
    }

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

    @Test
    void doesNotSilentlyDiscardAClassLevelLogicDeleteDeclaration() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            assertThrows(MappingException.class, () -> models.metadata(MissingLogicDeleteField.class));
        }
    }

    @TableLogic(field = "missing")
    private static final class MissingLogicDeleteField {
        private Integer deleted;
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
