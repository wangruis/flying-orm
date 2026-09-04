package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RepositoryOptimisticLocksTest {

    @Test
    void usesEveryCompositePrimaryKeyFieldToLocateABatchUpdate() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<Membership> metadata = models.metadata(Membership.class);
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("tenantId", 7L);
            values.put("userId", 9L);
            values.put("version", 3L);
            values.put("displayName", "Ada");

            BatchOptimisticUpdate update = RepositoryOptimisticLocks.batchUpdate(metadata, values);

            assertEquals(Map.of("displayName", "Ada"), update.values());
            assertEquals(2, update.where().children().size());
            TermCondition tenant = assertInstanceOf(TermCondition.class, update.where().children().get(0));
            TermCondition user = assertInstanceOf(TermCondition.class, update.where().children().get(1));
            assertEquals("tenant_id", tenant.field());
            assertEquals(7L, tenant.value());
            assertEquals("user_id", user.field());
            assertEquals(9L, user.value());
        }
    }

    @TableName("memberships")
    private static final class Membership {
        @TableId
        private Long tenantId;
        @TableId
        private Long userId;
        @Version
        private Long version;
        private String displayName;
    }
}
