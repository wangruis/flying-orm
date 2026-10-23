package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

class RepositoryUpdateSupportTest {

    @Test
    void internalIdentityUsesOneCanonicalLookupPerKeyWhilePublicKeysKeepAliases() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<Membership> metadata = models.metadata(Membership.class);
            Map<String, Object> identity = new HashMap<>() {
                @Override
                public boolean containsKey(Object key) {
                    throw new AssertionError("owned identity already has canonical column names");
                }
            };
            identity.put("tenant_id", 7L);
            identity.put("user_id", 9L);
            assertEquals(RepositoryKeys.fromKey(metadata, Map.of("tenantId", 7L, "USER_ID", 9L)).children(),
                    RepositoryKeys.fromValues(metadata, identity).children());
        }
    }

    @Test
    void ordinaryEntityBatchUpdateDoesNotRequireAVersionField() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<OrdinaryMembership> metadata = models.metadata(OrdinaryMembership.class);
            OrdinaryMembership entity = new OrdinaryMembership();
            entity.tenantId = 7L;
            entity.userId = 9L;
            entity.displayName = "Ada";
            BatchUpdate update = RepositoryUpdateSupport.batchUpdate(
                    metadata, models.entityValues(OrdinaryMembership.class), entity);
            assertNull(update.lock());
            assertEquals(Map.of("display_name", "Ada"), update.values());
            assertEquals(2, update.where().children().size());
            assertEquals("tenant_id", assertInstanceOf(TermCondition.class, update.where().children().get(0)).field());
            assertEquals("user_id", assertInstanceOf(TermCondition.class, update.where().children().get(1)).field());
        }
    }

    @Test
    void usesEveryCompositePrimaryKeyFieldToLocateABatchUpdate() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntityMetadata<Membership> metadata = models.metadata(Membership.class);
            Membership entity = new Membership();
            entity.tenantId = 7L;
            entity.userId = 9L;
            entity.version = 3L;
            entity.displayName = "Ada";

            BatchUpdate update = RepositoryUpdateSupport.batchUpdate(
                    metadata, models.entityValues(Membership.class), entity);

            assertEquals(Map.of("display_name", "Ada"), update.values());
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
    private static final class OrdinaryMembership {
        @TableId private Long tenantId;
        @TableId private Long userId;
        private String displayName;
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
