package com.flying.orm.rdb.cache;

import java.util.Objects;

/** flying-orm 所有长期缓存的统一策略入口。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record OrmCachePolicy(CacheRegionPolicy metadata,
                             CacheRegionPolicy sqlPlans,
                             CacheRegionPolicy conditionPlans,
                             CacheRegionPolicy entityMappings) {

    public OrmCachePolicy {
        metadata = Objects.requireNonNull(metadata, "metadata cache policy must not be null");
        sqlPlans = Objects.requireNonNull(sqlPlans, "sql plan cache policy must not be null");
        conditionPlans = Objects.requireNonNull(conditionPlans, "condition plan cache policy must not be null");
        entityMappings = Objects.requireNonNull(entityMappings, "entity mapping cache policy must not be null");
    }

    public static OrmCachePolicy safeDefaults() {
        return new OrmCachePolicy(CacheRegionPolicy.metadataDefaults(),
                                  CacheRegionPolicy.sqlPlanDefaults(),
                                  CacheRegionPolicy.conditionPlanDefaults(),
                                  CacheRegionPolicy.entityMappingDefaults());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** 启动期使用的可变 Builder，构建结果本身仍是不可变值对象。 */
    public static final class Builder {
        private CacheRegionPolicy metadata = CacheRegionPolicy.metadataDefaults();
        private CacheRegionPolicy sqlPlans = CacheRegionPolicy.sqlPlanDefaults();
        private CacheRegionPolicy conditionPlans = CacheRegionPolicy.conditionPlanDefaults();
        private CacheRegionPolicy entityMappings = CacheRegionPolicy.entityMappingDefaults();

        public Builder metadata(CacheRegionPolicy policy) {
            metadata = Objects.requireNonNull(policy, "metadata cache policy must not be null");
            return this;
        }

        public Builder sqlPlans(CacheRegionPolicy policy) {
            sqlPlans = Objects.requireNonNull(policy, "sql plan cache policy must not be null");
            return this;
        }

        public Builder conditionPlans(CacheRegionPolicy policy) {
            conditionPlans = Objects.requireNonNull(policy, "condition plan cache policy must not be null");
            return this;
        }

        public Builder entityMappings(CacheRegionPolicy policy) {
            entityMappings = Objects.requireNonNull(policy, "entity mapping cache policy must not be null");
            return this;
        }

        public OrmCachePolicy build() {
            return new OrmCachePolicy(metadata, sqlPlans, conditionPlans, entityMappings);
        }
    }
}
