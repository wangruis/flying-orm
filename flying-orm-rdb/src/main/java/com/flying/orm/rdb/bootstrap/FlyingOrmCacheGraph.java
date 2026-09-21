package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.util.Objects;

/**
 * 管理一套客户端对象图拥有的 ORM 内存缓存，不接管连接池和外部执行器。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class FlyingOrmCacheGraph implements AutoCloseable {

    private final EntityModelRegistry entityModels;
    private final MetadataCacheInvalidator metadata;

    FlyingOrmCacheGraph(EntityModelRegistry entityModels,
                        MetadataCacheInvalidator metadata) {
        this.entityModels = Objects.requireNonNull(entityModels, "entity model registry must not be null");
        this.metadata = Objects.requireNonNull(metadata, "metadata cache invalidator must not be null");
    }

    @Override
    public void close() {
        // 先释放长期持有应用 Class 的实体计划，再清理元数据和 SQL 结构缓存。
        entityModels.close();
        metadata.invalidateAll();
    }
}
