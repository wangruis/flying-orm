package com.flying.orm.rdb.lifecycle;

import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Objects;

/**
 * 原生响应式监听器收到的只读现场。
 *
 * @param metadata 实体的缓存元数据
 * @param entity 当前实体
 * @param phase 当前阶段
 * @param result SQL 成功结果；before 阶段和 PostLoad 没有结果时为 null
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record EntityLifecycleEvent<T>(EntityMetadata<T> metadata,
                                      T entity,
                                      EntityLifecyclePhase phase,
                                      Object result) {

    public EntityLifecycleEvent {
        metadata = Objects.requireNonNull(metadata, "entity lifecycle metadata must not be null");
        entity = Objects.requireNonNull(entity, "entity lifecycle entity must not be null");
        phase = Objects.requireNonNull(phase, "entity lifecycle phase must not be null");
    }
}
