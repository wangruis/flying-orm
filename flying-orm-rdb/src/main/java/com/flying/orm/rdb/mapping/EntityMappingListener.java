package com.flying.orm.rdb.mapping;

import java.util.List;
import java.util.Objects;

/**
 * 实体映射生命周期监听器，不依赖 Spring，也不绑定线程本地变量。
 *
 * <p>同一个监听器可能被多个 R2DBC 订阅并发调用，实现类必须自己保证线程安全。回调运行在当前映射链路中，
 * 抛出的异常会终止本次读写，因此这里只适合轻量审计、字段归一化结果检查和指标采集，不应做阻塞网络调用。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface EntityMappingListener {

    EntityMappingListener NONE = new EntityMappingListener() {
    };

    /** 写参数已经从实体读取完成、尚未交给 SQL 渲染器时调用。 */
    default void beforeWrite(EntityMappingEvent event) {
    }

    /** 数据库行已经成功构造成实体后调用。 */
    default void afterRead(EntityMappingEvent event) {
    }

    /** @return 不做任何事、可以全局复用的监听器 */
    static EntityMappingListener none() {
        return NONE;
    }

    /**
     * 按声明顺序组合多个监听器。返回对象只保存不可变列表，可安全复用。
     *
     * @param listeners 需要依次执行的监听器
     * @return 组合后的监听器
     */
    static EntityMappingListener compose(EntityMappingListener... listeners) {
        List<EntityMappingListener> ordered = List.of(listeners).stream()
                                                      .map(listener -> Objects.requireNonNull(
                                                              listener, "entity mapping listener must not be null"))
                                                      .filter(listener -> listener != NONE)
                                                      .toList();
        if (ordered.isEmpty()) {
            return NONE;
        }
        return new EntityMappingListener() {
            @Override
            public void beforeWrite(EntityMappingEvent event) {
                ordered.forEach(listener -> listener.beforeWrite(event));
            }

            @Override
            public void afterRead(EntityMappingEvent event) {
                ordered.forEach(listener -> listener.afterRead(event));
            }
        };
    }
}
