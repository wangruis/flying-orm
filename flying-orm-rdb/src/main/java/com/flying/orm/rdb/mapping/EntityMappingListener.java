package com.flying.orm.rdb.mapping;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * 实体映射回调，不依赖 Spring，也不绑定线程本地变量。
 *
 * <p>通过 {@link RowMapper#of(Class, EntityMappingListener)} 注册的监听器只自动收到 {@link #afterRead}。
 * {@link #beforeWrite} 由调用方显式派发，ORM 写入入口没有自动注册或调用它的机制。</p>
 *
 * <p>同一个监听器可能被多个 R2DBC 订阅并发调用，实现类必须自己保证线程安全。
 * afterRead 异常会终止当前映射链路；手动 beforeWrite 的异常直接传播给调用方，由调用方决定是否继续写入。
 * 回调只适合轻量检查、审计和指标采集，不应做阻塞网络调用。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public interface EntityMappingListener {

    EntityMappingListener NONE = new EntityMappingListener() {
    };

    /**
     * 调用方可在写参数读取完成后、执行写入前显式派发；ORM 不会自动调用本方法。
     * 抛出的异常传播给派发方，不代表数据库写入或事务结果。
     *
     * @param event 调用方提供的映射事件
     */
    default void beforeWrite(EntityMappingEvent event) {
    }

    /**
     * 注册到 RowMapper 后，在数据库行成功构造成实体时自动调用。
     *
     * @param event 当前读取映射事件
     */
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
        List<EntityMappingListener> ordered = Arrays.stream(Objects.requireNonNull(
                        listeners, "entity mapping listeners must not be null"))
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
