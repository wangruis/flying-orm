package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 以引用计数管理派生客户端共享的缓存图和字段保护密钥。
 *
 * <p>{@link FlyingOrmClients} 的 {@code with...} 方法只改变默认策略，底层渲染器、缓存与密钥仍是同一对象图。
 * 每个派生客户端持有一份引用，只有最后一个客户端关闭时才真正清理资源，避免一个视图提前使其他视图失效。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class FlyingOrmSharedResources {

    private final FlyingOrmCacheGraph cacheGraph;
    private final ProtectedFieldRuntime protectedFields;
    private final AtomicInteger references = new AtomicInteger(1);

    FlyingOrmSharedResources(FlyingOrmCacheGraph cacheGraph, ProtectedFieldRuntime protectedFields) {
        this.cacheGraph = Objects.requireNonNull(cacheGraph, "client cache graph must not be null");
        this.protectedFields = Objects.requireNonNull(
                protectedFields, "protected field runtime must not be null");
    }

    /** 为一个新派生客户端保留共享资源；资源已经释放后不能复活。 */
    void retain() {
        int current = references.get();
        while (current > 0) {
            if (references.compareAndSet(current, current + 1)) {
                return;
            }
            current = references.get();
        }
        throw new IllegalStateException("flying ORM client resources are closed");
    }

    /** 释放一个客户端引用，并在最后一个引用离开时按缓存、密钥的顺序完成清理。 */
    void release() {
        int remaining = references.decrementAndGet();
        if (remaining < 0) {
            references.incrementAndGet();
            throw new IllegalStateException("flying ORM client resources are already released");
        }
        if (remaining == 0) {
            try {
                cacheGraph.close();
            } finally {
                protectedFields.close();
            }
        }
    }
}
