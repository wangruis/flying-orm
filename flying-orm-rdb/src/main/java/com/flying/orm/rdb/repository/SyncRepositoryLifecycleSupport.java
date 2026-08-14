package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * 同步 Repository 的生命周期边界。
 *
 * <p>实体注解回调和 {@link ReactiveEntityListener} 是既有的响应式扩展契约。同步 JDBC 执行普通 CRUD 时，
 * 没有任何回调就直接返回，不创建 Mono、不会等待，也不会把 JDBC 工作塞回 Reactor。只有真的声明了回调或监听器时，
 * 才在这里这个明确的同步边界等待监听器完成。</p>
 *
 * @param <T> 实体类型
 */
final class SyncRepositoryLifecycleSupport<T> {

    private final EntityLifecycleDispatcher<T> dispatcher;
    private final SyncRepositoryAwaiter awaiter;

    SyncRepositoryLifecycleSupport(EntityMetadata<T> metadata,
                                   ReactiveEntityListener<T> listener,
                                   SyncRepositoryAwaiter awaiter) {
        this.dispatcher = new EntityLifecycleDispatcher<>(
                Objects.requireNonNull(metadata, "repository entity metadata must not be null"), listener);
        this.awaiter = Objects.requireNonNull(awaiter, "sync repository awaiter must not be null");
    }

    boolean hasWork(EntityLifecyclePhase phase) {
        return dispatcher.hasWork(Objects.requireNonNull(phase, "entity lifecycle phase must not be null"));
    }

    void rejectNonBlockingThread() {
        awaiter.rejectNonBlockingThread();
    }

    long persist(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        fire(EntityLifecyclePhase.PRE_PERSIST, safeEntity, null);
        long rows = requireOperation(operation).getAsLong();
        fire(EntityLifecyclePhase.POST_PERSIST, safeEntity, rows);
        return rows;
    }

    long update(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        fire(EntityLifecyclePhase.PRE_UPDATE, safeEntity, null);
        long rows = requireOperation(operation).getAsLong();
        fire(EntityLifecyclePhase.POST_UPDATE, safeEntity, rows);
        return rows;
    }

    long remove(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        fire(EntityLifecyclePhase.PRE_REMOVE, safeEntity, null);
        long rows = requireOperation(operation).getAsLong();
        fire(EntityLifecyclePhase.POST_REMOVE, safeEntity, rows);
        return rows;
    }

    List<T> postLoad(List<T> entities) {
        List<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            // 最常见的查询没有监听器；这里直接还给 JDBC 映射结果，避免每次 select 都复制一次 List。
            return safeEntities;
        }
        List<T> result = new ArrayList<>(safeEntities.size());
        for (T entity : safeEntities) {
            fire(EntityLifecyclePhase.POST_LOAD, entity, null);
            result.add(entity);
        }
        return List.copyOf(result);
    }

    PageResult<T> postLoad(PageResult<T> page) {
        PageResult<T> safePage = Objects.requireNonNull(page, "repository page must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            return safePage;
        }
        List<T> rows = postLoad(safePage.rows());
        return new PageResult<>(rows, safePage.total(), safePage.page(), safePage.size());
    }

    void fire(EntityLifecyclePhase phase, T entity, Object result) {
        EntityLifecyclePhase safePhase = Objects.requireNonNull(phase, "entity lifecycle phase must not be null");
        if (dispatcher.hasWork(safePhase)) {
            // 生命周期监听器本来就是 Publisher 契约。这里是同步调用唯一允许等待它的位置。
            awaiter.awaitCompletion(dispatcher.fire(safePhase, entity, result));
        }
    }

    private static LongSupplier requireOperation(LongSupplier operation) {
        return Objects.requireNonNull(operation, "repository write operation must not be null");
    }
}
