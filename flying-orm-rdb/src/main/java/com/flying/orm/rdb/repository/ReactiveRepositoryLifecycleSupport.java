package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * 收口响应式 Repository 的生命周期调用。
 *
 * <p>这里不执行 SQL，也不保存实体状态，只负责保证回调顺序：before 回调完成后才执行数据库操作，
 * 数据库操作成功后才执行 after 回调。查询结果使用 concatMap，因此后置回调不会改变数据库返回顺序。
 * 取消和异常仍由 Reactor 链路向下游传播，不在这里偷偷订阅或阻塞。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryLifecycleSupport<T> {

    private final EntityLifecycleDispatcher<T> dispatcher;

    ReactiveRepositoryLifecycleSupport(EntityMetadata<T> metadata,
                                       ReactiveEntityListener<T> listener) {
        this.dispatcher = new EntityLifecycleDispatcher<>(
                Objects.requireNonNull(metadata, "repository entity metadata must not be null"), listener);
    }

    EntityLifecycleDispatcher<T> dispatcher() {
        return dispatcher;
    }

    Mono<Void> fire(EntityLifecyclePhase phase, T entity, Object result) {
        return dispatcher.fire(phase, entity, result);
    }

    Mono<Long> update(T entity, Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return fire(EntityLifecyclePhase.PRE_UPDATE, safeEntity, null)
                .then(Mono.defer(Objects.requireNonNull(operation, "repository update operation must not be null")))
                .flatMap(rows -> fire(EntityLifecyclePhase.POST_UPDATE, safeEntity, rows).thenReturn(rows));
    }

    Mono<Long> remove(T entity, Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return fire(EntityLifecyclePhase.PRE_REMOVE, safeEntity, null)
                .then(Mono.defer(Objects.requireNonNull(operation, "repository delete operation must not be null")))
                .flatMap(rows -> fire(EntityLifecyclePhase.POST_REMOVE, safeEntity, rows).thenReturn(rows));
    }

    Flux<T> postLoad(Flux<T> entities) {
        return Objects.requireNonNull(entities, "repository entities must not be null")
                // 回调按数据库结果顺序串行完成，避免监听器看到乱序实体。
                .concatMap(entity -> fire(EntityLifecyclePhase.POST_LOAD, entity, null).thenReturn(entity));
    }

    Mono<PageResult<T>> postLoad(PageResult<T> page) {
        PageResult<T> safePage = Objects.requireNonNull(page, "repository page must not be null");
        return postLoad(Flux.fromIterable(safePage.rows()))
                .collectList()
                .map(rows -> new PageResult<>(rows, safePage.total(), safePage.page(), safePage.size()));
    }
}
