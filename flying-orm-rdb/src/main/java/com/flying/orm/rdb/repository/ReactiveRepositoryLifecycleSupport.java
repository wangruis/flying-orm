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

    boolean hasWork(EntityLifecyclePhase phase) {
        return dispatcher.hasWork(phase);
    }

    Mono<Long> persist(T entity, Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST,
                     operation);
    }

    Mono<Long> update(T entity,
                      Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                     operation);
    }

    Mono<Long> remove(T entity,
                      Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_REMOVE, EntityLifecyclePhase.POST_REMOVE,
                     operation);
    }

    Flux<T> postLoad(Flux<T> entities) {
        Flux<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            return safeEntities;
        }
        // 回调按数据库结果顺序串行完成，避免监听器看到乱序实体。
        return safeEntities.concatMap(entity ->
                fire(EntityLifecyclePhase.POST_LOAD, entity, null).thenReturn(entity));
    }

    Mono<PageResult<T>> postLoad(PageResult<T> page) {
        PageResult<T> safePage = Objects.requireNonNull(page, "repository page must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            return Mono.just(safePage);
        }
        return postLoad(Flux.fromIterable(safePage.rows()))
                .collectList()
                .map(rows -> new PageResult<>(rows, safePage.total(), safePage.page(), safePage.size()));
    }

    private Mono<Long> write(T entity,
                             EntityLifecyclePhase prePhase,
                             EntityLifecyclePhase postPhase,
                             Supplier<Mono<Long>> operation) {
        Supplier<Mono<Long>> safeOperation = Objects.requireNonNull(
                operation, "repository write operation must not be null");
        return fire(prePhase, entity, null).then(Mono.defer(() -> {
            if (!hasWork(postPhase)) {
                return requireOperation(safeOperation.get());
            }
            return requireOperation(safeOperation.get())
                    .flatMap(rows -> fire(postPhase, entity, rows).thenReturn(rows));
        }));
    }

    private static Mono<Long> requireOperation(Mono<Long> operation) {
        return Objects.requireNonNull(operation, "repository write operation must return a Mono");
    }
}
