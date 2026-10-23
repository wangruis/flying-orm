package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.internal.sync.SyncBlockingGuard;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

/**
 * 同步 Repository 的生命周期边界。
 *
 * <p>{@link ReactiveEntityListener} 是同步和响应式 Repository 共用的生命周期扩展契约。同步 JDBC 执行普通 CRUD 时，
 * 没有监听器就直接返回，不创建 Mono、不会等待，也不会把 JDBC 工作塞回 Reactor。只有注册了监听器时，
 * 才在这里这个明确的同步边界等待监听器完成。</p>
 *
 * @param <T> 实体类型
 */
final class SyncRepositoryLifecycleSupport<T> {

    private final EntityLifecycleDispatcher<T> dispatcher;

    SyncRepositoryLifecycleSupport(EntityMetadata<T> metadata,
                                   ReactiveEntityListener<T> listener) {
        this.dispatcher = new EntityLifecycleDispatcher<>(
                Objects.requireNonNull(metadata, "repository entity metadata must not be null"), listener);
    }

    boolean hasWork() {
        return dispatcher.hasWork();
    }

    void rejectNonBlockingThread() {
        SyncBlockingGuard.rejectNonBlockingThread();
    }

    long persist(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST,
                     operation);
    }

    long update(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                     operation);
    }

    long remove(T entity, LongSupplier operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_REMOVE, EntityLifecyclePhase.POST_REMOVE,
                     operation);
    }

    List<T> postLoad(List<T> entities) {
        List<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        if (!hasWork()) {
            // 最常见的查询没有监听器；这里直接还给 JDBC 映射结果，避免每次 select 都复制一次 List。
            return safeEntities;
        }
        for (T entity : safeEntities) {
            fire(EntityLifecyclePhase.POST_LOAD, entity, null);
        }
        return safeEntities;
    }

    PageResult<T> postLoad(PageResult<T> page) {
        PageResult<T> safePage = Objects.requireNonNull(page, "repository page must not be null");
        if (!hasWork()) {
            return safePage;
        }
        postLoad(safePage.rows());
        return safePage;
    }

    void fire(EntityLifecyclePhase phase, T entity, Object result) {
        EntityLifecyclePhase safePhase = Objects.requireNonNull(phase, "entity lifecycle phase must not be null");
        if (dispatcher.hasWork()) {
            // 生命周期监听器本来就是 Publisher 契约。这里是同步调用唯一允许等待它的位置。
            SyncBlockingGuard.nullable(dispatcher.fire(safePhase, entity, result));
        }
    }

    /** 批量输入可能来自事件线程；先组合 PRE，再映射实体，不在上游发布线程等待。 */
    Publisher<T> beforeRows(Publisher<T> source, EntityLifecyclePhase phase) {
        if (!dispatcher.hasWork()) {
            return source;
        }
        return Flux.defer(() -> {
            Demand demand = new Demand();
            Publisher<T> requestedSource = subscriber -> source.subscribe(new Subscriber<>() {
                public void onSubscribe(Subscription subscription) {
                    if (demand.connect(subscription)) {
                        subscriber.onSubscribe(demand);
                    }
                }
                public void onNext(T entity) { subscriber.onNext(entity); }
                public void onError(Throwable error) { subscriber.onError(error); }
                public void onComplete() { subscriber.onComplete(); }
            });
            // concatMap(0) 仍会补取下一项；按 JDBC 的真实需求放行原源，避免提前消费和执行 PRE。
            return Flux.from(requestedSource)
                    .concatMap(entity -> dispatcher.fire(phase, entity, null).thenReturn(entity), 0)
                    .doOnRequest(demand::allow);
        });
    }

    private long write(T entity,
                       EntityLifecyclePhase prePhase,
                       EntityLifecyclePhase postPhase,
                       LongSupplier operation) {
        fire(prePhase, entity, null);
        LongSupplier safeOperation = Objects.requireNonNull(
                operation, "repository write operation must not be null");
        if (!hasWork()) {
            return safeOperation.getAsLong();
        }
        long rows = safeOperation.getAsLong();
        fire(postPhase, entity, rows);
        return rows;
    }

    /** 只把原下游已请求的额度交给异步 PRE 链；串行组合器的补取不能扩大该额度。 */
    private static final class Demand implements Subscription {
        private final AtomicInteger draining = new AtomicInteger();
        private Subscription upstream;
        private long allowed;
        private long pending;
        private boolean cancelled;

        synchronized boolean connect(Subscription subscription) {
            if (upstream != null || cancelled) {
                subscription.cancel();
                return false;
            }
            upstream = subscription;
            return true;
        }

        void allow(long count) {
            synchronized (this) { allowed = add(allowed, count); }
            drain();
        }

        @Override
        public void request(long count) {
            synchronized (this) { pending = add(pending, count); }
            drain();
        }

        private void drain() {
            if (draining.getAndIncrement() != 0) {
                return;
            }
            do {
                Subscription target;
                long count;
                synchronized (this) {
                    target = cancelled ? null : upstream;
                    count = target == null ? 0 : Math.min(allowed, pending);
                    allowed -= count;
                    pending -= count;
                }
                if (count != 0) {
                    target.request(count);
                }
            } while (draining.decrementAndGet() != 0);
        }

        @Override
        public void cancel() {
            Subscription target;
            synchronized (this) {
                if (cancelled) return;
                cancelled = true;
                target = upstream;
            }
            if (target != null) target.cancel();
        }

        private static long add(long current, long count) {
            return Long.MAX_VALUE - current < count ? Long.MAX_VALUE : current + count;
        }
    }

}
