package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
import java.util.function.Function;

/**
 * 在不依赖 Reactor 的前提下，为同步批量输入补 PRE 回调、实体映射和可回收的输入偏移。
 *
 * <p>下游的 request 原样交给上游。JDBC 批量写入器每次只领一行，因此这里不会先把 Publisher
 * 收集成 List；上游异步发布时，映射也在它自己的发布线程执行。</p>
 */
final class SyncRepositoryBatchRows<T, R> implements Publisher<R> {

    private final Publisher<T> source;
    private final Function<T, R> mapper;
    private final EntityLifecyclePhase beforePhase;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final SyncRepositoryBatchLifecycle<T> retention;

    SyncRepositoryBatchRows(Publisher<T> source,
                            Function<T, R> mapper,
                            EntityLifecyclePhase beforePhase,
                            SyncRepositoryLifecycleSupport<T> lifecycle,
                            SyncRepositoryBatchLifecycle<T> retention) {
        this.source = Objects.requireNonNull(source, "repository batch source must not be null");
        this.mapper = Objects.requireNonNull(mapper, "repository batch mapper must not be null");
        this.beforePhase = Objects.requireNonNull(beforePhase, "batch before lifecycle phase must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.retention = Objects.requireNonNull(retention, "repository batch lifecycle must not be null");
    }

    @Override
    public void subscribe(Subscriber<? super R> subscriber) {
        source.subscribe(new MappingSubscriber(Objects.requireNonNull(subscriber, "batch subscriber must not be null")));
    }

    private final class MappingSubscriber implements Subscriber<T> {
        private final Subscriber<? super R> downstream;
        private Subscription upstream;
        private long offset;
        private boolean done;

        private MappingSubscriber(Subscriber<? super R> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            Subscription candidate = Objects.requireNonNull(
                    subscription, "batch subscription must not be null");
            if (upstream != null || done) {
                // 保留首个订阅作为唯一请求与取消目标，重复订阅只能立即取消。
                candidate.cancel();
                return;
            }
            upstream = candidate;
            downstream.onSubscribe(candidate);
        }

        @Override
        public void onNext(T entity) {
            if (done) {
                return;
            }
            try {
                T safeEntity = Objects.requireNonNull(entity, "repository batch entity must not be null");
                lifecycle.fire(beforePhase, safeEntity, null);
                R row = Objects.requireNonNull(mapper.apply(safeEntity), "mapped batch row must not be null");
                retention.remember(offset++, safeEntity);
                downstream.onNext(row);
            } catch (RuntimeException error) {
                done = true;
                RuntimeException terminal = RepositoryFailureSupport.propagate(
                        RepositoryFailureSupport.afterCleanup(error, upstream::cancel));
                downstream.onError(terminal);
            } catch (Error error) {
                done = true;
                throw RepositoryFailureSupport.propagate(
                        RepositoryFailureSupport.afterCleanup(error, upstream::cancel));
            }
        }

        @Override
        public void onError(Throwable error) {
            if (!done) {
                done = true;
                downstream.onError(Objects.requireNonNull(error, "batch publisher error must not be null"));
            }
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                downstream.onComplete();
            }
        }
    }
}
