package com.flying.orm.rdb.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.context.Context;

import java.time.Duration;
import java.util.Objects;

/**
 * 下游取消查询后继续排空当前 R2DBC 结果，使驱动已接收的引用计数行在连接归还前完成释放。
 *
 * <p>正常消费时只透传 demand 和信号，不缓存行；取消后停止向下游发信号并请求剩余行，排空完成信号由连接级
 * 清理域等待。超过清理时限时由清理域取消上游并隔离连接。</p>
 *
 * @author wangr
 * @date 2026-08-14
 * @version v1.0
 */
final class R2dbcCancellationDrain {

    private R2dbcCancellationDrain() {
    }

    static <T> Publisher<T> drain(Publisher<T> source,
                                  R2dbcLargeObjectScope cleanupScope,
                                  Duration cleanupTimeout) {
        Publisher<T> safeSource = Objects.requireNonNull(source, "R2DBC result source must not be null");
        R2dbcLargeObjectScope safeScope = Objects.requireNonNull(
                cleanupScope, "large object cleanup scope must not be null");
        Duration safeTimeout = Objects.requireNonNull(
                cleanupTimeout, "resource cleanup timeout must not be null");
        return Flux.from(subscriber -> safeSource.subscribe(new DrainSubscriber<>(
                subscriber, safeScope, safeTimeout)));
    }

    private static final class DrainSubscriber<T> implements CoreSubscriber<T>, Subscription {

        private final Subscriber<? super T> downstream;
        private final R2dbcLargeObjectScope cleanupScope;
        private final Duration cleanupTimeout;
        private final Sinks.Empty<Void> drained = Sinks.empty();
        private Subscription upstream;
        private volatile boolean cancelled;
        private volatile boolean terminated;

        private DrainSubscriber(Subscriber<? super T> downstream,
                                R2dbcLargeObjectScope cleanupScope,
                                Duration cleanupTimeout) {
            this.downstream = Objects.requireNonNull(downstream, "R2DBC result subscriber must not be null");
            this.cleanupScope = cleanupScope;
            this.cleanupTimeout = cleanupTimeout;
        }

        @Override
        public Context currentContext() {
            return downstream instanceof CoreSubscriber<?> core ? core.currentContext() : Context.empty();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            if (upstream != null) {
                subscription.cancel();
                return;
            }
            upstream = Objects.requireNonNull(subscription, "R2DBC result subscription must not be null");
            cleanupScope.registerDrain(drained.asMono(), this::abort, cleanupTimeout);
            downstream.onSubscribe(this);
        }

        @Override
        public void request(long demand) {
            upstream.request(demand);
        }

        @Override
        public void cancel() {
            if (cancelled || terminated) {
                return;
            }
            cancelled = true;
            if (cleanupScope.hasActiveRows()) {
                terminated = true;
                upstream.cancel();
                drained.tryEmitError(new IllegalStateException(
                        "R2DBC result drain aborted because a large object row is active"));
                return;
            }
            try {
                upstream.request(Long.MAX_VALUE);
            } catch (Throwable failure) {
                terminated = true;
                upstream.cancel();
                drained.tryEmitError(failure);
            }
        }

        @Override
        public void onNext(T value) {
            if (!cancelled && !terminated) {
                downstream.onNext(value);
            }
        }

        @Override
        public void onError(Throwable failure) {
            if (terminated) {
                return;
            }
            terminated = true;
            if (cancelled) {
                drained.tryEmitError(failure);
                return;
            }
            drained.tryEmitEmpty();
            downstream.onError(failure);
        }

        @Override
        public void onComplete() {
            if (terminated) {
                return;
            }
            terminated = true;
            drained.tryEmitEmpty();
            if (!cancelled) {
                downstream.onComplete();
            }
        }

        private void abort() {
            Subscription current = upstream;
            if (current != null && !terminated) {
                current.cancel();
            }
        }
    }
}
