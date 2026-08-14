package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 串行转发标量与 LOB 行；每次最多请求一个驱动行，LOB 完成前不取下一行。
 *
 * @author wangr
 * @date 2026-08-14
 * @version v1.0
 */
final class R2dbcDemandAwareRows {
    private R2dbcDemandAwareRows() {
    }
    /**
     * 将已经完成同步行读取的值流转换为严格 demand 驱动的 DynamicRow 流。
     * @param source 每项为 DynamicRow 或仅发出一个 DynamicRow 的异步 Publisher
     * @return 不预取下一驱动行的结果流
     */
    static Publisher<DynamicRow> map(Publisher<?> source) {
        Publisher<?> safeSource = Objects.requireNonNull(source, "row source must not be null");
        return subscriber -> Flux.from(safeSource).subscribe(new RowSubscriber(subscriber));
    }

    /** 单行在途的订阅状态；request 循环避免同步 Publisher 递归请求导致栈增长。 */
    private static final class RowSubscriber implements CoreSubscriber<Object>, Subscription {
        private final Subscriber<? super DynamicRow> downstream;
        private final AtomicInteger requestWip = new AtomicInteger();
        private Subscription upstream;
        private Subscription inner;
        private long requested;
        private boolean upstreamRequested;
        private boolean rowActive;
        private boolean upstreamDone;
        private boolean cancelled;
        private boolean terminated;
        private boolean downstreamSignalling;
        private Throwable pendingFailure;

        private RowSubscriber(Subscriber<? super DynamicRow> downstream) {
            this.downstream = Objects.requireNonNull(downstream, "row subscriber must not be null");
        }

        @Override
        public Context currentContext() {
            return downstream instanceof CoreSubscriber<?> core ? core.currentContext() : Context.empty();
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            Objects.requireNonNull(subscription, "row subscription must not be null");
            synchronized (this) {
                if (upstream != null || cancelled) {
                    subscription.cancel();
                    return;
                }
                upstream = subscription;
            }
            downstream.onSubscribe(this);
            requestUpstreamIfNeeded();
        }

        @Override
        public void request(long count) {
            if (count <= 0L) {
                fail(new IllegalArgumentException("request amount must be greater than zero"));
                return;
            }
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                requested = addCap(requested, count);
            }
            requestUpstreamIfNeeded();
        }

        @Override
        public void cancel() {
            Subscription safeUpstream;
            Subscription safeInner;
            synchronized (this) {
                if (cancelled) {
                    return;
                }
                cancelled = true;
                safeUpstream = upstream;
                safeInner = inner;
            }
            if (safeInner != null) {
                safeInner.cancel();
            }
            if (safeUpstream != null) {
                safeUpstream.cancel();
            }
        }

        @Override
        public void onNext(Object value) {
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                upstreamRequested = false;
                rowActive = true;
            }
            if (value instanceof DynamicRow direct) {
                emit(direct);
                rowComplete();
                return;
            }
            if (!(value instanceof Publisher<?> publisher)) {
                fail(new IllegalStateException("row mapper returned an unsupported value"));
                return;
            }
            @SuppressWarnings("unchecked")
            Publisher<? extends DynamicRow> rows = (Publisher<? extends DynamicRow>) publisher;
            Flux.from(rows).subscribe(new InnerSubscriber());
        }

        @Override
        public void onError(Throwable failure) {
            fail(failure);
        }

        @Override
        public void onComplete() {
            boolean signal;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                upstreamDone = true;
                signal = !rowActive;
                if (signal) {
                    terminated = true;
                }
            }
            if (signal) {
                downstream.onComplete();
            }
        }

        private void emit(DynamicRow row) {
            Throwable delayedFailure;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                if (requested != Long.MAX_VALUE) {
                    requested--;
                }
                downstreamSignalling = true;
            }
            try {
                downstream.onNext(row);
            } finally {
                synchronized (this) {
                    downstreamSignalling = false;
                    delayedFailure = pendingFailure;
                    pendingFailure = null;
                }
                if (delayedFailure != null) {
                    downstream.onError(delayedFailure);
                }
            }
        }

        private void rowComplete() {
            boolean complete;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                inner = null;
                rowActive = false;
                complete = upstreamDone;
                if (upstreamDone) {
                    terminated = true;
                }
            }
            if (complete) {
                downstream.onComplete();
            } else {
                requestUpstreamIfNeeded();
            }
        }

        private void fail(Throwable failure) {
            Throwable safeFailure = Objects.requireNonNull(failure, "row failure must not be null");
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(safeFailure);
            Throwable signalFailure = fatal == null ? safeFailure : fatal;
            Subscription safeUpstream;
            Subscription safeInner;
            boolean delaySignal;
            synchronized (this) {
                if (cancelled || terminated) {
                    return;
                }
                terminated = true;
                safeUpstream = upstream;
                safeInner = inner;
                delaySignal = downstreamSignalling;
                if (delaySignal) {
                    pendingFailure = signalFailure;
                }
            }
            if (safeInner != null) {
                safeInner.cancel();
            }
            if (safeUpstream != null) {
                safeUpstream.cancel();
            }
            if (!delaySignal) {
                downstream.onError(signalFailure);
            }
        }

        private void requestUpstreamIfNeeded() {
            if (requestWip.getAndIncrement() != 0) {
                return;
            }
            int missed = 1;
            do {
                Subscription safeUpstream = null;
                synchronized (this) {
                    if (!cancelled && !terminated && !upstreamDone && !upstreamRequested
                            && !rowActive && requested > 0L && upstream != null) {
                        upstreamRequested = true;
                        safeUpstream = upstream;
                    }
                }
                if (safeUpstream != null) {
                    try {
                        safeUpstream.request(1L);
                    } catch (Throwable failure) {
                        fail(failure);
                    }
                }
                missed = requestWip.addAndGet(-missed);
            } while (missed != 0);
        }

        private static long addCap(long current, long increment) {
            long total = current + increment;
            return total < 0L ? Long.MAX_VALUE : total;
        }
        /** 仅异步 LOB 行创建，普通标量热路径不会进入这里。 */
        private final class InnerSubscriber implements CoreSubscriber<DynamicRow> {
            private boolean emitted;

            @Override
            public Context currentContext() {
                return RowSubscriber.this.currentContext();
            }

            @Override
            public void onSubscribe(Subscription subscription) {
                synchronized (RowSubscriber.this) {
                    if (cancelled || terminated) {
                        subscription.cancel();
                        return;
                    }
                    inner = subscription;
                }
                subscription.request(1L);
            }

            @Override
            public void onNext(DynamicRow row) {
                if (emitted) {
                    fail(new IllegalStateException("row mapper emitted more than one value"));
                    return;
                }
                emitted = true;
                emit(row);
            }

            @Override
            public void onError(Throwable failure) {
                fail(failure);
            }

            @Override
            public void onComplete() {
                rowComplete();
            }
        }
    }
}
