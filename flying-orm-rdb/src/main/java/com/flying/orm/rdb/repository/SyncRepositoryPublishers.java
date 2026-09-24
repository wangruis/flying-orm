package com.flying.orm.rdb.repository;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** 同步集合批量入口使用的最小 Publisher，按下游需求逐项读取，不借用 Reactor。 */
final class SyncRepositoryPublishers {

    private SyncRepositoryPublishers() {
    }

    static <T> Publisher<T> fromIterable(Iterable<T> rows) {
        Iterable<T> safeRows = Objects.requireNonNull(rows, "repository batch rows must not be null");
        return subscriber -> subscriber.onSubscribe(new IterableSubscription<>(subscriber, safeRows));
    }

    /** 只把原下游已请求的额度交给异步 PRE 链；串行组合器的补取不能扩大该额度。 */
    static final class Demand implements Subscription {
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

    private static final class IterableSubscription<T> implements Subscription {
        private final Subscriber<? super T> downstream;
        private final Iterable<T> source;
        private Iterator<T> rows;
        private boolean cancelled;
        private boolean completed;
        private boolean draining;
        private long requested;

        private IterableSubscription(Subscriber<? super T> downstream, Iterable<T> source) {
            this.downstream = Objects.requireNonNull(downstream, "repository batch subscriber must not be null");
            this.source = Objects.requireNonNull(source, "repository batch rows must not be null");
        }

        @Override
        public synchronized void request(long count) {
            if (cancelled || completed) {
                return;
            }
            if (count <= 0L) {
                cancelled = true;
                downstream.onError(new IllegalArgumentException("batch demand must be positive"));
                return;
            }
            requested = Long.MAX_VALUE - requested < count ? Long.MAX_VALUE : requested + count;
            if (draining) {
                return;
            }
            draining = true;
            try {
                while (!cancelled && requested > 0L) {
                    Iterator<T> currentRows = rows();
                    if (!currentRows.hasNext()) {
                        completed = true;
                        downstream.onComplete();
                        return;
                    }
                    requested--;
                    downstream.onNext(Objects.requireNonNull(currentRows.next(), "repository batch row must not be null"));
                }
            } catch (Error fatal) {
                cancelled = true;
                throw fatal;
            } catch (RuntimeException error) {
                cancelled = true;
                downstream.onError(error);
            } finally {
                draining = false;
            }
        }

        @Override
        public synchronized void cancel() {
            cancelled = true;
        }

        private Iterator<T> rows() {
            if (rows == null) {
                rows = Objects.requireNonNull(source.iterator(), "repository batch iterator must not be null");
            }
            return rows;
        }
    }
}
