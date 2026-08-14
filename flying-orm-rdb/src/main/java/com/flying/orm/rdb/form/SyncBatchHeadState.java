package com.flying.orm.rdb.form;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 协调同步批量首行桥接的下游订阅、需求计数及单终止信号。
 *
 * <p>线程安全：所有可变状态均由同一把锁保护；驱动下游回调和取消上游均在锁外执行，
 * 防止调用方重入时死锁。首行最多缓存一条，后续行必须有下游需求才会转发。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class SyncBatchHeadState<T> {

    final ReentrantLock lock = new ReentrantLock();

    final Condition changed = lock.newCondition();

    Subscription upstream;

    Subscriber<? super T> downstream;

    boolean downstreamSubscribed;

    T first;

    Throwable failure;

    boolean firstReceived;

    boolean firstDelivered;

    boolean completed;

    boolean terminalDelivered;

    boolean closed;

    long demand;

    boolean isEmpty() {
        lock.lock();
        try {
            return !firstReceived;
        } finally {
            lock.unlock();
        }
    }

    T first() {
        lock.lock();
        try {
            if (!firstReceived) {
                throw new IllegalStateException("empty batch source has no first row");
            }
            return first;
        } finally {
            lock.unlock();
        }
    }

    void subscribe(Subscriber<? super T> subscriber) {
        Subscriber<? super T> safeSubscriber = Objects.requireNonNull(
                subscriber, "batch subscriber must not be null");
        boolean rejected;
        lock.lock();
        try {
            rejected = downstream != null;
            if (!rejected) {
                downstream = safeSubscriber;
            }
        } finally {
            lock.unlock();
        }
        if (rejected) {
            safeSubscriber.onSubscribe(EmptySubscription.INSTANCE);
            safeSubscriber.onError(new IllegalStateException("batch source supports only one execution"));
            return;
        }
        safeSubscriber.onSubscribe(new SyncBatchHeadSubscription<>(this));
        signalTerminalAfterSubscribe(safeSubscriber);
    }

    void onSubscribe(Subscription subscription) {
        Subscription safeSubscription = Objects.requireNonNull(subscription, "batch subscription must not be null");
        boolean cancel;
        lock.lock();
        try {
            cancel = upstream != null || closed;
            if (!cancel) {
                upstream = safeSubscription;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (cancel) {
            safeSubscription.cancel();
            return;
        }
        safeSubscription.request(1);
    }

    void onNext(T value) {
        Subscriber<? super T> target = null;
        Throwable protocolFailure = null;
        Subscription cancel = null;
        lock.lock();
        try {
            if (closed || completed || terminalDelivered) {
                return;
            }
            if (!firstReceived) {
                first = Objects.requireNonNull(value, "batch row must not be null");
                firstReceived = true;
                changed.signalAll();
                return;
            }
            if (downstream == null || demand == 0L) {
                failure = new IllegalStateException("batch publisher emitted a row without downstream demand");
                completed = true;
                cancel = upstream;
                if (firstDelivered && downstream != null) {
                    target = downstream;
                    protocolFailure = failure;
                    terminalDelivered = true;
                }
            } else {
                demand--;
                target = downstream;
            }
        } finally {
            lock.unlock();
        }
        if (cancel != null) {
            cancel.cancel();
        }
        if (protocolFailure != null) {
            target.onError(protocolFailure);
        } else if (target != null) {
            target.onNext(value);
        }
    }

    void onError(Throwable error) {
        Subscriber<? super T> target;
        lock.lock();
        try {
            if (closed || terminalDelivered) {
                return;
            }
            failure = Objects.requireNonNull(error, "batch publisher error must not be null");
            completed = true;
            target = downstreamSubscribed && (!firstReceived || firstDelivered) ? downstream : null;
            terminalDelivered = target != null;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (target != null) {
            target.onError(error);
        }
    }

    void onComplete() {
        Subscriber<? super T> target;
        lock.lock();
        try {
            if (closed || terminalDelivered) {
                return;
            }
            completed = true;
            target = downstreamSubscribed && (!firstReceived || firstDelivered) ? downstream : null;
            terminalDelivered = target != null;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (target != null) {
            target.onComplete();
        }
    }

    void close() {
        Subscription cancel;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            cancel = upstream;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (cancel != null) {
            cancel.cancel();
        }
    }

    void awaitFirst(Duration timeout) throws InterruptedException, TimeoutException {
        long remaining = timeout.isZero() ? Long.MAX_VALUE : saturatingNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (!firstReceived && !completed && failure == null && !closed) {
                if (remaining == Long.MAX_VALUE) {
                    changed.await();
                } else if ((remaining = changed.awaitNanos(remaining)) <= 0L) {
                    throw new TimeoutException("batch input timed out before the first row");
                }
            }
            if (failure != null) {
                throw rethrow(failure);
            }
        } finally {
            lock.unlock();
        }
    }

    private void signalTerminalAfterSubscribe(Subscriber<? super T> subscriber) {
        Throwable terminalFailure = null;
        boolean terminalComplete = false;
        lock.lock();
        try {
            downstreamSubscribed = true;
            if (!terminalDelivered && completed && (!firstReceived || firstDelivered)) {
                terminalDelivered = true;
                terminalFailure = failure;
                terminalComplete = failure == null;
            }
        } finally {
            lock.unlock();
        }
        if (terminalFailure != null) {
            subscriber.onError(terminalFailure);
        } else if (terminalComplete) {
            subscriber.onComplete();
        }
    }

    private static long saturatingNanos(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static RuntimeException rethrow(Throwable error) {
        if (error instanceof Error fatal) {
            throw fatal;
        }
        return error instanceof RuntimeException runtime
                ? runtime : new IllegalStateException("batch publisher failed", error);
    }

    private enum EmptySubscription implements Subscription {
        INSTANCE;

        @Override public void request(long count) { }

        @Override public void cancel() { }
    }

}
