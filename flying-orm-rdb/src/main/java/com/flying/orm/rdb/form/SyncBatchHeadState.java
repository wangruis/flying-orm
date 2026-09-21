package com.flying.orm.rdb.form;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;
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
final class SyncBatchHeadState<T> implements Subscription {

    private final ReentrantLock lock = new ReentrantLock();

    private final Condition changed = lock.newCondition();

    private Subscription upstream;

    private Subscriber<? super T> downstream;

    private boolean downstreamSubscribed;

    private T first;

    private Throwable failure;

    private boolean firstReceived;

    private boolean firstEmitting;

    private boolean firstDelivered;

    private boolean completed;

    private boolean terminalDelivered;

    private boolean closed;

    private long demand;

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
        safeSubscriber.onSubscribe(this);
        signalTerminalAfterSubscribe(safeSubscriber);
    }

    @Override
    public void request(long count) {
        if (count <= 0L) {
            signalInvalidDemand();
            return;
        }
        T firstValue = null;
        Subscriber<? super T> target;
        Subscription requestUpstream = null;
        long upstreamDemand = count;
        Throwable terminalFailure;
        boolean terminalComplete;
        lock.lock();
        try {
            if (closed || terminalDelivered) {
                return;
            }
            target = downstream;
            long remaining = count;
            if (!firstDelivered && !firstEmitting && firstReceived) {
                firstEmitting = true;
                firstValue = first;
                remaining--;
            }
            terminalFailure = firstDelivered && completed ? failure : null;
            terminalComplete = firstDelivered && completed && failure == null;
            if (terminalFailure != null || terminalComplete) {
                terminalDelivered = true;
            }
            if (!completed && remaining > 0L) {
                demand = saturatedAdd(demand, remaining);
                if (!firstEmitting) {
                    requestUpstream = upstream;
                }
            }
        } finally {
            lock.unlock();
        }
        if (firstValue != null) {
            target.onNext(firstValue);
            lock.lock();
            try {
                firstEmitting = false;
                firstDelivered = true;
                // onNext 允许同步取消；取消后不能继续发送已缓存的终止信号或请求上游。
                if (closed || terminalDelivered) {
                    return;
                }
                // 回调返回前终态只登记；由首行回放者在这里认领唯一一次终态发送。
                terminalFailure = completed ? failure : null;
                terminalComplete = completed && failure == null;
                if (completed) {
                    terminalDelivered = true;
                } else {
                    // 首行在途期间的重入或并发 request 只累计需求，不能越过首行请求后续行。
                    requestUpstream = upstream;
                    upstreamDemand = demand;
                }
            } finally {
                lock.unlock();
            }
        }
        if (terminalFailure != null) {
            target.onError(terminalFailure);
        } else if (terminalComplete) {
            target.onComplete();
        } else if (requestUpstream != null && upstreamDemand > 0L) {
            requestUpstream.request(upstreamDemand);
        }
    }

    @Override
    public void cancel() {
        close();
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
        T safeValue = Objects.requireNonNull(value, "batch row must not be null");
        Subscriber<? super T> target = null;
        Throwable protocolFailure = null;
        Subscription cancel = null;
        lock.lock();
        try {
            if (closed || completed || terminalDelivered) {
                return;
            }
            if (!firstReceived) {
                first = safeValue;
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
            target.onNext(safeValue);
        }
    }

    void onError(Throwable error) {
        Throwable safeError = Objects.requireNonNull(error, "batch publisher error must not be null");
        Subscriber<? super T> target;
        lock.lock();
        try {
            if (closed || terminalDelivered) {
                return;
            }
            failure = safeError;
            completed = true;
            target = downstreamSubscribed && (!firstReceived || firstDelivered) ? downstream : null;
            terminalDelivered = target != null;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (target != null) {
            target.onError(safeError);
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

    void awaitFirst() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (!firstReceived && !completed && failure == null && !closed) {
                changed.await();
            }
            if (failure != null) {
                throw rethrow(failure);
            }
        } finally {
            lock.unlock();
        }
    }

    private void signalInvalidDemand() {
        IllegalArgumentException error = new IllegalArgumentException("batch demand must be positive");
        Subscriber<? super T> target;
        Subscription cancel;
        lock.lock();
        try {
            if (closed || terminalDelivered) {
                return;
            }
            failure = error;
            completed = true;
            terminalDelivered = true;
            closed = true;
            target = downstream;
            cancel = upstream;
        } finally {
            lock.unlock();
        }
        if (cancel != null) {
            cancel.cancel();
        }
        target.onError(error);
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    private void signalTerminalAfterSubscribe(Subscriber<? super T> subscriber) {
        Throwable terminalFailure = null;
        boolean terminalComplete = false;
        lock.lock();
        try {
            // onSubscribe 可以同步取消。取消已经终止此桥接，不能再补发之前缓存的终止信号。
            if (closed) {
                return;
            }
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
