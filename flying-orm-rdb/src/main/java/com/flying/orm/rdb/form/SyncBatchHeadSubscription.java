package com.flying.orm.rdb.form;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * 将同步批量首行状态转换为下游 Reactive Streams 的需求、取消和终止信号。
 *
 * <p>请求在锁内更新状态，在锁外请求或取消上游并通知下游，因此既保留单行缓存，
 * 也不把用户回调放进共享锁。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class SyncBatchHeadSubscription<T> implements Subscription {

    private final SyncBatchHeadState<T> state;

    SyncBatchHeadSubscription(SyncBatchHeadState<T> state) {
        this.state = java.util.Objects.requireNonNull(state, "batch head state must not be null");
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
        Throwable terminalFailure;
        boolean terminalComplete;
        state.lock.lock();
        try {
            if (state.closed || state.terminalDelivered) {
                return;
            }
            target = state.downstream;
            long remaining = count;
            if (!state.firstDelivered && state.firstReceived) {
                state.firstDelivered = true;
                firstValue = state.first;
                remaining--;
            }
            terminalFailure = state.firstDelivered && state.completed ? state.failure : null;
            terminalComplete = state.firstDelivered && state.completed && state.failure == null;
            if (terminalFailure != null || terminalComplete) {
                state.terminalDelivered = true;
            }
            if (!state.completed && remaining > 0L) {
                state.demand = saturatedAdd(state.demand, remaining);
                requestUpstream = state.upstream;
            }
        } finally {
            state.lock.unlock();
        }
        if (firstValue != null) {
            target.onNext(firstValue);
            state.lock.lock();
            try {
                // onNext 允许同步取消；取消后不能继续发送已缓存的终止信号或请求上游。
                if (state.closed) {
                    return;
                }
            } finally {
                state.lock.unlock();
            }
        }
        if (terminalFailure != null) {
            target.onError(terminalFailure);
        } else if (terminalComplete) {
            target.onComplete();
        } else if (requestUpstream != null) {
            requestUpstream.request(count - (firstValue == null ? 0L : 1L));
        }
    }

    @Override
    public void cancel() {
        state.close();
    }

    private void signalInvalidDemand() {
        IllegalArgumentException error = new IllegalArgumentException("batch demand must be positive");
        Subscriber<? super T> target;
        Subscription cancel;
        state.lock.lock();
        try {
            if (state.closed || state.terminalDelivered) {
                return;
            }
            state.failure = error;
            state.completed = true;
            state.terminalDelivered = true;
            state.closed = true;
            target = state.downstream;
            cancel = state.upstream;
        } finally {
            state.lock.unlock();
        }
        if (cancel != null) {
            cancel.cancel();
        }
        target.onError(error);
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
