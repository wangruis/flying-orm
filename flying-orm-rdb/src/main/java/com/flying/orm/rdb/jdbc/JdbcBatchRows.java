package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.internal.DurationLimits;
import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 把 {@link Publisher} 的批量输入变成同步 JDBC 写入器可以逐行领取的有界通道。
 *
 * <p>这里故意只允许一条尚未领取的行：写入器执行完当前行或当前分片后，才会再向上游
 * request(1)。这样无论上游是内存集合、消息流还是异步发布者，都不会因为 JDBC 执行较慢而把
 * 整批参数堆到内存里。onNext 只放入一个槽位并唤醒等待线程，不做 SQL、不阻塞等待数据库。</p>
 */
final class JdbcBatchRows implements Subscriber<Object[]>, AutoCloseable {

    private final Publisher<Object[]> publisher;
    private final int parameterCount;
    private final long maxRowBytes;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    private Subscription subscription;
    private ProtectedBatchRows.RowView nextRow;
    private Throwable failure;
    private boolean started;
    private boolean requested;
    private boolean completed;
    private boolean closed;

    JdbcBatchRows(Publisher<Object[]> publisher, int parameterCount, long maxRowBytes) {
        this.publisher = Objects.requireNonNull(publisher, "batch row publisher must not be null");
        if (parameterCount < 0 || maxRowBytes <= 0L) {
            throw new IllegalArgumentException("invalid jdbc batch row ownership limits");
        }
        this.parameterCount = parameterCount;
        this.maxRowBytes = maxRowBytes;
    }

    /**
     * 领取下一行；返回 {@code null} 代表上游正常结束。ATOMIC 可传入整批剩余时间，把持有事务连接时的
     * 输入等待纳入事务时限；INDEPENDENT 在形成分片时传 0，避免 ORM 重复治理上游生产和连接池排队。
     */
    /** Returns the decoded owned row and estimate captured at the subscriber boundary. */
    ProtectedBatchRows.RowView nextRowView(Duration remaining)
            throws InterruptedException, TimeoutException {
        startIfNeeded();
        long remainingNanos = remaining.isZero() ? Long.MAX_VALUE : DurationLimits.nanos(remaining);
        while (true) {
            Subscription demand = null;
            lock.lockInterruptibly();
            try {
                if (nextRow != null) {
                    ProtectedBatchRows.RowView row = nextRow;
                    nextRow = null;
                    return row;
                }
                if (failure != null) {
                    throw rethrow(failure);
                }
                if (completed || closed) {
                    return null;
                }
                if (!requested && subscription != null) {
                    requested = true;
                    demand = subscription;
                } else {
                    remainingNanos = awaitChange(remainingNanos);
                }
            } finally {
                lock.unlock();
            }
            if (demand != null) {
                // Reactive Streams 允许 request 同步触发 onNext，因此必须在锁外请求。
                demand.request(1);
            }
        }
    }

    @Override
    public void onSubscribe(Subscription candidate) {
        Subscription safeSubscription = Objects.requireNonNull(candidate, "batch subscription must not be null");
        boolean cancel;
        lock.lock();
        try {
            cancel = subscription != null || completed || closed;
            if (!cancel) {
                subscription = safeSubscription;
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
        if (cancel) {
            safeSubscription.cancel();
        }
    }

    @Override
    public void onNext(Object[] row) {
        Object[] safeRow = Objects.requireNonNull(row, "batch row must not be null");
        Subscription cancel = null;
        Throwable terminalFailure = null;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            if (!requested || nextRow != null) {
                failure = new IllegalStateException("batch publisher violated one-row backpressure contract");
                terminalFailure = failure;
                completed = true;
                cancel = subscription;
                subscription = null;
            } else {
                requested = false;
                try {
                    // BatchWriteRequest 约定 onNext 后整行所有权转移；这里只校验形状并记录预算。
                    nextRow = BatchRowSnapshotter.snapshotView(
                            safeRow, parameterCount, maxRowBytes, "row bytes");
                } catch (Throwable error) {
                    failure = error;
                    terminalFailure = error;
                    completed = true;
                    cancel = subscription;
                    subscription = null;
                }
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (cancel != null) {
            cancelAfterFailure(cancel, terminalFailure);
        }
    }

    @Override
    public void onError(Throwable error) {
        Throwable safeError = Objects.requireNonNull(error, "batch publisher error must not be null");
        lock.lock();
        try {
            if (!completed && !closed) {
                failure = safeError;
                completed = true;
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void onComplete() {
        lock.lock();
        try {
            completed = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        Subscription cancel;
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            cancel = subscription;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        if (cancel != null) {
            cancel.cancel();
        }
    }

    private void startIfNeeded() {
        boolean subscribe = false;
        lock.lock();
        try {
            if (!started) {
                started = true;
                subscribe = true;
            }
        } finally {
            lock.unlock();
        }
        if (subscribe) {
            publisher.subscribe(this);
        }
    }

    private long awaitChange(long remainingNanos) throws InterruptedException, TimeoutException {
        if (remainingNanos == Long.MAX_VALUE) {
            changed.await();
            return Long.MAX_VALUE;
        }
        if (remainingNanos <= 0L || (remainingNanos = changed.awaitNanos(remainingNanos)) <= 0L) {
            throw new TimeoutException("jdbc batch input timed out");
        }
        return remainingNanos;
    }

    private static RuntimeException rethrow(Throwable error) {
        // 上游 Error 也是原始失败事实；交给外层事务边界完成 rollback 后再决定如何公开，不能在通道层降级成普通异常。
        if (error instanceof Error fatal) {
            throw fatal;
        }
        if (error instanceof RuntimeException runtime) {
            return runtime;
        }
        return new IllegalStateException("batch publisher failed", error);
    }

    private void cancelAfterFailure(Subscription cancel, Throwable primary) {
        try {
            cancel.cancel();
        } catch (Throwable cleanup) {
            VirtualMachineError primaryFatal = findVirtualMachineError(primary);
            VirtualMachineError cleanupFatal = findVirtualMachineError(cleanup);
            Throwable selected = primaryFatal != null ? primaryFatal : cleanupFatal != null ? cleanupFatal : primary;
            Throwable secondary = selected == primary ? cleanup : primary;
            if (secondary != null && secondary != selected) {
                addSuppressedIfAcyclic(selected, secondary);
            }
            lock.lock();
            try {
                failure = selected;
                completed = true;
                changed.signalAll();
            } finally {
                lock.unlock();
            }
            if (selected instanceof VirtualMachineError fatal) {
                throw fatal;
            }
        }
    }
}
