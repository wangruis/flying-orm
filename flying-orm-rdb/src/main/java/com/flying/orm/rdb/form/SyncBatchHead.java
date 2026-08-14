package com.flying.orm.rdb.form;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * 同步批量规划只预读一行，并把同一次上游订阅继续交给 JDBC 批量执行器消费。
 *
 * <p>本类型只负责一次上游订阅及首行探测；下游订阅、需求和终止信号由
 * {@link SyncBatchHeadState} 协调，避免把两套状态机耦合在同一个大类中。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v1.0
 */
final class SyncBatchHead<T> implements Publisher<T>, Subscriber<T>, AutoCloseable {

    private final Publisher<T> source;

    private final SyncBatchHeadState<T> state = new SyncBatchHeadState<>();

    private SyncBatchHead(Publisher<T> source) {
        this.source = Objects.requireNonNull(source, "batch source must not be null");
    }

    static <T> SyncBatchHead<T> open(Publisher<T> source, Duration timeout)
            throws InterruptedException, TimeoutException {
        SyncBatchHead<T> head = new SyncBatchHead<>(source);
        try {
            head.source.subscribe(head);
            head.state.awaitFirst(Objects.requireNonNull(timeout, "batch timeout must not be null"));
            return head;
        } catch (InterruptedException | TimeoutException | RuntimeException | Error error) {
            closeAfterFailure(head, error);
            throw error;
        }
    }

    boolean isEmpty() {
        return state.isEmpty();
    }

    T first() {
        return state.first();
    }

    @Override
    public void subscribe(Subscriber<? super T> subscriber) {
        state.subscribe(subscriber);
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        state.onSubscribe(subscription);
    }

    @Override
    public void onNext(T value) {
        state.onNext(value);
    }

    @Override
    public void onError(Throwable error) {
        state.onError(error);
    }

    @Override
    public void onComplete() {
        state.onComplete();
    }

    @Override
    public void close() {
        state.close();
    }

    /**
     * 首行探测已失败时仍先取消上游；VME 保持最高优先级，且只在不会形成 cause/suppressed 环时保留清理上下文。
     */
    private static void closeAfterFailure(SyncBatchHead<?> head, Throwable primary) {
        VirtualMachineError primaryFatal = findVirtualMachineError(primary);
        try {
            head.close();
        } catch (Throwable cleanup) {
            if (primaryFatal != null) {
                addSuppressedIfAcyclic(primaryFatal, cleanup);
                throw primaryFatal;
            }
            VirtualMachineError cleanupFatal = findVirtualMachineError(cleanup);
            if (cleanupFatal != null) {
                addSuppressedIfAcyclic(cleanupFatal, primary);
                throw cleanupFatal;
            }
            addSuppressedIfAcyclic(primary, cleanup);
        }
        if (primaryFatal != null) {
            throw primaryFatal;
        }
    }

    /** 以对象身份遍历 cause 与 suppressed 图，避免 cleanup 诊断反向连接成 Throwable 环。 */
    private static void addSuppressedIfAcyclic(Throwable primary, Throwable secondary) {
        if (primary == secondary || reaches(primary, secondary) || reaches(secondary, primary)) {
            return;
        }
        primary.addSuppressed(secondary);
    }

    private static boolean reaches(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }

    /** 上游错误和取消清理都可能包装 JVM 致命错误，必须按对象身份恢复原错误。 */
    private static VirtualMachineError findVirtualMachineError(Throwable root) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return null;
    }
}
