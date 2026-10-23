package com.flying.orm.rdb.form;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * 同步批量规划的首行预读；同一次订阅的回放、需求与终态由 Reactor 管理。
 * JDBC 仍由调用线程执行，close 负责取消本对象拥有的输入订阅。
 *
 * @author wangr
 * @version v4.1.2
 */
final class SyncBatchHead<T> implements AutoCloseable {
    private final CountDownLatch ready = new CountDownLatch(1);
    private Disposable subscription;
    private Publisher<T> rows;
    private T first;
    private volatile Throwable failure;

    static <T> SyncBatchHead<T> open(Publisher<T> source) throws InterruptedException {
        Objects.requireNonNull(source, "batch source must not be null");
        SyncBatchHead<T> head = new SyncBatchHead<>();
        try {
            // 这里只启动一个可取消的输入订阅；不预取后续行，不在回调内运行 JDBC。
            head.subscription = Flux.from(source).doOnError(head::failed)
                    .<Void>switchOnFirst((signal, replay) -> {
                        head.first = signal.get();
                        head.rows = replay;
                        head.ready.countDown();
                        return Mono.never();
                    }).subscribe(ignored -> { }, head::failed);
            head.ready.await();
            if (head.failure instanceof Error fatal) throw fatal;
            if (head.failure instanceof RuntimeException runtime) throw runtime;
            if (head.failure != null) throw new IllegalStateException("batch publisher failed", head.failure);
            return head;
        } catch (InterruptedException | RuntimeException | Error error) {
            closeAfterFailure(head, error);
            throw error;
        }
    }

    boolean isEmpty() { return first == null; }

    T first() {
        if (isEmpty()) throw new IllegalStateException("empty batch source has no first row");
        return first;
    }

    Publisher<T> rows() { return rows; }

    private void failed(Throwable error) {
        failure = error;
        ready.countDown();
    }

    @Override
    public void close() {
        if (subscription != null) subscription.dispose();
    }

    /** 首行失败仍取消输入，保留原始错误及取消失败。 */
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
        if (primaryFatal != null) throw primaryFatal;
    }
}
