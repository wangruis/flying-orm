package com.flying.orm.rdb.internal.batch;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.Objects;

/**
 * 同步批量逐行转换，使用 primitive long 序号，避免每行装箱。集合发布直接复用 Reactor。
 *
 * @author wangr
 * @version v4.1.2
 */
public final class BatchPublishers {

    private BatchPublishers() {
    }

    public static <T, R> Publisher<R> mapIndexed(Publisher<T> source, IndexedMapper<T, R> mapper) {
        Publisher<T> safeSource = Objects.requireNonNull(source, "batch source must not be null");
        IndexedMapper<T, R> safeMapper = Objects.requireNonNull(mapper, "batch mapper must not be null");
        return subscriber -> safeSource.subscribe(new MappingSubscriber<>(subscriber, safeMapper));
    }

    @FunctionalInterface
    public interface IndexedMapper<T, R> {
        R map(T value, long index);
    }

    private static final class MappingSubscriber<T, R> implements Subscriber<T> {
        private final Subscriber<? super R> downstream;
        private final IndexedMapper<T, R> mapper;
        private Subscription upstream;
        private long index;
        private boolean done;

        private MappingSubscriber(Subscriber<? super R> downstream, IndexedMapper<T, R> mapper) {
            this.downstream = Objects.requireNonNull(downstream, "batch subscriber must not be null");
            this.mapper = mapper;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            Subscription candidate = Objects.requireNonNull(
                    subscription, "batch subscription must not be null");
            if (upstream != null || done) {
                // Subscriber 只能接受一个上游订阅；协议违规的后续订阅不能替换真实取消目标。
                candidate.cancel();
                return;
            }
            upstream = candidate;
            downstream.onSubscribe(candidate);
        }

        @Override
        public void onNext(T value) {
            T safeValue = Objects.requireNonNull(value, "batch source value must not be null");
            if (done) {
                return;
            }
            try {
                downstream.onNext(Objects.requireNonNull(
                        mapper.map(safeValue, index++), "mapped batch row must not be null"));
            } catch (Error fatal) {
                done = true;
                cancelAfterFailure(upstream, fatal);
                throw fatal;
            } catch (RuntimeException error) {
                done = true;
                cancelAfterFailure(upstream, error);
                downstream.onError(error);
            }
        }

        @Override
        public void onError(Throwable error) {
            Throwable safeError = Objects.requireNonNull(error, "batch publisher error must not be null");
            if (!done) {
                done = true;
                downstream.onError(safeError);
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

    /**
     * 取消已建立的上游时保留失败优先级：primary VME 不被 cleanup 覆盖；普通 primary 遇 cleanup VME 时提升
     * cleanup。普通 Runtime 清理失败仅作为主错误的诊断上下文，保持既有 onError 语义。
     */
    private static void cancelAfterFailure(Subscription upstream, Throwable primary) {
        VirtualMachineError primaryFatal = findVirtualMachineError(primary);
        try {
            upstream.cancel();
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
            if (cleanup instanceof Error fatal) {
                addSuppressedIfAcyclic(fatal, primary);
                throw fatal;
            }
            addSuppressedIfAcyclic(primary, cleanup);
        }
        if (primaryFatal != null) {
            throw primaryFatal;
        }
    }
}
