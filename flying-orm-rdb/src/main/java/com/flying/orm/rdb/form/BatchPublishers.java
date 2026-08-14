package com.flying.orm.rdb.form;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

/** 不借助 Reactor 对批量行做逐行、带原始下标的转换。 */
final class BatchPublishers {

    private BatchPublishers() {
    }

    static <T, R> Publisher<R> mapIndexed(Publisher<T> source, IndexedMapper<T, R> mapper) {
        Publisher<T> safeSource = Objects.requireNonNull(source, "batch source must not be null");
        IndexedMapper<T, R> safeMapper = Objects.requireNonNull(mapper, "batch mapper must not be null");
        return subscriber -> safeSource.subscribe(new MappingSubscriber<>(subscriber, safeMapper));
    }

    /** 把同步集合按下游需求逐行发布，List 入口不再为了批量执行创建 Reactor Flux。 */
    static <T> Publisher<T> fromIterable(Iterable<T> source) {
        Iterable<T> safeSource = Objects.requireNonNull(source, "batch iterable must not be null");
        return subscriber -> {
            Subscriber<? super T> safeSubscriber = Objects.requireNonNull(
                    subscriber, "batch subscriber must not be null");
            safeSubscriber.onSubscribe(new IterableSubscription<>(safeSubscriber, safeSource));
        };
    }

    @FunctionalInterface
    interface IndexedMapper<T, R> {
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
            if (done) {
                return;
            }
            try {
                downstream.onNext(Objects.requireNonNull(
                        mapper.map(value, index++), "mapped batch row must not be null"));
            } catch (VirtualMachineError fatal) {
                // VME 不能降级成 Reactive Streams 的普通 onError；先取消上游，避免失败后继续产行。
                done = true;
                cancelAfterFailure(upstream, fatal);
                throw fatal;
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
            if (!done) {
                done = true;
                downstream.onError(error);
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

    /** 取消失败的异常图可能来自同一驱动链；按身份遍历后才追加 suppressed，避免反向环。 */
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

    /** mapper 与取消回调都属于可扩展边界，普通包装异常中的 VME 也不能被降级为下游业务错误。 */
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

    private static final class IterableSubscription<T> implements Subscription {
        private final Subscriber<? super T> downstream;
        private final Iterable<T> source;
        private java.util.Iterator<T> iterator;
        private boolean draining;
        private boolean cancelled;
        private boolean completed;
        private long requested;

        private IterableSubscription(Subscriber<? super T> downstream, Iterable<T> source) {
            this.downstream = downstream;
            this.source = Objects.requireNonNull(source, "batch iterable must not be null");
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
                    java.util.Iterator<T> current = iterator();
                    if (!current.hasNext()) {
                        completed = true;
                        downstream.onComplete();
                        return;
                    }
                    requested--;
                    downstream.onNext(Objects.requireNonNull(current.next(), "batch row must not be null"));
                }
            } catch (VirtualMachineError fatal) {
                // Iterator 或下游触发 VME 后订阅已不可继续；标记取消再保留 JVM 原始终止语义。
                cancelled = true;
                throw fatal;
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

        private java.util.Iterator<T> iterator() {
            if (iterator == null) {
                iterator = Objects.requireNonNull(source.iterator(), "batch iterator must not be null");
            }
            return iterator;
        }
    }
}
