package com.flying.orm.testkit.concurrent;

import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;

/**
 * 用固定并发重复执行一段响应式操作，给真实数据库兼容测试和轻量压力检查提供统一统计口径。
 * 这个类不创建线程池，也不管理连接池；并发和取消都沿用 Reactor 与调用方传入的 Publisher。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public final class ReactiveConcurrencyProbe {

    private ReactiveConcurrencyProbe() {
    }

    /**
     * 执行并发探测。单个操作失败会被计数并继续执行，整体超时才会取消尚未完成的操作。
     *
     * @param plan      请求数量、并发上限和整体超时
     * @param operation 根据请求序号创建响应式操作，不能返回 null
     * @return 汇总结果，不会因为单个操作失败而抛错
     */
    public static Mono<Result> run(Plan plan, IntFunction<? extends Publisher<?>> operation) {
        Plan safePlan = Objects.requireNonNull(plan, "reactive concurrency plan must not be null");
        IntFunction<? extends Publisher<?>> safeOperation = Objects.requireNonNull(
                operation,
                "reactive concurrency operation must not be null");

        return Mono.defer(() -> {
            Counters counters = new Counters(safePlan.operations());
            long startedAt = System.nanoTime();
            Mono<Result> execution = Flux.range(0, safePlan.operations())
                                         .flatMap(index -> invoke(index, safeOperation, counters),
                                                  safePlan.concurrency())
                                         .then(Mono.fromSupplier(() -> counters.result(startedAt, false)));
            if (safePlan.timeout().isZero()) {
                return execution;
            }
            return execution.timeout(safePlan.timeout())
                            .onErrorResume(TimeoutException.class,
                                           ignored -> Mono.just(counters.result(startedAt, true)));
        });
    }

    private static Mono<Void> invoke(int index,
                                     IntFunction<? extends Publisher<?>> operation,
                                     Counters counters) {
        return Mono.defer(() -> {
            counters.started.incrementAndGet();
            int currentInFlight = counters.inFlight.incrementAndGet();
            counters.maxInFlight.accumulateAndGet(currentInFlight, Math::max);

            Publisher<?> publisher;
            try {
                publisher = Objects.requireNonNull(operation.apply(index),
                                                   "reactive concurrency operation returned null");
            } catch (RuntimeException | Error error) {
                rethrowFatal(error);
                publisher = Mono.error(error);
            }

            return Flux.from(publisher)
                       .then()
                       .then(Mono.<Void>fromRunnable(() -> {
                           counters.completed.incrementAndGet();
                           counters.inFlight.decrementAndGet();
                       }))
                       .onErrorResume(error -> {
                           rethrowFatal(error);
                           counters.failed.incrementAndGet();
                           counters.failures.computeIfAbsent(error.getClass().getName(), ignored -> new LongAdder())
                                            .increment();
                           counters.inFlight.decrementAndGet();
                           return Mono.<Void>empty();
                       })
                       .doOnCancel(() -> counters.inFlight.decrementAndGet());
        });
    }

    /** Reactor 致命错误及被驱动包装的 JVM 致命错误都必须原样离开探针。 */
    private static void rethrowFatal(Throwable error) {
        Exceptions.throwIfFatal(error);
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        pending.push(error);
        while (!pending.isEmpty()) {
            Throwable current = pending.pop();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                throw fatal;
            }
            if (current.getCause() != null) {
                pending.push(current.getCause());
            }
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed != null) {
                    pending.push(suppressed);
                }
            }
        }
    }

    /**
     * @param operations 计划执行多少个操作
     * @param concurrency 最多同时运行多少个操作
     * @param timeout     整体最多运行多久，0 表示不限
     */
    public record Plan(int operations, int concurrency, Duration timeout) {

        /**
         * 参数错误要在发起数据库调用前暴露。
         */
        public Plan {
            if (operations <= 0) {
                throw new IllegalArgumentException("reactive concurrency operations must be greater than zero");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("reactive concurrency limit must be greater than zero");
            }
            timeout = Objects.requireNonNull(timeout, "reactive concurrency timeout must not be null");
            if (timeout.isNegative()) {
                throw new IllegalArgumentException("reactive concurrency timeout must not be negative");
            }
        }
    }

    /**
     * @param requested      计划请求数
     * @param started        实际启动数
     * @param completed      成功完成数
     * @param failed         失败数
     * @param cancelled      因整体超时被取消的已启动操作数
     * @param maxInFlight    观测到的峰值并发
     * @param elapsed        实际耗时
     * @param timedOut       是否触发整体超时
     * @param failuresByType 按异常类名汇总的失败数量，不包含参数值或 SQL
     */
    public record Result(int requested,
                         int started,
                         int completed,
                         int failed,
                         int cancelled,
                         int maxInFlight,
                         Duration elapsed,
                         boolean timedOut,
                         Map<String, Long> failuresByType) {

        /**
         * 固化 Map 并检查统计值，避免探针自己产出互相矛盾的数据。
         */
        public Result {
            elapsed = Objects.requireNonNull(elapsed, "reactive concurrency elapsed time must not be null");
            failuresByType = Map.copyOf(Objects.requireNonNull(failuresByType,
                                                               "reactive concurrency failures must not be null"));
            if (requested < 0 || started < 0 || completed < 0 || failed < 0 || cancelled < 0 || maxInFlight < 0) {
                throw new IllegalArgumentException("reactive concurrency counters must not be negative");
            }
            if (started > requested || completed + failed + cancelled > started) {
                throw new IllegalArgumentException("reactive concurrency counters are inconsistent");
            }
        }
    }

    private static final class Counters {

        private final int requested;
        private final AtomicInteger started = new AtomicInteger();
        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger maxInFlight = new AtomicInteger();
        private final Map<String, LongAdder> failures = new ConcurrentHashMap<>();

        private Counters(int requested) {
            this.requested = requested;
        }

        private Result result(long startedAt, boolean timedOut) {
            int startedCount = started.get();
            int completedCount = completed.get();
            int failedCount = failed.get();
            int cancelledCount = Math.max(0, startedCount - completedCount - failedCount);
            Map<String, Long> failureCounts = new TreeMap<>();
            failures.forEach((type, count) -> failureCounts.put(type, count.sum()));
            return new Result(requested,
                              startedCount,
                              completedCount,
                              failedCount,
                              cancelledCount,
                              maxInFlight.get(),
                              Duration.ofNanos(System.nanoTime() - startedAt),
                              timedOut,
                              failureCounts);
        }
    }
}
