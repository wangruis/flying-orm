package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.internal.DurationLimits;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * 给多行结果流加绝对截止时间。普通 Flux timeout 会在每行到达后重新计时，只能发现“多久没有下一行”；
 * 这里的计时器从订阅开始只启动一次，限制的是拿到并消费完整结果的总时长。
 *
 * <p>截止时间到达时 timeout 会取消上游，连接由外层 usingWhen 释放。驱动是否能把取消继续传给数据库
 * 由具体 R2DBC 实现决定，因此超时代表客户端停止等待，不承诺数据库端语句已经立刻终止。</p>
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
final class SqlExecutionTimeouts {

    private SqlExecutionTimeouts() {
    }

    static <T> Flux<T> total(Flux<T> source, Duration timeout) {
        return total(source, timeout, timeout);
    }

    /** 使用剩余预算调度，但在公开异常中保留调用方配置的原始总时限。 */
    static <T> Flux<T> total(Flux<T> source, Duration remaining, Duration configuredTimeout) {
        Duration safeConfigured = Objects.requireNonNull(
                configuredTimeout, "configured sql execution timeout must not be null");
        return total(source,
                     remaining,
                     safeConfigured,
                     () -> new SqlExecutionTimeoutException(
                             safeConfigured,
                             new TimeoutException("R2DBC SQL execution deadline elapsed")));
    }

    /** 允许调用方验证超时失败只在截止点真正获胜时创建。 */
    static <T> Flux<T> total(Flux<T> source,
                             Duration remaining,
                             Duration configuredTimeout,
                             Supplier<? extends Throwable> timeoutFailure) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeRemaining = Objects.requireNonNull(remaining, "sql execution timeout must not be null");
        Objects.requireNonNull(
                configuredTimeout, "configured sql execution timeout must not be null");
        Supplier<? extends Throwable> safeFailure = Objects.requireNonNull(
                timeoutFailure, "sql execution timeout failure must not be null");
        return absolute(safeSource,
                        safeRemaining,
                        (task, delay) -> Schedulers.parallel().schedule(
                                task, DurationLimits.nanos(delay), TimeUnit.NANOSECONDS),
                        Flux.defer(() -> Flux.error(Objects.requireNonNull(
                                safeFailure.get(), "sql execution timeout failure must not be null"))));
    }

    /** 给内部结果流施加不改写异常类型的绝对截止时间。 */
    static <T> Flux<T> absolute(Flux<T> source, Duration timeout) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        return absolute(safeSource,
                        safeTimeout,
                        (task, delay) -> Schedulers.parallel().schedule(
                                task, DurationLimits.nanos(delay), TimeUnit.NANOSECONDS));
    }

    /** 测试协作入口允许直接观察生产截止任务的终态释放。 */
    static <T> Flux<T> absolute(Flux<T> source,
                                Duration timeout,
                                BiFunction<Runnable, Duration, Disposable> scheduler) {
        return absolute(source, timeout, scheduler,
                        Flux.defer(() -> Flux.error(new TimeoutException())));
    }

    private static <T> Flux<T> absolute(Flux<T> source,
                                        Duration timeout,
                                        BiFunction<Runnable, Duration, Disposable> scheduler,
                                        Publisher<T> fallback) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        BiFunction<Runnable, Duration, Disposable> safeScheduler = Objects.requireNonNull(
                scheduler, "sql execution deadline scheduler must not be null");
        return Flux.defer(() -> {
            Sinks.One<Long> deadline = Sinks.one();
            Disposable deadlineTask = Objects.requireNonNull(
                    safeScheduler.apply(() -> deadline.tryEmitValue(0L), safeTimeout),
                    "sql execution deadline task must not be null");
            return absolute(safeSource, deadline::asMono, fallback)
                    .doFinally(ignored -> deadlineTask.dispose());
        });
    }

    private static <T> Flux<T> absolute(Flux<T> source,
                                        Supplier<? extends Publisher<?>> deadlineFactory,
                                        Publisher<T> fallback) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Supplier<? extends Publisher<?>> safeDeadlineFactory = Objects.requireNonNull(
                deadlineFactory, "sql execution deadline factory must not be null");
        return Flux.defer(() -> {
            /*
             * timeout 会在每行后切换截止订阅，因此生产入口使用同一个热 Sinks.One 保存绝对截止点。
             * 正常完成、失败或取消时外层 doFinally 会撤销计时任务；不能使用 cache，否则高 QPS 查询
             * 正常结束后仍会把定时任务保留到到期。超时仲裁与上游取消由 Reactor timeout 统一串行化，
             * 不使用独立状态标志，避免正常完成与截止信号竞争时误判终态。
             */
            Publisher<?> deadline = Objects.requireNonNull(
                    safeDeadlineFactory.get(), "sql execution deadline publisher must not be null");
            return safeSource.timeout(deadline, ignored -> deadline, fallback);
        });
    }

}
