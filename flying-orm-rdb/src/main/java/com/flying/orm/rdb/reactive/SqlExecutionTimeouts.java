package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 给多行结果流加绝对截止时间。普通 Flux timeout 会在每行到达后重新计时，只能发现“多久没有下一行”；
 * 这里的计时器从订阅开始只启动一次，限制的是拿到并消费完整结果的总时长。
 *
 * <p>截止时间到达时 takeUntilOther 会取消上游，连接由外层 usingWhen 释放。驱动是否能把取消继续传给数据库
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
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
        return Flux.defer(() -> {
            Sinks.One<Long> deadline = Sinks.one();
            Disposable deadlineTask = Schedulers.parallel().schedule(
                    () -> deadline.tryEmitValue(0L),
                    safeTimeout.toNanos(),
                    TimeUnit.NANOSECONDS);
            return total(safeSource, safeTimeout, deadline::asMono)
                    .doFinally(ignored -> deadlineTask.dispose());
        });
    }

    /** 测试协作入口允许观察截止 Publisher 的取消；生产调用仍只接受 Duration。 */
    static <T> Flux<T> total(Flux<T> source,
                             Duration timeout,
                             Supplier<? extends Publisher<?>> deadlineFactory) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql result source must not be null");
        Duration safeTimeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
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
            return safeSource.timeout(deadline, ignored -> deadline)
                             .onErrorMap(TimeoutException.class,
                                         error -> new SqlExecutionTimeoutException(safeTimeout, error));
        });
    }
}
