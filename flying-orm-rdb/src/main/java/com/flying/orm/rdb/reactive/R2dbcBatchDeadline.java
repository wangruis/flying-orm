package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.internal.InternalApi;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 一次批量订阅共用的总截止时间。
 *
 * <p>连接池返回连接后才创建截止点；连接排队完全服从上层连接池。连接内的事务、分片 SQL、回执写入和提交
 * 共用同一份预算，开始下一步不会重新计时。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
@InternalApi
public record R2dbcBatchDeadline(Duration timeout, long startedAtNanos) {

    public R2dbcBatchDeadline {
        timeout = Objects.requireNonNull(timeout, "batch timeout must not be null");
    }

    public static R2dbcBatchDeadline start(Duration timeout) {
        return new R2dbcBatchDeadline(timeout, nowNanos());
    }

    <T> Mono<T> protect(Mono<T> source) {
        return protect(source, () -> new TimeoutException("r2dbc batch timed out before the next operation"));
    }

    /** 允许边界层给同一批量截止点选择稳定的公开异常，同时保留源 Publisher 自己的超时。 */
    <T> Mono<T> protect(Mono<T> source, Supplier<? extends Throwable> timeoutFailure) {
        Mono<T> safeSource = Objects.requireNonNull(source, "batch mono must not be null");
        Supplier<? extends Throwable> safeFailure = Objects.requireNonNull(
                timeoutFailure, "batch timeout failure supplier must not be null");
        if (timeout.isZero()) {
            return safeSource;
        }
        return Mono.defer(() -> {
            Duration remaining = remaining();
            return remaining.isZero()
                    ? Mono.error(Objects.requireNonNull(safeFailure.get(), "batch timeout failure must not be null"))
                    : safeSource.timeout(remaining, Mono.defer(() -> Mono.error(Objects.requireNonNull(
                            safeFailure.get(), "batch timeout failure must not be null"))));
        });
    }

    <T> Flux<T> protect(Flux<T> source) {
        Flux<T> safeSource = Objects.requireNonNull(source, "batch flux must not be null");
        return protectFlux(safeSource, remaining -> SqlExecutionTimeouts.absolute(safeSource, remaining));
    }

    /** 测试协作入口允许观察批量截止 Publisher 的取消；生产调用仍只接受结果流。 */
    <T> Flux<T> protect(Flux<T> source,
                        Function<Duration, ? extends Publisher<?>> deadlineFactory) {
        Flux<T> safeSource = Objects.requireNonNull(source, "batch flux must not be null");
        Function<Duration, ? extends Publisher<?>> safeDeadlineFactory = Objects.requireNonNull(
                deadlineFactory, "batch deadline factory must not be null");
        return protectFlux(safeSource,
                           remaining -> SqlExecutionTimeouts.absolute(
                                   safeSource,
                                   () -> safeDeadlineFactory.apply(remaining)));
    }

    private <T> Flux<T> protectFlux(Flux<T> safeSource,
                                    Function<Duration, Flux<T>> protectionFactory) {
        if (timeout.isZero()) {
            return safeSource;
        }
        return Flux.defer(() -> {
            Duration remaining = remaining();
            if (remaining.isZero()) {
                return Flux.<T>error(new TimeoutException("r2dbc batch timed out before the next operation"));
            }
            return protectionFactory.apply(remaining);
        });
    }

    public Duration remaining() {
        long remainingNanos = saturatingNanos(timeout) - (nowNanos() - startedAtNanos);
        return remainingNanos <= 0L ? Duration.ZERO : Duration.ofNanos(remainingNanos);
    }

    private static long saturatingNanos(Duration value) {
        try {
            return value.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static long nowNanos() {
        // 使用 Reactor 的单调时钟，测试换成虚拟时间后，超时预算仍和 Publisher 的 timeout 保持一致。
        return Schedulers.parallel().now(TimeUnit.NANOSECONDS);
    }
}
