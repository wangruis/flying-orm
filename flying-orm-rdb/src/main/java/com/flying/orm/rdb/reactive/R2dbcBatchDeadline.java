package com.flying.orm.rdb.reactive;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一次批量订阅共用的总截止时间。
 *
 * <p>连接获取、分片执行、回执读写和提交都从同一份总预算里扣时间。开始新分片不会重新计时，
 * 因而慢连接池或大量小分片不能把调用方设置的超时悄悄放大。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
record R2dbcBatchDeadline(Duration timeout, long startedAtNanos) {

    R2dbcBatchDeadline {
        timeout = Objects.requireNonNull(timeout, "batch timeout must not be null");
    }

    static R2dbcBatchDeadline start(Duration timeout) {
        return new R2dbcBatchDeadline(timeout, nowNanos());
    }

    <T> Mono<T> protect(Mono<T> source) {
        Mono<T> safeSource = Objects.requireNonNull(source, "batch mono must not be null");
        if (timeout.isZero()) {
            return safeSource;
        }
        return Mono.defer(() -> {
            Duration remaining = remaining();
            return remaining.isZero()
                    ? Mono.<T>error(new TimeoutException("r2dbc batch timed out before the next operation"))
                    : safeSource.timeout(remaining);
        });
    }

    <T> Flux<T> protect(Flux<T> source) {
        Flux<T> safeSource = Objects.requireNonNull(source, "batch flux must not be null");
        if (timeout.isZero()) {
            return safeSource;
        }
        return Flux.defer(() -> {
            Duration remaining = remaining();
            if (remaining.isZero()) {
                return Flux.<T>error(new TimeoutException("r2dbc batch timed out before the next operation"));
            }
            // cache 使同一个单调截止信号不受 timeout 在每个 onNext 后取消/重订阅的影响，持续数据也不能重置总预算。
            Mono<Long> deadlineSignal = Mono.delay(remaining).cache();
            return safeSource.timeout(deadlineSignal, ignored -> deadlineSignal);
        });
    }

    private Duration remaining() {
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
