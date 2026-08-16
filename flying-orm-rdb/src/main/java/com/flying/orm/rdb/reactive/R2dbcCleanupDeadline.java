package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.internal.ReactiveTimeouts;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 一次 R2DBC 资源收尾共用的绝对截止时间。
 *
 * <p>LOB、事务回滚、会话 reset、连接关闭和物理失效只能消费同一份清理预算，后续阶段不能重新获得
 * 完整的 {@code cleanupTimeout}。0 明确表示由可靠的外部资源边界负责，不在 ORM 内限时。</p>
 *
 * @author wangr
 * @date 2026-08-15
 * @version v2.0
 */
final class R2dbcCleanupDeadline {

    private final Duration timeout;
    private final long startedAtNanos;

    private final AtomicBoolean finalInvalidationAttempted = new AtomicBoolean();

    private R2dbcCleanupDeadline(Duration timeout, long startedAtNanos) {
        this.timeout = Objects.requireNonNull(timeout, "cleanup timeout must not be null");
        this.startedAtNanos = startedAtNanos;
    }

    static R2dbcCleanupDeadline start(Duration timeout) {
        return new R2dbcCleanupDeadline(timeout, nowNanos());
    }

    <T> Mono<T> protect(Mono<T> source) {
        Mono<T> safeSource = Objects.requireNonNull(source, "cleanup publisher must not be null");
        if (timeout.isZero()) {
            return safeSource;
        }
        return Mono.defer(() -> {
            Duration remaining = remaining();
            if (remaining.isZero()) {
                // 截止点后不再等待，但仍按订阅顺序给同步释放动作一次立即完成机会。
                return Mono.firstWithSignal(safeSource, Mono.error(timeoutFailure()));
            }
            return safeSource.timeout(ReactiveTimeouts.duration(remaining));
        });
    }

    /**
     * 预算耗尽时仍订阅一次物理连接失效入口，让同步池淘汰动作有机会立即完成；异步挂起实现会被同一信号立刻取消。
     * 同一 deadline 最多允许一次这种终态尝试，不能按待清理资源数量不断追加时间。
     */
    <T> Mono<T> protectInvalidation(Mono<T> source) {
        Mono<T> safeSource = Objects.requireNonNull(source, "connection invalidation publisher must not be null");
        if (timeout.isZero()) {
            return safeSource;
        }
        return Mono.defer(() -> {
            Duration remaining = remaining();
            if (!remaining.isZero()) {
                return safeSource.timeout(ReactiveTimeouts.duration(remaining));
            }
            if (!finalInvalidationAttempted.compareAndSet(false, true)) {
                return Mono.error(timeoutFailure());
            }
            return Mono.firstWithSignal(safeSource, Mono.error(timeoutFailure()));
        });
    }

    Duration remaining() {
        if (timeout.isZero()) {
            return Duration.ZERO;
        }
        long elapsed = nowNanos() - startedAtNanos;
        long remaining = saturatedNanos(timeout) - Math.max(0L, elapsed);
        return remaining <= 0L ? Duration.ZERO : Duration.ofNanos(remaining);
    }

    boolean unlimited() {
        return timeout.isZero();
    }

    private static TimeoutException timeoutFailure() {
        return new TimeoutException("R2DBC resource cleanup timed out");
    }

    private static long saturatedNanos(Duration value) {
        try {
            return value.toNanos();
        } catch (ArithmeticException ignored) {
            return Long.MAX_VALUE;
        }
    }

    private static long nowNanos() {
        return Schedulers.parallel().now(TimeUnit.NANOSECONDS);
    }
}
