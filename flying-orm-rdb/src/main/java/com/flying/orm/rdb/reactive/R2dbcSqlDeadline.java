package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.internal.ReactiveTimeouts;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一次 R2DBC SQL 执行共享的绝对截止时间。
 *
 * <p>该截止时间只在连接已经借到、准备执行 SQL 时开始。事务解析和连接池排队归上层事务管理器与连接池负责；
 * 资源清理由独立 cleanup deadline 保护，不会因为 SQL 截止时间耗尽而跳过 LOB 释放或连接隔离。</p>
 *
 * @author wangr
 * @date 2026-08-15
 * @version v2.0
 */
final class R2dbcSqlDeadline {

    private final Duration executionTimeout;
    private final long startedAtNanos;

    private R2dbcSqlDeadline(SqlExecutionOptions options, long startedAtNanos) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        this.executionTimeout = safeOptions.timeout();
        this.startedAtNanos = startedAtNanos;
    }

    static R2dbcSqlDeadline start(SqlExecutionOptions options) {
        return new R2dbcSqlDeadline(options, nowNanos());
    }

    static R2dbcSqlDeadline currentOrStart(ContextView context, SqlExecutionOptions options) {
        ContextView safeContext = Objects.requireNonNull(context, "reactor context must not be null");
        return safeContext.<R2dbcSqlDeadline>getOrEmpty(R2dbcSqlDeadline.class)
                          .orElseGet(() -> start(options));
    }

    <T> Flux<T> bind(Flux<T> source) {
        return Objects.requireNonNull(source, "sql deadline flux must not be null")
                      .contextWrite(context -> context.put(R2dbcSqlDeadline.class, this));
    }

    <T> Mono<T> bind(Mono<T> source) {
        return Objects.requireNonNull(source, "sql deadline mono must not be null")
                      .contextWrite(context -> context.put(R2dbcSqlDeadline.class, this));
    }

    <T> Mono<T> protectExecution(Mono<T> source) {
        Mono<T> safeSource = Objects.requireNonNull(source, "sql execution mono must not be null");
        if (executionTimeout.isZero()) {
            return safeSource;
        }
        return Mono.defer(() -> {
            Duration remaining = remaining(executionTimeout);
            return remaining.isZero()
                    ? Mono.error(executionTimeout())
                    : safeSource.timeout(ReactiveTimeouts.duration(remaining), Mono.error(executionTimeout()));
        });
    }

    <T> Flux<T> protectExecution(Flux<T> source) {
        Flux<T> safeSource = Objects.requireNonNull(source, "sql execution flux must not be null");
        if (executionTimeout.isZero()) {
            return safeSource;
        }
        return Flux.defer(() -> {
            Duration remaining = remaining(executionTimeout);
            return remaining.isZero()
                    ? Flux.error(executionTimeout())
                    : SqlExecutionTimeouts.total(safeSource, remaining, executionTimeout);
        });
    }

    private Duration remaining(Duration timeout) {
        if (timeout.isZero()) {
            return Duration.ZERO;
        }
        long elapsed = Math.max(0L, nowNanos() - startedAtNanos);
        long remaining = saturatedNanos(timeout) - elapsed;
        return remaining <= 0L ? Duration.ZERO : Duration.ofNanos(remaining);
    }

    private SqlExecutionTimeoutException executionTimeout() {
        return new SqlExecutionTimeoutException(
                executionTimeout, new TimeoutException("R2DBC SQL execution deadline elapsed"));
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
