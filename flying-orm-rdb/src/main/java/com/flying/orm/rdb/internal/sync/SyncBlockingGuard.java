package com.flying.orm.rdb.internal.sync;

import com.flying.orm.rdb.internal.DurationLimits;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;

/**
 * 同步 JDBC 门面的线程边界，以及响应式实体生命周期回调的受控等待工具。
 *
 * <p>数据库访问本身始终走原生 JDBC。同步入口不能运行在 Reactor non-blocking 线程，否则会直接占住
 * WebFlux 事件循环；实体监听器仍使用 Mono 契约时，只允许在普通线程或虚拟线程按明确超时等待。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class SyncBlockingGuard {

    private SyncBlockingGuard() {
    }

    /** 在读取集合、获取 JDBC 连接或执行 SQL 之前拒绝 Reactor 事件线程。 */
    public static void rejectNonBlockingThread() {
        if (Schedulers.isInNonBlockingThread()) {
            throw new IllegalStateException("blocking JDBC API must not run on a Reactor non-blocking thread");
        }
    }

    /** 等待允许空完成的实体生命周期操作；这里不执行数据库 I/O。 */
    public static <T> T nullable(Mono<T> operation, Duration timeout) {
        rejectNonBlockingThread();
        return Objects.requireNonNull(operation, "sync lifecycle operation must not be null")
                      .block(DurationLimits.clamp(
                              requirePositiveTimeout(timeout, "sync lifecycle timeout")));
    }

    /** 同步等待必须有明确上限。 */
    public static Duration requirePositiveTimeout(Duration timeout, String name) {
        Duration safeTimeout = Objects.requireNonNull(timeout, name + " must not be null");
        if (safeTimeout.isZero() || safeTimeout.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return safeTimeout;
    }
}
