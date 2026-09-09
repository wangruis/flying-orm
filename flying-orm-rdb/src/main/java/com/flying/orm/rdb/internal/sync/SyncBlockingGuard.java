package com.flying.orm.rdb.internal.sync;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;

/**
 * 同步 JDBC 门面的线程边界，以及响应式实体生命周期回调的受控等待工具。
 *
 * <p>数据库访问本身始终走原生 JDBC。同步入口不能运行在 Reactor non-blocking 线程，否则会直接占住
 * WebFlux 事件循环；实体监听器仍使用 Mono 契约时，只允许在普通线程或虚拟线程同步等待。
 * 监听器的执行时限由上层控制，本桥接层不设置等待预算。</p>
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
    public static <T> T nullable(Mono<T> operation) {
        rejectNonBlockingThread();
        return Objects.requireNonNull(operation, "sync lifecycle operation must not be null")
                      .block();
    }

    /** 保留旧签名；只接受表示无 ORM 等待预算的零值，非零时限必须由上层管理。 */
    public static <T> T nullable(Mono<T> operation, Duration timeout) {
        if (!Objects.requireNonNull(timeout, "sync lifecycle timeout must not be null").isZero()) {
            throw new IllegalArgumentException(
                    "sync lifecycle timeout must be zero; lifecycle timeouts must be managed by the caller");
        }
        return nullable(operation);
    }

    /** 仅保留正值参数校验契约；本方法不应用等待时限。 */
    public static Duration requirePositiveTimeout(Duration timeout, String name) {
        Duration safeTimeout = Objects.requireNonNull(timeout, name + " must not be null");
        if (safeTimeout.isZero() || safeTimeout.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return safeTimeout;
    }
}
