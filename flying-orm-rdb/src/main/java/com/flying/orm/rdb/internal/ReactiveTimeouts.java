package com.flying.orm.rdb.internal;

import java.time.Duration;
import java.util.Objects;

/**
 * 把公开的 {@link Duration} 安全适配到 Reactor 以 {@code long} 纳秒表示的定时边界。
 *
 * <p>Java Duration 的范围大于 Reactor 定时器的纳秒范围。合法的极远截止时间应等价于当前运行期内
 * 不会到期，不能在 Publisher 装配阶段抛出算术溢出。</p>
 *
 * @author wangr
 * @date 2026-08-15
 * @version v1.0
 */
@InternalApi
public final class ReactiveTimeouts {

    private static final Duration MAX_REACTOR_TIMEOUT = Duration.ofNanos(Long.MAX_VALUE);

    private ReactiveTimeouts() {
    }

    /**
     * 返回 Reactor 能安全换算为纳秒的等价超时。
     *
     * @param timeout 已校验为非负的超时
     * @return 原超时或 Reactor 可表示的最远截止时间
     */
    public static Duration duration(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "reactive timeout must not be null");
        // 先比较 seconds/nanos 字段，不在普通热路径重复执行带溢出检查的乘法。
        return safeTimeout.compareTo(MAX_REACTOR_TIMEOUT) > 0 ? MAX_REACTOR_TIMEOUT : safeTimeout;
    }

    /**
     * 将 Duration 转为饱和纳秒值，供直接调度接口使用。
     *
     * @param timeout 已校验为非负的超时
     * @return 可安全传给调度器的纳秒值
     */
    public static long nanos(Duration timeout) {
        return duration(timeout).toNanos();
    }
}
