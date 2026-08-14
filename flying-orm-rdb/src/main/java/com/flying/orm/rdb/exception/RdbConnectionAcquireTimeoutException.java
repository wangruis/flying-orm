package com.flying.orm.rdb.exception;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 等待 R2DBC 连接超过上限时抛出这个异常。它属于连接资源不足，不代表 SQL 本身执行慢。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
public final class RdbConnectionAcquireTimeoutException extends RdbException {

    private static final long serialVersionUID = 1L;

    private final Duration timeout;

    /**
     * 创建连接获取超时异常。
     *
     * @param timeout 最多等待连接多久，必须大于 0
     * @param cause   Reactor 发出的原始超时异常
     */
    public RdbConnectionAcquireTimeoutException(Duration timeout, TimeoutException cause) {
        super(RdbErrorKind.CONNECTION,
              "database connection acquisition timed out after " + requirePositive(timeout),
              null,
              null,
              Objects.requireNonNull(cause, "connection acquisition timeout cause must not be null"));
        this.timeout = timeout;
    }

    /**
     * @return 配置的连接等待上限
     */
    public Duration timeout() {
        return timeout;
    }

    private static Duration requirePositive(Duration timeout) {
        Duration safeTimeout = Objects.requireNonNull(timeout, "connection acquisition timeout must not be null");
        if (safeTimeout.isZero() || safeTimeout.isNegative()) {
            throw new IllegalArgumentException("connection acquisition timeout must be greater than zero");
        }
        return safeTimeout;
    }
}
