package com.flying.orm.rdb.execution;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.time.Duration;
import java.util.Objects;

/**
 * SQL 执行超过调用方设置的时间上限。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public final class SqlExecutionTimeoutException extends RuntimeException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final Duration timeout;

    /**
     * @param timeout 本次调用设置的时间上限
     * @param cause Reactor 超时产生的原始异常
     */
    public SqlExecutionTimeoutException(Duration timeout, Throwable cause) {
        super("sql execution timed out: timeout=" + timeout, cause);
        this.timeout = Objects.requireNonNull(timeout, "sql execution timeout must not be null");
    }

    /**
     * @return 本次调用设置的时间上限
     */
    public Duration timeout() {
        return timeout;
    }

    /** @return 不包含 SQL 和参数值的统一执行错误报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("EXECUTION", "TIMEOUT", timeout.toString(), null, null, getMessage());
    }
}
