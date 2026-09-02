package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.internal.DurationLimits;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * 为一次 JDBC 受保护写维护统一截止点，避免业务 SQL、owner 查询和侧索引 SQL 各自重新获得完整超时。
 *
 * <p>JDBC 驱动仍负责中断正在执行的单条语句；本类型只在语句之间收紧剩余时间，并在提交前阻止已经超时的
 * 工作继续提交，不使用后台线程伪造驱动无法保证的异步取消。</p>
 *
 * @author wangr
 * @date 2026-08-11
 * @version v1.0
 */
final class JdbcProtectedWriteDeadline {

    private final SqlExecutionOptions options;
    private final long startedAt;
    private final long timeoutNanos;

    private JdbcProtectedWriteDeadline(SqlExecutionOptions options) {
        this.options = Objects.requireNonNull(options, "sql execution options must not be null");
        this.startedAt = System.nanoTime();
        this.timeoutNanos = options.timeout().isZero() ? Long.MAX_VALUE : DurationLimits.nanos(options.timeout());
    }

    /** @return 从当前执行保护开始计时的统一截止点 */
    static JdbcProtectedWriteDeadline start(SqlExecutionOptions options) {
        return new JdbcProtectedWriteDeadline(options);
    }

    /** @return 保留所有容量边界、仅把语句超时收紧为当前剩余时间的执行选项 */
    SqlExecutionOptions remainingOptions() {
        return timeoutNanos == Long.MAX_VALUE
                ? options : options.withTimeout(Duration.ofNanos(remainingNanos()));
    }

    /** 提交或进入下一阶段前确认整次调用仍在截止时间内。 */
    void requireRemaining() {
        if (timeoutNanos != Long.MAX_VALUE) {
            remainingNanos();
        }
    }

    /** 侧索引复用批量执行器时仍使用这次写入的原始截止点。 */
    JdbcBatchSupport.BatchDeadline batchDeadline() {
        return new JdbcBatchSupport.BatchDeadline(timeoutNanos == Long.MAX_VALUE
                ? Long.MAX_VALUE : DurationLimits.addSaturated(startedAt, timeoutNanos));
    }

    SqlExecutionTimeoutException timeout(TimeoutException cause) {
        return new SqlExecutionTimeoutException(options.timeout(), cause);
    }

    private long remainingNanos() {
        long remaining = timeoutNanos - (System.nanoTime() - startedAt);
        if (remaining <= 0L) {
            throw timeout(new TimeoutException("jdbc protected write timed out"));
        }
        return remaining;
    }

}
