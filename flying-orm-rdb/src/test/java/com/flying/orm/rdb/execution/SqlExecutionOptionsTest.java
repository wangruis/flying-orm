package com.flying.orm.rdb.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证 SQL 执行与资源清理使用彼此独立的有界保护配置。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
class SqlExecutionOptionsTest {

    /** 安全默认值必须限制清理时间，避免已完成业务结果被连接关闭永久挂起。 */
    @Test
    void safeDefaultsBoundResourceCleanupToFiveSeconds() {
        assertEquals(Duration.ofSeconds(5), SqlExecutionOptions.safeDefaults().cleanupTimeout());
        assertEquals(64L * 1024 * 1024, SqlExecutionOptions.safeDefaults().maxResultBytes());
        assertEquals(0, SqlExecutionOptions.safeDefaults().fetchSize());
    }

    /** 显式 unlimited 同时解除普通 SQL 和资源清理时限。 */
    @Test
    void unlimitedExplicitlyDisablesResourceCleanupTimeout() {
        assertEquals(Duration.ZERO, SqlExecutionOptions.unlimited().cleanupTimeout());
        assertEquals(0L, SqlExecutionOptions.unlimited().maxResultBytes());
    }

    /** 超时便捷入口也必须约束取消后的连接清理，否则 usingWhen 会无限延迟超时信号。 */
    @Test
    void timeoutFactoryBoundsCancellationCleanup() {
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofMillis(300));

        assertEquals(Duration.ofMillis(300), options.timeout());
        assertEquals(Duration.ofMillis(300), options.cleanupTimeout());
        assertEquals(SqlExecutionOptions.DEFAULT_MAX_ROWS, options.maxRows());
    }

    /** 只关闭业务 SQL 总超时时，资源清理仍须保留安全默认边界。 */
    @Test
    void zeroExecutionTimeoutKeepsDefaultCleanupBoundary() {
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ZERO);

        assertEquals(Duration.ZERO, options.timeout());
        assertEquals(SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT, options.cleanupTimeout());
    }

    /** 负清理时限没有可执行语义，必须在配置构造边界立即拒绝。 */
    @Test
    void rejectsNegativeResourceCleanupTimeout() {
        assertThrows(IllegalArgumentException.class,
                     () -> new SqlExecutionOptions(Duration.ZERO,
                                                   0,
                                                   0,
                                                   0,
                                                   0,
                                                   Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class,
                     () -> SqlExecutionOptions.safeDefaults().withFetchSize(-1));
    }
}
