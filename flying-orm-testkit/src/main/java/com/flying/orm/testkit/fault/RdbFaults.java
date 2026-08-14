package com.flying.orm.testkit.fault;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

/**
 * 常见数据库故障的测试替身。错误分类和 SQLState 都按 flying-orm 的公开口径给出，
 * 测试上层恢复逻辑时不用依赖某个驱动的私有异常类。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class RdbFaults {

    private RdbFaults() {
    }

    /** 模拟连接在执行期间断开。 */
    public static RdbException connectionInterrupted() {
        return fault(RdbErrorKind.CONNECTION, "08006", "injected database connection interruption");
    }

    /** 模拟数据库或驱动报告执行超时。 */
    public static RdbException timeout() {
        return fault(RdbErrorKind.TIMEOUT, "HYT00", "injected database operation timeout");
    }

    /** 模拟数据库检测到事务死锁。 */
    public static RdbException deadlock() {
        return fault(RdbErrorKind.DEADLOCK, "40P01", "injected database deadlock");
    }

    /** 模拟等待行锁或表锁超时。 */
    public static RdbException lockTimeout() {
        return fault(RdbErrorKind.LOCK_TIMEOUT, "55P03", "injected database lock timeout");
    }

    /** 模拟驱动确认操作已取消。 */
    public static RdbException cancelled() {
        return fault(RdbErrorKind.CANCELLED, "57014", "injected database cancellation");
    }

    private static RdbException fault(RdbErrorKind kind, String sqlState, String detail) {
        IllegalStateException cause = new IllegalStateException(detail);
        return new RdbException(kind, detail, sqlState, 0, cause);
    }
}
