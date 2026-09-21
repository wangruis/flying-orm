package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import io.r2dbc.spi.R2dbcTimeoutException;

import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeoutException;

/**
 * 观测里使用的数据库异常分类。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum SqlFailureCategory {
    /** 执行成功，没有失败原因。 */
    NONE,
    /** 唯一键或主键重复。 */
    DUPLICATE_KEY,
    /** 外键、非空、检查约束等完整性错误。 */
    CONSTRAINT,
    /** SQL、对象名或参数布局错误。 */
    BAD_SQL,
    /** 建连失败、连接中断或连接不可用。 */
    CONNECTION,
    /** SQL 执行超过调用方设置的时间上限。 */
    TIMEOUT,
    /** 数据库检测到死锁并终止了当前操作。 */
    DEADLOCK,
    /** 等锁超过数据库允许的时间。 */
    LOCK_TIMEOUT,
    /** 订阅被调用方主动取消。 */
    CANCELLED,
    /** 查询结果超过允许返回的最大行数。 */
    ROW_LIMIT,
    /** 查询结果累计估算内存超过执行保护上限。 */
    RESULT_MEMORY_LIMIT,
    /** 乐观锁条件没有命中预期数据。 */
    OPTIMISTIC_LOCK,
    /** 当前信息不足，不能可靠判断 SQL 执行的失败原因；不表示事务结果。 */
    UNKNOWN;

    /**
     * 把执行异常归到稳定的观测类别。
     *
     * <p>批量和异步调用可能给驱动异常再包几层。这里沿 cause 链检查当前异常及最多 32 层原因，
     * 遇到自引用提前结束；深度上限也保证循环原因链不会导致无限遍历。</p>
     *
     * @param error 执行链抛出的异常
     * @return 可用于指标、日志和告警聚合的粗粒度类别
     */
    public static SqlFailureCategory classify(Throwable error) {
        boolean optimisticLock = false;
        boolean timeout = false;
        boolean rowLimit = false;
        boolean memoryLimit = false;
        RdbException databaseFailure = null;
        Throwable current = error;
        for (int depth = 0; current != null && depth <= 32; depth++) {
            optimisticLock |= current instanceof BatchOptimisticLockException;
            timeout |= current instanceof TimeoutException
                    || current instanceof SQLTimeoutException
                    || current instanceof R2dbcTimeoutException;
            rowLimit |= current instanceof SqlRowLimitExceededException;
            memoryLimit |= current instanceof SqlResultMemoryLimitExceededException;
            if (databaseFailure == null && current instanceof RdbException rdbException) {
                databaseFailure = rdbException;
            }
            Throwable next = current.getCause();
            if (next == current) {
                break;
            }
            current = next;
        }
        if (optimisticLock) {
            return OPTIMISTIC_LOCK;
        }
        if (timeout) {
            return TIMEOUT;
        }
        if (rowLimit) {
            return ROW_LIMIT;
        }
        if (memoryLimit) {
            return RESULT_MEMORY_LIMIT;
        }
        return databaseFailure == null ? UNKNOWN : fromKind(databaseFailure.kind());
    }

    static SqlFailureCategory fromKind(RdbErrorKind kind) {
        return switch (kind) {
            case DUPLICATE_KEY -> DUPLICATE_KEY;
            case CONSTRAINT -> CONSTRAINT;
            case BAD_SQL -> BAD_SQL;
            case CONNECTION -> CONNECTION;
            case TIMEOUT -> TIMEOUT;
            case DEADLOCK -> DEADLOCK;
            case LOCK_TIMEOUT -> LOCK_TIMEOUT;
            case CANCELLED -> CANCELLED;
            case UNKNOWN -> UNKNOWN;
        };
    }

}
