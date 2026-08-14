package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

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
    /** 当前信息不足，不能可靠判断失败原因或事务结果。 */
    UNKNOWN;

    /**
     * 把执行异常归到稳定的观测类别。
     *
     * <p>批量、异步和事务层经常会给驱动异常再包几层。这里会沿 cause 链查找真正原因，
     * 并按对象身份记录已经访问的节点，因此第三方异常即使错误地形成循环也不会卡住观测线程。</p>
     *
     * @param error 执行链抛出的异常
     * @return 可用于指标、日志和告警聚合的粗粒度类别
     */
    public static SqlFailureCategory classify(Throwable error) {
        if (find(error, BatchOptimisticLockException.class) != null) {
            return OPTIMISTIC_LOCK;
        }
        if (find(error, SqlExecutionTimeoutException.class) != null) {
            return TIMEOUT;
        }
        if (find(error, SqlRowLimitExceededException.class) != null) {
            return ROW_LIMIT;
        }
        if (find(error, SqlResultMemoryLimitExceededException.class) != null) {
            return RESULT_MEMORY_LIMIT;
        }
        RdbException rdbException = find(error, RdbException.class);
        return rdbException == null ? UNKNOWN : fromKind(rdbException.kind());
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

    private static <T extends Throwable> T find(Throwable error, Class<T> type) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            current = current.getCause();
        }
        return null;
    }
}
