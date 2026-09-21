package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchExecutionState;

import java.util.Objects;

/**
 * 上层最常用的执行结果语义。比 status 和 failureCategory 更适合直接拿来分支处理。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public enum SqlExecutionResultKind {
    /** SQL 执行成功。 */
    SUCCESS,
    /** 调用被主动取消。 */
    CANCELLED,
    /** 执行超过调用方设置的时间上限。 */
    TIMEOUT,
    /** 查询返回行数超过上限。 */
    ROW_LIMIT,
    /** 查询结果累计估算内存超过执行保护上限。 */
    RESULT_MEMORY_LIMIT,
    /** 唯一键或主键重复。 */
    DUPLICATE_KEY,
    /** 数据完整性约束失败。 */
    CONSTRAINT,
    /** SQL、对象名或参数布局错误。 */
    BAD_SQL,
    /** 建连失败、连接中断或连接不可用。 */
    CONNECTION,
    /** 数据库检测到死锁。 */
    DEADLOCK,
    /** 等锁超过数据库允许的时间。 */
    LOCK_TIMEOUT,
    /** 乐观锁条件没有命中预期数据。 */
    OPTIMISTIC_LOCK,
    /** 只有部分输入具有成功执行事实。 */
    PARTIAL,
    /** 现有驱动信息不足以确认 SQL 执行结果。 */
    UNKNOWN;

    /**
     * 把普通 SQL 的执行状态和失败分类收口成上层容易处理的结果语义。
     *
     * @param status 执行状态
     * @param category 失败分类，成功时传 {@link SqlFailureCategory#NONE}
     * @return 统一结果语义
     */
    public static SqlExecutionResultKind fromSql(SqlExecutionStatus status, SqlFailureCategory category) {
        SqlExecutionStatus safeStatus = Objects.requireNonNull(status, "sql execution status must not be null");
        SqlFailureCategory safeCategory = Objects.requireNonNull(category, "sql failure category must not be null");
        if (safeStatus == SqlExecutionStatus.SUCCESS) {
            return SUCCESS;
        }
        if (safeStatus == SqlExecutionStatus.CANCELLED) {
            return CANCELLED;
        }
        return fromFailureCategory(safeCategory);
    }

    /** 仅合并 SQL 执行状态与失败分类，不推断事务结果。 */
    public static SqlExecutionResultKind fromBatch(BatchExecutionState state, SqlFailureCategory category) {
        Objects.requireNonNull(state, "batch execution state must not be null");
        Objects.requireNonNull(category, "sql failure category must not be null");
        return switch (state) {
            case SUCCESS -> SUCCESS;
            case CANCELLED -> CANCELLED;
            case TIMED_OUT -> TIMEOUT;
            case PARTIAL -> PARTIAL;
            case FAILED, UNKNOWN -> category == SqlFailureCategory.NONE ? UNKNOWN : fromFailureCategory(category);
        };
    }

    private static SqlExecutionResultKind fromFailureCategory(SqlFailureCategory category) {
        SqlFailureCategory safeCategory = Objects.requireNonNull(category,
                                                                 "sql failure category must not be null");
        return switch (safeCategory) {
            case NONE -> SUCCESS;
            case DUPLICATE_KEY -> DUPLICATE_KEY;
            case CONSTRAINT -> CONSTRAINT;
            case BAD_SQL -> BAD_SQL;
            case CONNECTION -> CONNECTION;
            case TIMEOUT -> TIMEOUT;
            case DEADLOCK -> DEADLOCK;
            case LOCK_TIMEOUT -> LOCK_TIMEOUT;
            case CANCELLED -> CANCELLED;
            case ROW_LIMIT -> ROW_LIMIT;
            case RESULT_MEMORY_LIMIT -> RESULT_MEMORY_LIMIT;
            case OPTIMISTIC_LOCK -> OPTIMISTIC_LOCK;
            case UNKNOWN -> UNKNOWN;
        };
    }
}
