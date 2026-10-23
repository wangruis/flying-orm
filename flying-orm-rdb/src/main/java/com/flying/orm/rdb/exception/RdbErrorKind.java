package com.flying.orm.rdb.exception;

/**
 * 数据库错误先归成这几类，上层业务不用认识每个驱动自己的异常写法。
 *
 * @author wangr
 * @date 2026-07-26
 * @version v1.0
 */
public enum RdbErrorKind {
    /** 唯一键或主键重复。 */
    DUPLICATE_KEY("database duplicate key conflict"),
    /** 外键、非空、检查约束等完整性错误。 */
    CONSTRAINT("database integrity constraint failed"),
    /** SQL 语法、对象名或参数布局错误。 */
    BAD_SQL("database rejected sql"),
    /** 建连、连接中断或连接不可用。 */
    CONNECTION("database connection failed"),
    /** SQL 执行超过约定时间。 */
    TIMEOUT("database operation timed out"),
    /** 数据库检测到死锁并终止了当前操作。 */
    DEADLOCK("database transaction deadlocked"),
    /** 等锁超过数据库允许时间。 */
    LOCK_TIMEOUT("database lock wait timed out"),
    /** 调用被主动取消。 */
    CANCELLED("database operation was cancelled"),
    /** 驱动信息不足，暂时不能可靠归类。 */
    UNKNOWN("database operation failed");

    private final String message;

    RdbErrorKind(String message) {
        this.message = message;
    }

    /** @return 可公开的固定分类说明，不包含驱动消息、SQL 或参数值。 */
    public String message() {
        return message;
    }
}
