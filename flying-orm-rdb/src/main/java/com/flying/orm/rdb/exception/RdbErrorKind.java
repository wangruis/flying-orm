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
    DUPLICATE_KEY,
    /** 外键、非空、检查约束等完整性错误。 */
    CONSTRAINT,
    /** SQL 语法、对象名或参数布局错误。 */
    BAD_SQL,
    /** 建连、连接中断或连接不可用。 */
    CONNECTION,
    /** SQL 执行超过约定时间。 */
    TIMEOUT,
    /** 数据库检测到死锁并终止了当前操作。 */
    DEADLOCK,
    /** 等锁超过数据库允许时间。 */
    LOCK_TIMEOUT,
    /** 调用被主动取消。 */
    CANCELLED,
    /** 驱动信息不足，暂时不能可靠归类。 */
    UNKNOWN
}
