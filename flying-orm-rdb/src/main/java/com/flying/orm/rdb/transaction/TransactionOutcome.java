package com.flying.orm.rdb.transaction;

/**
 * 上层事务管理器最终能够确认的物理事务结果。
 *
 * <p>JDBC 和 R2DBC 共用这三个状态，批量结果、事务观测和实体生命周期因而不会因为执行内核不同
 * 而出现两套解释。这里没有“执行成功”状态：SQL 成功加入外部事务时只能先返回 ENLISTED，必须等
 * 上层真正提交或回滚后，才能得到这里的最终结果。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public enum TransactionOutcome {
    /** 物理事务已经确认提交。 */
    COMMITTED,
    /** 物理事务已经确认回滚。 */
    ROLLED_BACK,
    /** 事务管理器也无法可靠判断最终结果。 */
    UNKNOWN
}
