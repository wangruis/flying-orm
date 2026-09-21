package com.flying.orm.rdb.observation;

/**
 * 批量观测事件的类型。记录累计执行进度和整次调用汇总。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum BatchExecutionEventType {

    /** 已形成新的累计 SQL 执行事实。 */
    PROGRESS,
    /** 整次批量写入已经汇总完成。 */
    SUMMARY
}
