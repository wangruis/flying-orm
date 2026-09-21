package com.flying.orm.rdb.batch;

/**
 * 一次批量 SQL 执行已经形成的事实状态；不表达外层事务最终提交结果。
 *
 * @author wangr
 * @version v3.2
 */
public enum BatchExecutionState {
    SUCCESS,
    FAILED,
    TIMED_OUT,
    CANCELLED,
    PARTIAL,
    UNKNOWN
}
