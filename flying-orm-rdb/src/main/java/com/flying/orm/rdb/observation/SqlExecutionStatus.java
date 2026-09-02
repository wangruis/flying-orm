package com.flying.orm.rdb.observation;

/**
 * SQL 执行最终状态。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum SqlExecutionStatus {
    SUCCESS,
    ERROR,
    CANCELLED
}
