package com.flying.orm.rdb.observation;

/**
 * 执行器这次在做什么。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum SqlExecutionOperation {
    QUERY,
    UPDATE,
    CHUNKED_BATCH_WRITE
}
