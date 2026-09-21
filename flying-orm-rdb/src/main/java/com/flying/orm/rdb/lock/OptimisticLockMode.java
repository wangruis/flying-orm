package com.flying.orm.rdb.lock;

/**
 * 乐观锁字段更新方式。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public enum OptimisticLockMode {

    /**
     * 数字版本号自增，比如 version = version + 1。
     */
    INCREMENT,

    /**
     * 调用方直接给新值，比如 updated_at = now。
     */
    ASSIGN
}
