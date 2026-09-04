package com.flying.orm.rdb.lock;

/**
 * 锁冲突时由数据库采用的受控等待方式。
 *
 * @author wangr
 * @version v3.2
 */
public enum ReadLockWait {
    WAIT,
    NOWAIT,
    SKIP_LOCKED
}
