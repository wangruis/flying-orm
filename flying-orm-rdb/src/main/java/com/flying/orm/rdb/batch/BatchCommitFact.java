package com.flying.orm.rdb.batch;

/**
 * 当前返回点能够证明的事务结果。
 *
 * @author wangr
 * @version v3.2
 */
public enum BatchCommitFact {
    COMMITTED,
    ROLLED_BACK,
    PENDING_EXTERNAL,
    UNKNOWN,
    NOT_APPLICABLE
}
