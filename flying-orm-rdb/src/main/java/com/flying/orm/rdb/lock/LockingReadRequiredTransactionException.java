package com.flying.orm.rdb.lock;

/**
 * 锁定读取在 SQL 或自有连接获取前没有发现调用方管理事务时抛出。
 *
 * @author wangr
 * @version v3.2
 */
public final class LockingReadRequiredTransactionException extends IllegalStateException {

    public LockingReadRequiredTransactionException() {
        super("locking read requires a caller-managed transaction");
    }
}
