package com.flying.orm.rdb.jdbc;

/**
 * JDBC 批量预检后才发现外部事务的明确拒绝信号。
 *
 * <p>单独的类型让外层保留“SQL 前拒绝”语义，不会把它误包装成已经执行过的批量失败。</p>
 */
final class JdbcExternalTransactionModeException extends IllegalStateException {

    JdbcExternalTransactionModeException(String message) {
        super(message);
    }
}
