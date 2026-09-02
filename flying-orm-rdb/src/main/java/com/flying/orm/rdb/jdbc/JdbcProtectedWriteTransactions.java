package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.sql.Connection;

/**
 * 集中处理 JDBC 受保护写的回滚确认和 UNKNOWN 分类。
 *
 * <p>事务结果无法确认时构造稳定 UNKNOWN；连接会在事务终态记录后直接归还给提供者。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedWriteTransactions {

    private JdbcProtectedWriteTransactions() {
    }

    static RollbackResult rollback(Connection connection,
                                   boolean external,
                                   boolean transactionStarted,
                                   Throwable primary) {
        if (external || !transactionStarted) {
            return new RollbackResult(true, null);
        }
        if (isUnknown(primary)) {
            return new RollbackResult(false, null);
        }
        JdbcBatchSupport.RollbackOutcome rollback = JdbcBatchSupport.rollbackAfterFailure(connection, primary);
        return new RollbackResult(rollback.confirmed(), rollback.cleanupFatal());
    }

    static RdbException commitUnknown(Throwable cause) {
        return new RdbException(RdbErrorKind.UNKNOWN, "protected write commit outcome is unknown",
                                null, null, cause);
    }

    static RdbException rollbackUnknown(Throwable cause) {
        return new RdbException(RdbErrorKind.UNKNOWN, "protected write rollback outcome is unknown",
                                null, null, cause);
    }

    static boolean isUnknown(Throwable error) {
        return error instanceof RdbException rdb && rdb.kind() == RdbErrorKind.UNKNOWN;
    }

    /** 记录回滚是否确认，以及清理路径中必须优先传播的 JVM 致命错误。 */
    record RollbackResult(boolean confirmed, VirtualMachineError fatal) {
    }
}
