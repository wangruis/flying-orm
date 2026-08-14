package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.sql.Connection;

/**
 * 集中处理 JDBC 受保护写的回滚确认、UNKNOWN 分类和 auto-commit 恢复。
 *
 * <p>事务结果无法确认时先隔离自有连接，再构造稳定 UNKNOWN；恢复阶段继续遵守 VME 优先和异常图无环规则。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedWriteTransactions {

    private JdbcProtectedWriteTransactions() {
    }

    static RollbackResult rollback(Connection connection,
                                   JdbcConnectionProvider.JdbcConnectionLease lease,
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
        if (!rollback.confirmed()) {
            lease.discardAfterUncertainTransaction(primary);
        }
        return new RollbackResult(rollback.confirmed(), rollback.cleanupFatal());
    }

    static Throwable restoreAutoCommit(Connection connection,
                                       JdbcConnectionProvider.JdbcConnectionLease lease,
                                       Throwable primary) {
        Throwable failure = JdbcBatchSupport.restoreAutoCommit(connection);
        if (failure == null) {
            return null;
        }
        lease.discardAfterUncertainTransaction(failure);
        VirtualMachineError primaryFatal = JdbcThrowableGraph.findVirtualMachineError(primary);
        VirtualMachineError restoreFatal = JdbcThrowableGraph.findVirtualMachineError(failure);
        if (primaryFatal != null) {
            JdbcThrowableGraph.addSuppressedIfAcyclic(primaryFatal, failure);
            throw primaryFatal;
        }
        if (restoreFatal != null) {
            JdbcThrowableGraph.addSuppressedIfAcyclic(restoreFatal, primary);
            throw restoreFatal;
        }
        if (primary == null) {
            // 数据库事务已经确认提交；普通会话恢复故障只能隔离连接，不能把成功改写成失败。
            return failure;
        }
        JdbcThrowableGraph.addSuppressedIfAcyclic(primary, failure);
        return null;
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
