package com.flying.orm.rdb.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/** JDBC 语句取消的公共细节放在这里，普通查询和高级游标走同一套中断语义。 */
final class JdbcStatementControl {

    private JdbcStatementControl() {
    }

    static void requireNotInterrupted(Statement statement) throws SQLException {
        if (!Thread.currentThread().isInterrupted()) {
            return;
        }
        SQLException cancelled = new SQLException(
                "jdbc operation was cancelled because the calling thread was interrupted", "HY008");
        try {
            statement.cancel();
        } catch (SQLException | RuntimeException | Error cancelFailure) {
            VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(cancelFailure);
            if (fatal != null) {
                JdbcThrowableGraph.addSuppressedIfAcyclic(fatal, cancelled);
                throw fatal;
            }
            // 取消是尽力而为；下面稳定的 HY008 仍然要告诉上层这次调用已被主动中断。
            JdbcThrowableGraph.addSuppressedIfAcyclic(cancelled, cancelFailure);
        }
        throw cancelled;
    }
}
