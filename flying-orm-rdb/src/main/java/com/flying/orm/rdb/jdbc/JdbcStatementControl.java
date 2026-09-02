package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.directVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import java.sql.SQLException;
import java.sql.Statement;

/** JDBC 语句取消的公共细节放在这里，普通查询和高级游标走同一套中断语义。 */
final class JdbcStatementControl {

    private JdbcStatementControl() {
    }

    static void requireNotInterrupted() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw cancelled();
        }
    }

    static void requireNotInterrupted(Statement statement) throws SQLException {
        if (!Thread.currentThread().isInterrupted()) {
            return;
        }
        SQLException cancelled = cancelled();
        try {
            statement.cancel();
        } catch (SQLException | RuntimeException | Error cancelFailure) {
            VirtualMachineError fatal = directVirtualMachineError(cancelFailure);
            if (fatal != null) {
                suppress(fatal, cancelled);
                throw fatal;
            }
            // 取消是尽力而为；下面稳定的 HY008 仍然要告诉上层这次调用已被主动中断。
            suppress(cancelled, cancelFailure);
        }
        throw cancelled;
    }

    private static SQLException cancelled() {
        return new SQLException(
                "jdbc operation was cancelled because the calling thread was interrupted", "HY008");
    }
}
