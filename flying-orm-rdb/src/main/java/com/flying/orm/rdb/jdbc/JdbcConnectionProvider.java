package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.directVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;

/**
 * 每次执行 SQL 时确定 JDBC 连接的实际所有者。
 *
 * <p>外部事务连接只是借用，调用结束不能关闭；DataSource 连接由 flying-orm 本次调用借出，结束时必须归还。
 * 把这个判断集中在一个小对象里，可以避免查询、更新和生成键路径各自维护一套容易出错的关闭分支。</p>
 */
final class JdbcConnectionProvider {

    private final DataSource dataSource;
    private final JdbcTransactionParticipant transactionParticipant;
    private final SqlExecutionObserver cleanupObserver;

    JdbcConnectionProvider(DataSource dataSource, JdbcTransactionParticipant transactionParticipant) {
        this(dataSource, transactionParticipant, null);
    }

    JdbcConnectionProvider(DataSource dataSource,
                           JdbcTransactionParticipant transactionParticipant,
                           SqlExecutionObserver cleanupObserver) {
        this.dataSource = Objects.requireNonNull(dataSource, "jdbc data source must not be null");
        this.transactionParticipant = Objects.requireNonNull(
                transactionParticipant, "jdbc transaction participant must not be null");
        this.cleanupObserver = cleanupObserver;
    }

    JdbcConnectionProvider withTransactionParticipant(JdbcTransactionParticipant participant) {
        return new JdbcConnectionProvider(dataSource, participant, cleanupObserver);
    }

    JdbcConnectionLease acquire() throws SQLException {
        Optional<JdbcTransactionContext> transaction = currentTransaction();
        if (transaction.isPresent()) {
            return JdbcConnectionLease.external(transaction.get());
        }
        return acquireOwned();
    }

    /** 调用入口已确认没有外部事务时，按该次调用的归属借出自有连接。 */
    JdbcConnectionLease acquireOwned() throws SQLException {
        return JdbcConnectionLease.owned(dataSource.getConnection(), cleanupObserver);
    }

    Optional<JdbcTransactionContext> currentTransaction() {
        return Objects.requireNonNull(
                transactionParticipant.currentTransaction(), "current transaction must not be null");
    }

    /**
     * 一次 SQL 使用的连接租约。关闭外部租约是空操作，关闭自有租约才会把连接归还给连接池。
     *
     * <p>自有连接始终通过 JDBC 标准 {@code close()} 归还；是否复用或物理断开由驱动和连接池决定。
     * 外部事务连接始终由外部事务控制，不能由本租约关闭。</p>
     */
    static final class JdbcConnectionLease implements AutoCloseable {

        private final Connection connection;
        private final SqlTransactionSource transactionSource;
        private final boolean closeConnection;
        private final JdbcTransactionContext externalTransaction;
        private final SqlExecutionObserver cleanupObserver;
        private boolean transactionOutcomeConfirmed;
        private boolean suppressConfirmedCloseFailure;
        private Throwable transactionOutcomeUnknownFailure;

        private JdbcConnectionLease(Connection connection,
                                     SqlTransactionSource transactionSource,
                                     boolean closeConnection,
                                     JdbcTransactionContext externalTransaction,
                                     SqlExecutionObserver cleanupObserver) {
            this.connection = Objects.requireNonNull(connection, "jdbc connection must not be null");
            this.transactionSource = Objects.requireNonNull(
                    transactionSource, "sql transaction source must not be null");
            this.closeConnection = closeConnection;
            this.externalTransaction = externalTransaction;
            this.cleanupObserver = cleanupObserver == null ? null : SqlExecutionObservers.safe(cleanupObserver);
        }

        static JdbcConnectionLease external(JdbcTransactionContext transaction) {
            JdbcTransactionContext safeTransaction = Objects.requireNonNull(
                    transaction, "jdbc transaction context must not be null");
            return new JdbcConnectionLease(safeTransaction.connection(), SqlTransactionSource.EXTERNAL,
                                           false, safeTransaction, null);
        }

        static JdbcConnectionLease owned(Connection connection) {
            return owned(connection, null);
        }

        static JdbcConnectionLease owned(Connection connection, SqlExecutionObserver cleanupObserver) {
            return new JdbcConnectionLease(
                    connection, SqlTransactionSource.AUTO_COMMIT, true, null, cleanupObserver);
        }

        Connection connection() {
            return connection;
        }

        SqlTransactionSource transactionSource() {
            return transactionSource;
        }

        JdbcTransactionContext externalTransaction() {
            return externalTransaction;
        }

        /** 标记数据库事务已经明确提交或回滚，后续连接归还故障不能改写该终态。 */
        void markTransactionOutcomeConfirmed() {
            if (closeConnection) {
                transactionOutcomeConfirmed = true;
                suppressConfirmedCloseFailure = true;
            }
        }

        /** 已有主异常会接住 close 的 suppressed 诊断；这里只补充“事务终态已确认”的观测事实。 */
        void markTransactionOutcomeConfirmedWithPrimaryFailure() {
            if (closeConnection) {
                transactionOutcomeConfirmed = true;
            }
        }

        /**
         * 提交或回滚结果无法确认时，连接归还故障只能补充诊断，不能把 UNKNOWN 改写成普通 FAILED。
         */
        void markTransactionOutcomeUnknown(Throwable primaryFailure) {
            if (closeConnection) {
                transactionOutcomeUnknownFailure = Objects.requireNonNull(
                        primaryFailure, "unknown transaction outcome failure must not be null");
            }
        }

        @Override
        public void close() throws SQLException {
            if (!closeConnection) {
                return;
            }
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                if (!finishCloseFailure(closeFailure)) {
                    throw closeFailure;
                }
            } catch (RuntimeException | Error closeFailure) {
                if (!finishCloseFailure(closeFailure)) {
                    throw closeFailure;
                }
            }
        }

        /**
         * 事务已确认时普通 close 故障是否继续抛出，由调用方是否已有主异常决定；
         * 驱动 fatal 始终优先于 observer fatal。
         */
        private boolean finishCloseFailure(Throwable closeFailure) {
            VirtualMachineError driverFatal = directVirtualMachineError(closeFailure);
            if (!transactionOutcomeConfirmed && transactionOutcomeUnknownFailure == null) {
                if (driverFatal != null) {
                    throw driverFatal;
                }
                return false;
            }
            Error observerFatal = observeCloseFailure(
                    driverFatal == null ? closeFailure : driverFatal, transactionOutcomeConfirmed);
            if (driverFatal != null) {
                if (transactionOutcomeUnknownFailure != null) {
                    suppress(driverFatal, transactionOutcomeUnknownFailure);
                }
                if (observerFatal != null && observerFatal != driverFatal) {
                    suppress(driverFatal, observerFatal);
                }
                throw driverFatal;
            }
            if (observerFatal != null) {
                if (transactionOutcomeUnknownFailure != null) {
                    suppress(observerFatal, transactionOutcomeUnknownFailure);
                }
                suppress(observerFatal, closeFailure);
                throw observerFatal;
            }
            if (transactionOutcomeUnknownFailure != null) {
                suppress(transactionOutcomeUnknownFailure, closeFailure);
                return true;
            }
            return suppressConfirmedCloseFailure;
        }

        private Error observeCloseFailure(Throwable closeFailure, boolean outcomeConfirmed) {
            if (cleanupObserver == null) {
                return null;
            }
            try {
                cleanupObserver.onResourceCleanup(new ResourceCleanupObservation(
                        SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        outcomeConfirmed,
                        closeFailure));
                return null;
            } catch (Error observerFailure) {
                return observerFailure;
            }
        }

    }
}
