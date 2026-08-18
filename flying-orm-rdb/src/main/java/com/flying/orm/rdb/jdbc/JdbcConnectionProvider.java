package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

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
        Optional<JdbcTransactionContext> transaction = transactionParticipant.currentTransactionForExecution();
        if (transaction.isPresent()) {
            return JdbcConnectionLease.external(transaction.get());
        }
        return JdbcConnectionLease.owned(dataSource.getConnection(), cleanupObserver);
    }

    String currentRoutingIdentity() {
        String requestedRoutingIdentity = transactionParticipant.currentRoutingIdentity();
        // 使用和 SQL 获取连接相同的校验入口：有事务时只能相信已锁定的 context 路由；
        // 无事务时保留当前请求路由。这样元数据缓存不会先命中错误分区，再在执行 SQL 时才发现路由冲突。
        return transactionParticipant.currentTransaction(requestedRoutingIdentity)
                                     .map(JdbcTransactionContext::routingIdentity)
                                     .orElse(requestedRoutingIdentity);
    }

    Optional<JdbcTransactionContext> currentTransaction() {
        return transactionParticipant.currentTransactionForExecution();
    }

    /**
     * 一次 SQL 使用的连接租约。关闭外部租约是空操作，关闭自有租约才会把连接归还给连接池。
     *
     * <p>提交或回滚结果不确定时，自有连接必须先尝试物理失效且不得再归还连接池；外部事务
     * 连接始终由外部事务控制，不能由本租约失效或关闭。</p>
     */
    static final class JdbcConnectionLease implements AutoCloseable {

        private static final Executor DIRECT_EXECUTOR = Runnable::run;

        private final Connection connection;
        private final SqlTransactionSource transactionSource;
        private final boolean closeConnection;
        private final JdbcTransactionContext externalTransaction;
        private final SqlExecutionObserver cleanupObserver;
        private boolean discardConnection;
        private Throwable discardCause;
        private boolean transactionOutcomeConfirmed;
        private boolean suppressConfirmedCloseFailure;

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
            this.cleanupObserver = cleanupObserver;
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

        /**
         * 提交或回滚未确认后隔离自有连接。即使驱动不支持 abort，也绝不能再调用 close 把租约归还给池。
         */
        void discardAfterUncertainTransaction(Throwable uncertainty) {
            if (!closeConnection) {
                return;
            }
            discardConnection = true;
            discardCause = Objects.requireNonNull(uncertainty, "transaction uncertainty must not be null");
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

        @Override
        public void close() throws SQLException {
            if (!closeConnection) {
                return;
            }
            if (!discardConnection) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    discardConnection = true;
                    discardCause = closeFailure;
                    if (!finishCloseFailure(closeFailure)) {
                        throw closeFailure;
                    }
                } catch (RuntimeException | Error closeFailure) {
                    discardConnection = true;
                    discardCause = closeFailure;
                    if (!finishCloseFailure(closeFailure)) {
                        throw closeFailure;
                    }
                }
                return;
            }
            abortDiscardedConnection();
        }

        /**
         * 先完成物理隔离，再发布脱敏清理事实。事务已确认时普通 close 故障是否继续抛出，
         * 由调用方是否已有主异常决定；驱动 fatal 始终优先于 observer fatal。
         */
        private boolean finishCloseFailure(Throwable closeFailure) {
            VirtualMachineError abortFatal = null;
            try {
                abortDiscardedConnection();
            } catch (VirtualMachineError fatal) {
                abortFatal = fatal;
            }
            VirtualMachineError driverFatal = abortFatal == null
                    ? JdbcThrowableGraph.findVirtualMachineError(closeFailure) : abortFatal;
            if (!transactionOutcomeConfirmed) {
                if (driverFatal != null) {
                    throw driverFatal;
                }
                return false;
            }
            Error observerFatal = observeConfirmedCloseFailure(
                    driverFatal == null ? closeFailure : driverFatal);
            if (driverFatal != null) {
                if (observerFatal != null && observerFatal != driverFatal) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(driverFatal, observerFatal);
                }
                throw driverFatal;
            }
            if (observerFatal != null) {
                JdbcThrowableGraph.addSuppressedIfAcyclic(observerFatal, closeFailure);
                throw observerFatal;
            }
            return suppressConfirmedCloseFailure;
        }

        private Error observeConfirmedCloseFailure(Throwable closeFailure) {
            if (cleanupObserver == null) {
                return null;
            }
            try {
                cleanupObserver.onResourceCleanup(new ResourceCleanupObservation(
                        SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true,
                        closeFailure));
                return null;
            } catch (RuntimeException observerFailure) {
                return JdbcThrowableGraph.findVirtualMachineError(observerFailure);
            } catch (Error observerFailure) {
                VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(observerFailure);
                return fatal == null ? observerFailure : fatal;
            }
        }

        private void abortDiscardedConnection() {
            try {
                connection.abort(DIRECT_EXECUTOR);
            } catch (VirtualMachineError abortFailure) {
                if (abortFailure != discardCause) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(abortFailure, discardCause);
                }
                throw abortFailure;
            } catch (SQLException | RuntimeException | Error invalidationFailure) {
                VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(invalidationFailure);
                if (fatal != null) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(fatal, discardCause);
                    throw fatal;
                }
                if (invalidationFailure != discardCause) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(discardCause, invalidationFailure);
                }
            }
        }
    }
}
