package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.result.DynamicRow;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.commitUnknown;
import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.isUnknown;
import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.rollback;
import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.rollbackUnknown;

/**
 * 在一条原生 JDBC 连接上原子执行受保护字段业务写入和 CONTAINS 侧索引维护。
 *
 * <p>外部事务只借用连接，不提交、回滚或关闭；自有连接会显式开启事务。提交结果不确定或回滚无法确认时，
 * 租约会被标记为不可复用，避免未知事务连接回到连接池。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedWriteExecutor {

    /** owner 每行至少贡献一个参数；多读一行已经不可能生成可执行的受保护 UPDATE。 */
    private final JdbcConnectionProvider connections;
    private final JdbcExecutionObservationSupport observations;

    JdbcProtectedWriteExecutor(JdbcConnectionProvider connections,
                               JdbcExecutionObservationSupport observations) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
    }

    SqlWriteResult execute(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        JdbcProtectedWriteObservation observation = new JdbcProtectedWriteObservation(observations, safeWork);
        JdbcConnectionProvider.JdbcConnectionLease lease = null;
        SqlWriteResult result = null;
        Throwable operationFailure = null;
        try {
            lease = connections.acquire();
            observation.transactionSource(lease.transactionSource());
            result = execute(lease, safeWork, safeOptions);
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            throw observation.failure(error);
        } finally {
            if (lease != null) {
                // 提交结果已确认后，连接归还故障只进入资源观测；致命清理错误仍由统一清理器提升。
                JdbcResources.close(
                        SqlExecutionOperation.UPDATE,
                        result != null, operationFailure, observations, lease);
            }
        }
        observation.success(result);
        return result;
    }

    private SqlWriteResult execute(JdbcConnectionProvider.JdbcConnectionLease lease,
                                   ProtectedWriteWork work,
                                   SqlExecutionOptions options) throws SQLException {
        Connection connection = lease.connection();
        boolean external = lease.transactionSource() == SqlTransactionSource.EXTERNAL;
        boolean transactionStarted = external;
        JdbcProtectedWriteDeadline deadline = JdbcProtectedWriteDeadline.start(options);
        try {
            if (!external) {
                transactionStarted = true;
                try {
                    if (connection.getAutoCommit()) {
                        connection.setAutoCommit(false);
                    }
                } catch (SQLException | RuntimeException | Error stateFailure) {
                    throw stateFailure;
                }
            }
            List<Map<String, Object>> owners = work.kind() == ProtectedWriteWork.Kind.UPDATE
                    ? readOwners(connection, work, deadline.remainingOptions())
                    : List.of(work.knownOwner());
            SqlWriteResult result = work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()
                    ? new SqlWriteResult(0L, List.of())
                    : write(connection, work,
                            work.kind() == ProtectedWriteWork.Kind.UPDATE
                                    ? work.writeRequestForOwners(owners) : work.writeRequest(),
                            deadline.remainingOptions());
            work.requireStableOwnerSet(owners, result);
            if (result.affectedRows() > 0L) {
                if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
                    owners = List.of(work.resolveInsertOwner(result));
                }
                replaceTokens(connection, work, owners, deadline);
            }
            deadline.requireRemaining();
            if (!external) {
                JdbcStatementControl.requireNotInterrupted();
                try {
                    connection.commit();
                } catch (SQLException | RuntimeException | Error commitFailure) {
                    throw commitUnknown(commitFailure);
                }
            }
            return result;
        } catch (VirtualMachineError fatal) {
            JdbcProtectedWriteTransactions.RollbackResult rollback = rollback(
                    connection, external, transactionStarted, fatal);
            if (rollback.fatal() != null) {
                throw rollback.fatal();
            }
            if (!rollback.confirmed() && !isUnknown(fatal)) {
                RdbException unknown = rollbackUnknown(fatal);
                throw unknown;
            }
            throw fatal;
        } catch (SQLException | RuntimeException | Error error) {
            JdbcProtectedWriteTransactions.RollbackResult rollback = rollback(
                    connection, external, transactionStarted, error);
            if (rollback.fatal() != null) {
                throw rollback.fatal();
            }
            if (!rollback.confirmed() && !isUnknown(error)) {
                RdbException unknown = rollbackUnknown(error);
                RuntimeException unresolved = error instanceof GeneratedKeyReadException keyFailure
                        ? new GeneratedKeyReadException(keyFailure.affectedRows(), unknown)
                        : unknown;
                throw unresolved;
            }
            if (!external && rollback.confirmed() && error instanceof GeneratedKeyReadException keyFailure) {
                rethrowGeneratedKeyCause(keyFailure);
            }
            throw error;
        }
    }

    private static List<Map<String, Object>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         SqlExecutionOptions options) throws SQLException {
        SqlRequest request = work.ownerQuery();
        SqlExecutionOptions ownerReadOptions = ProtectedWriteWork.ownerReadOptions(options);
        List<DynamicRow> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(request.sql())) {
            JdbcStatementOptions.apply(statement, ownerReadOptions);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                JdbcResultSetReader.readQueryRows(
                        resultSet, statement, SqlStatementType.SELECT, ownerReadOptions, rows);
            }
        }
        return rows.stream().map(work::ownerFrom).toList();
    }

    private static SqlWriteResult write(Connection connection,
                                        ProtectedWriteWork work,
                                        SqlRequest request,
                                        SqlExecutionOptions options) throws SQLException {
        boolean keys = work.requiresGeneratedKeys();
        try (PreparedStatement statement = keys
                ? connection.prepareStatement(request.sql(), new String[]{work.generatedOwnerField()})
                : connection.prepareStatement(request.sql())) {
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            long rows;
            try {
                rows = statement.executeLargeUpdate();
            } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
                rows = statement.executeUpdate();
            }
            if (!keys) {
                return new SqlWriteResult(rows, List.of());
            }
            try (ResultSet generated = statement.getGeneratedKeys()) {
                return new SqlWriteResult(rows, JdbcResultSetReader.readGeneratedKeys(generated, options));
            } catch (SQLException | RuntimeException | Error failure) {
                throw new GeneratedKeyReadException(rows, failure);
            }
        }
    }

    private static void rethrowGeneratedKeyCause(GeneratedKeyReadException failure) throws SQLException {
        Throwable cause = failure.getCause();
        if (cause instanceof SQLException sqlFailure) {
            throw sqlFailure;
        }
        if (cause instanceof RuntimeException runtimeFailure) {
            throw runtimeFailure;
        }
        throw (Error) cause;
    }

    private static void replaceTokens(Connection connection,
                                      ProtectedWriteWork work,
                                      List<Map<String, Object>> owners,
                                      JdbcProtectedWriteDeadline deadline) throws SQLException {
        try {
            if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
                JdbcProtectedBatchSideIndex.insertOwners(
                        connection, work, owners, deadline.batchDeadline());
            } else {
                JdbcProtectedBatchSideIndex.replaceOwners(
                        connection, work, owners, deadline.batchDeadline());
            }
        } catch (java.util.concurrent.TimeoutException expired) {
            throw deadline.timeout(expired);
        }
    }

}
