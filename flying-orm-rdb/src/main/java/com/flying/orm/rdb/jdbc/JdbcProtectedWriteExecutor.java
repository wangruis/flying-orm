package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;

/**
 * 在一条原生 JDBC 连接上原子执行受保护字段业务写入和 CONTAINS 侧索引维护。
 *
 * <p>只借用上层外部事务连接，不自开事务、不提交、不回滚、不关闭外部连接。
 * 多语句总时限由上层治理，旧正 timeout 配置在入口明确拒绝，不改成每条语句分别计时。</p>
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
        if (!safeOptions.timeout().isZero()) {
            throw new UnsupportedOperationException("protected multi-statement write timeout must be managed by the caller");
        }
        JdbcTransactionContext transaction = connections.currentTransaction().orElseThrow(
                () -> new IllegalStateException("atomic protected write requires an external jdbc transaction"));
        JdbcProtectedWriteObservation observation = new JdbcProtectedWriteObservation(observations, safeWork);
        JdbcConnectionProvider.JdbcConnectionLease lease = null;
        SqlWriteResult result = null;
        Throwable operationFailure = null;
        try {
            lease = JdbcConnectionProvider.JdbcConnectionLease.external(transaction);
            observation.transactionSource(lease.transactionSource());
            result = execute(lease, safeWork, safeOptions);
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            rethrowVirtualMachineError(error);
            throw observation.failure(error);
        } finally {
            if (lease != null) {
                // 正常资源收尾仍保留；借用租约不关闭上层拥有的事务连接。
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
        List<Map<String, Object>> owners = work.kind() == ProtectedWriteWork.Kind.UPDATE
                ? readOwners(connection, work, options)
                : List.of(work.knownOwner());
        SqlWriteResult result = work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()
                ? new SqlWriteResult(0L, List.of())
                : write(connection, work,
                        work.kind() == ProtectedWriteWork.Kind.UPDATE
                                ? work.writeRequestForOwners(owners) : work.writeRequest(),
                        options);
        work.requireStableOwnerSet(owners, result);
        if (result.affectedRows() > 0L) {
            if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
                owners = List.of(work.resolveInsertOwner(result));
            }
            replaceTokens(connection, work, owners);
        }
        return result;
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
                rethrowVirtualMachineError(failure);
                throw new GeneratedKeyReadException(rows, failure);
            }
        }
    }

    private static void replaceTokens(Connection connection,
                                      ProtectedWriteWork work,
                                      List<Map<String, Object>> owners) throws SQLException {
        if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
            JdbcProtectedBatchSideIndex.insertOwners(connection, work, owners);
        } else {
            JdbcProtectedBatchSideIndex.replaceOwners(connection, work, owners);
        }
    }

}
