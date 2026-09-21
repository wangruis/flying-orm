package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
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

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError;

/**
 * 在一条上层 JDBC 连接上执行受保护字段业务写入和 CONTAINS 侧索引维护。
 *
 * <p>连接由上层访问端口提供并释放，ORM 不控制事务或物理连接。
 * 多语句总时限由上层治理，ORM 不配置时间政策。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class JdbcProtectedWriteExecutor {

    /** owner 每行至少贡献一个参数；多读一行已经不可能生成可执行的受保护 UPDATE。 */
    private final JdbcSqlExecutor connections;
    private final JdbcExecutionObservationSupport observations;

    JdbcProtectedWriteExecutor(JdbcSqlExecutor connections,
                               JdbcExecutionObservationSupport observations) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
    }

    SqlWriteResult execute(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        JdbcProtectedWriteObservation observation = new JdbcProtectedWriteObservation(observations, safeWork);
        Connection connection = null;
        SqlRequest representative = safeWork.kind() == ProtectedWriteWork.Kind.UPDATE
                ? safeWork.ownerQuery() : safeWork.writeRequest();
        SqlWriteResult result = null;
        Throwable operationFailure = null;
        try {
            try {
                connection = connections.acquire(representative);
                result = execute(connection, safeWork, safeOptions, observation);
            } catch (SQLException | RuntimeException | Error error) {
                operationFailure = error;
                throw error;
            } finally {
                // 语句级清理完成后调用上层释放端口；释放错误也经过同一终态观测。
                connections.release(connection, representative, SqlExecutionOperation.UPDATE,
                        result != null, operationFailure);
            }
            observation.success(result);
            return result;
        } catch (SQLException | RuntimeException error) {
            throw observation.failure(error);
        }
    }

    private SqlWriteResult execute(Connection connection,
                                   ProtectedWriteWork work,
                                   SqlExecutionOptions options,
                                   JdbcProtectedWriteObservation observation) throws SQLException {
        try {
            List<Map<String, Object>> owners = work.kind() == ProtectedWriteWork.Kind.UPDATE
                    ? readOwners(connection, work, options)
                    : List.of(work.knownOwner());
            SqlWriteResult result = work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()
                    ? new SqlWriteResult(0L, List.of())
                    : write(connection, work,
                            work.kind() == ProtectedWriteWork.Kind.UPDATE
                                    ? work.writeRequestForOwners(owners) : work.writeRequest(),
                            options, observation);
            work.requireStableOwnerSet(owners, result);
            if (result.affectedRows() > 0L) {
                if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
                    owners = List.of(work.resolveInsertOwner(result));
                }
                replaceTokens(connection, work, owners);
            }
            return result;
        } catch (SQLException | RuntimeException | Error error) {
            // 在上层 release 前提升本执行单元的 TWR 清理错误，保持致命主错误身份。
            rethrowTryWithResourcesVirtualMachineError(error);
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
            JdbcStatementBinder.bind(statement, OwnedBindableValues.ownedValues(request.parameters()));
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
                                        SqlExecutionOptions options,
                                        JdbcProtectedWriteObservation observation) throws SQLException {
        boolean keys = work.requiresGeneratedKeys();
        try (PreparedStatement statement = keys
                ? connection.prepareStatement(request.sql(), new String[]{work.generatedOwnerField()})
                : connection.prepareStatement(request.sql())) {
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, OwnedBindableValues.ownedValues(request.parameters()));
            JdbcStatementControl.requireNotInterrupted(statement);
            long rows;
            try {
                rows = statement.executeLargeUpdate();
            } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
                rows = statement.executeUpdate();
            }
            observation.confirmedRows(rows);
            if (!keys) {
                return new SqlWriteResult(rows, List.of());
            }
            try (ResultSet generated = statement.getGeneratedKeys()) {
                return new SqlWriteResult(rows, JdbcResultSetReader.readGeneratedKeys(generated, options));
            } catch (SQLException | RuntimeException | Error failure) {
                rethrowTryWithResourcesVirtualMachineError(failure);
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
