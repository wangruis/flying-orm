package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.commitUnknown;
import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.isUnknown;
import static com.flying.orm.rdb.jdbc.JdbcProtectedWriteTransactions.restoreAutoCommit;
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
        boolean restoreAutoCommit = false;
        boolean transactionStarted = external;
        boolean transactionOutcomeKnown = external;
        Throwable failure = null;
        JdbcProtectedWriteDeadline deadline = JdbcProtectedWriteDeadline.start(options);
        try {
            if (!external) {
                transactionStarted = true;
                try {
                    if (connection.getAutoCommit()) {
                        connection.setAutoCommit(false);
                        restoreAutoCommit = true;
                    }
                } catch (SQLException | RuntimeException | Error stateFailure) {
                    // 驱动连事务状态都无法可靠读取或切换时，不得把该自有连接重新放回连接池。
                    lease.discardAfterUncertainTransaction(stateFailure);
                    throw stateFailure;
                }
            }
            List<Map<String, Object>> owners = work.kind() == ProtectedWriteWork.Kind.UPDATE
                    ? readOwners(connection, work, deadline.remainingOptions())
                    : List.of(new LinkedHashMap<>(work.knownOwner()));
            SqlWriteResult result = work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()
                    ? new SqlWriteResult(0L, List.of())
                    : write(connection, work,
                            work.kind() == ProtectedWriteWork.Kind.UPDATE
                                    ? work.writeRequestForOwners(owners) : work.writeRequest(),
                            deadline.remainingOptions());
            requireStableOwnerSet(work, owners, result);
            if (result.affectedRows() > 0L) {
                if (work.kind() == ProtectedWriteWork.Kind.INSERT) {
                    owners = List.of(resolveInsertOwner(work, result));
                }
                replaceTokens(connection, work, owners, deadline);
            }
            deadline.requireRemaining();
            if (!external) {
                try {
                    connection.commit();
                    transactionOutcomeKnown = true;
                } catch (SQLException | RuntimeException | Error commitFailure) {
                    lease.discardAfterUncertainTransaction(commitFailure);
                    throw commitUnknown(commitFailure);
                }
            }
            return result;
        } catch (VirtualMachineError fatal) {
            failure = fatal;
            JdbcProtectedWriteTransactions.RollbackResult rollback = rollback(
                    connection, lease, external, transactionStarted, fatal);
            transactionOutcomeKnown = rollback.confirmed();
            if (rollback.fatal() != null) {
                failure = rollback.fatal();
                throw rollback.fatal();
            }
            if (!rollback.confirmed() && !isUnknown(fatal)) {
                RdbException unknown = rollbackUnknown(fatal);
                failure = unknown;
                throw unknown;
            }
            throw fatal;
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            JdbcProtectedWriteTransactions.RollbackResult rollback = rollback(
                    connection, lease, external, transactionStarted, error);
            transactionOutcomeKnown = rollback.confirmed();
            if (rollback.fatal() != null) {
                failure = rollback.fatal();
                throw rollback.fatal();
            }
            if (!rollback.confirmed() && !isUnknown(error)) {
                RdbException unknown = rollbackUnknown(error);
                RuntimeException unresolved = error instanceof GeneratedKeyReadException keyFailure
                        ? new GeneratedKeyReadException(keyFailure.affectedRows(), unknown)
                        : unknown;
                failure = unresolved;
                throw unresolved;
            }
            if (!external && rollback.confirmed() && error instanceof GeneratedKeyReadException keyFailure) {
                failure = keyFailure.getCause();
                rethrowGeneratedKeyCause(keyFailure);
            }
            throw error;
        } finally {
            if (!external && restoreAutoCommit && transactionOutcomeKnown) {
                Throwable restoreFailure = restoreAutoCommit(connection, lease, failure);
                if (restoreFailure != null) {
                    observations.cleanupFailure(
                            SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                            true,
                            restoreFailure);
                }
            }
        }
    }

    private static List<Map<String, Object>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         SqlExecutionOptions options) throws SQLException {
        SqlRequest request = work.ownerQuery();
        List<DynamicRow> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(request.sql())) {
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                JdbcResultSetReader.readQueryRows(
                        resultSet, statement, SqlStatementType.SELECT, options, rows);
            }
        }
        return rows.stream().map(row -> owner(work.ownerFields(), row)).toList();
    }

    private static SqlWriteResult write(Connection connection,
                                        ProtectedWriteWork work,
                                        SqlRequest request,
                                        SqlExecutionOptions options) throws SQLException {
        boolean keys = work.requiresGeneratedKeys();
        try (PreparedStatement statement = keys
                ? connection.prepareStatement(request.sql(), new String[]{generatedKeyColumn(work)})
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

    private static String generatedKeyColumn(ProtectedWriteWork work) {
        Map<String, Object> knownOwner = work.knownOwner();
        List<String> missing = work.ownerFields().stream()
                                   .filter(field -> !knownOwner.containsKey(field) || knownOwner.get(field) == null)
                                   .toList();
        if (missing.size() != 1) {
            throw new IllegalArgumentException("protected insert requires exactly one generated owner key");
        }
        return missing.getFirst();
    }

    private static void requireStableOwnerSet(ProtectedWriteWork work,
                                              List<Map<String, Object>> owners,
                                              SqlWriteResult result) {
        if (work.kind() == ProtectedWriteWork.Kind.UPDATE && result.affectedRows() != owners.size()) {
            throw new IllegalStateException("protected update row set changed concurrently");
        }
    }

    private static Map<String, Object> resolveInsertOwner(ProtectedWriteWork work, SqlWriteResult result) {
        Map<String, Object> owner = new LinkedHashMap<>(work.knownOwner());
        List<String> missing = work.ownerFields().stream()
                .filter(field -> !owner.containsKey(field) || owner.get(field) == null)
                .toList();
        if (missing.isEmpty()) {
            return Map.copyOf(owner);
        }
        if (result.generatedKeys().size() != 1 || missing.size() != 1) {
            throw new IllegalStateException("protected insert did not return one complete owner key");
        }
        DynamicRow generated = result.generatedKeys().getFirst();
        Object value = generated.containsKey(missing.getFirst())
                ? generated.get(missing.getFirst())
                : generated.value(0);
        owner.put(missing.getFirst(), Objects.requireNonNull(
                value, "protected insert generated owner key must not be null"));
        return Map.copyOf(owner);
    }

    private static void replaceTokens(Connection connection,
                                      ProtectedWriteWork work,
                                      List<Map<String, Object>> owners,
                                      JdbcProtectedWriteDeadline deadline) throws SQLException {
        for (Map<String, Object> owner : owners) {
            for (ProtectedWriteWork.FieldTokens field : work.fields()) {
                if (work.kind() != ProtectedWriteWork.Kind.INSERT) {
                    executeTokenUpdate(
                            connection, work.deleteSql(), ownerValues(work, owner, field, null),
                            deadline.remainingOptions(), false);
                }
                for (byte[] token : field.tokens()) {
                    executeTokenUpdate(
                            connection, work.insertSql(), ownerValues(work, owner, field, token),
                            deadline.remainingOptions(), true);
                }
            }
        }
    }

    private static List<Object> ownerValues(ProtectedWriteWork work,
                                            Map<String, Object> owner,
                                            ProtectedWriteWork.FieldTokens field,
                                            byte[] token) {
        List<Object> values = new ArrayList<>(work.ownerFields().size() + 2);
        work.ownerFields().forEach(name -> values.add(Objects.requireNonNull(
                owner.get(name), "protected write owner value must not be null")));
        values.add(field.fieldTag());
        if (token != null) {
            values.add(token);
        }
        return values;
    }

    private static void executeTokenUpdate(Connection connection,
                                           String sql,
                                           List<Object> parameters,
                                           SqlExecutionOptions options,
                                           boolean requireOneRow) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, parameters);
            JdbcStatementControl.requireNotInterrupted(statement);
            int affectedRows = statement.executeUpdate();
            if (requireOneRow && affectedRows != 1) {
                throw new IllegalStateException("protected side index insert must affect one row");
            }
        }
    }

    private static Map<String, Object> owner(List<String> fields, DynamicRow row) {
        Map<String, Object> owner = new LinkedHashMap<>();
        for (int index = 0; index < fields.size(); index++) {
            owner.put(fields.get(index), row.value(index));
        }
        return Map.copyOf(owner);
    }

}
