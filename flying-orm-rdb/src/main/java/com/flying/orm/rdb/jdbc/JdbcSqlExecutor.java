package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 原生 JDBC 同步 SQL 执行器。
 *
 * <p>它直接使用 {@link DataSource}、{@link PreparedStatement} 和 {@link ResultSet}，不会创建 Reactor 对象，
 * 也不会调用 R2DBC。实例只保存不可变配置，可以作为单例并发共享；连接、语句、结果集全部局限在一次调用内，
 * 并按 JDBC 的逆序资源规则关闭。</p>
 *
 * <p>查询会在读取每一行时检查行数和估算内存，不会先收完整个结果再判断。JDBC 的 query timeout 只能精确到秒，
 * 小于一秒的正超时会向上取整成一秒；连接池等待时间仍由 DataSource/连接池配置负责。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class JdbcSqlExecutor implements SyncSqlExecutor {

    private final JdbcConnectionProvider connections;
    private final SqlExecutionOptions defaultOptions;
    private final SqlExecutionObserver observer;
    private final JdbcExecutionObservationSupport observations;

    private JdbcSqlExecutor(JdbcConnectionProvider connections,
                            SqlExecutionOptions defaultOptions,
                            SqlExecutionObserver observer) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "sql execution options must not be null");
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.observations = JdbcExecutionObservationSupport.create(observer);
    }

    /** 创建使用保守执行保护和空 observer 的原生 JDBC 执行器。 */
    public static JdbcSqlExecutor create(DataSource dataSource) {
        return new JdbcSqlExecutor(new JdbcConnectionProvider(dataSource, JdbcTransactionParticipant.none()),
                                   SqlExecutionOptions.safeDefaults(), SqlExecutionObserver.noop());
    }

    /** 返回共享同一 DataSource、使用新默认保护的不可变执行器。 */
    public JdbcSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return new JdbcSqlExecutor(connections, options, observer);
    }

    /** 返回共享同一 DataSource、把执行事实交给新 observer 的不可变执行器。 */
    public JdbcSqlExecutor withObserver(SqlExecutionObserver observer) {
        return new JdbcSqlExecutor(connections, defaultOptions, observer);
    }

    /**
     * 接入上层已经绑定的 JDBC 事务连接。参与者按调用读取当前事务，执行器实例仍可并发共享。
     */
    public JdbcSqlExecutor withTransactionParticipant(JdbcTransactionParticipant participant) {
        return new JdbcSqlExecutor(connections.withTransactionParticipant(participant), defaultOptions, observer);
    }

    /**
     * 返回 JDBC 专有能力入口。它和当前执行器共享 DataSource、执行保护、观测器及外部事务参与者。
     */
    public JdbcAdvancedOperations advanced() {
        return new JdbcAdvancedOperations(connections, defaultOptions, observations);
    }

    @InternalApi
    @Override
    public String metadataCachePartition() {
        return connections.currentRoutingIdentity();
    }

    @Override
    public List<DynamicRow> query(SqlRequest request) {
        return query(request, defaultOptions);
    }

    @Override
    public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        long startedAt = System.nanoTime();
        List<DynamicRow> rows = new ArrayList<>();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            executeQuery(lease, safeRequest, safeOptions, rows);
            observations.success(SqlExecutionOperation.QUERY, safeRequest, rows.size(), startedAt, transactionSource);
            return Collections.unmodifiableList(rows);
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.QUERY, safeRequest, rows.size(), startedAt, translated,
                                 transactionSource);
            throw translated;
        }
    }

    @Override
    public long rowsUpdated(SqlRequest request) {
        return rowsUpdated(request, defaultOptions);
    }

    @Override
    public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        long startedAt = System.nanoTime();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            long rows = executeUpdate(lease, safeRequest, safeOptions);
            observations.success(SqlExecutionOperation.UPDATE, safeRequest, rows, startedAt, transactionSource);
            return rows;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.UPDATE, safeRequest, 0L, startedAt, translated,
                                 transactionSource);
            throw translated;
        }
    }

    @Override
    public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        long startedAt = System.nanoTime();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            SqlWriteResult result = executeUpdateReturningKeys(lease, safeRequest, safeOptions);
            observations.success(SqlExecutionOperation.UPDATE, safeRequest, result.affectedRows(), startedAt,
                                 transactionSource);
            return result;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.UPDATE, safeRequest, 0L, startedAt, translated,
                                 transactionSource);
            throw translated;
        }
    }

    @InternalApi
    @Override
    public SqlWriteResult atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        return new JdbcProtectedWriteExecutor(connections, observations).execute(work, options);
    }

    private void executeQuery(JdbcConnectionProvider.JdbcConnectionLease lease,
                              SqlRequest request,
                              SqlExecutionOptions options,
                              List<DynamicRow> result) throws SQLException {
        SqlStatementType statementType = SqlStatementType.fromSql(request.sql());
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        try {
            connection = lease.connection();
            statement = connection.prepareStatement(request.sql());
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            resultSet = statement.executeQuery();
            JdbcResultSetReader.readQueryRows(resultSet, statement, statementType, options, result);
            outcomeConfirmed = true;
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            throw error;
        } finally {
            discardConnectionFailure(lease, operationFailure);
            JdbcResources.close(SqlExecutionOperation.QUERY, outcomeConfirmed, operationFailure,
                                observations, resultSet, statement, lease);
        }
    }

    private long executeUpdate(JdbcConnectionProvider.JdbcConnectionLease lease,
                               SqlRequest request,
                               SqlExecutionOptions options) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        try {
            connection = lease.connection();
            statement = connection.prepareStatement(request.sql());
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            long affectedRows;
            try {
                affectedRows = statement.executeLargeUpdate();
            } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
                affectedRows = statement.executeUpdate();
            }
            outcomeConfirmed = true;
            return affectedRows;
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            throw error;
        } finally {
            discardConnectionFailure(lease, operationFailure);
            JdbcResources.close(SqlExecutionOperation.UPDATE, outcomeConfirmed, operationFailure,
                                observations, statement, lease);
        }
    }

    private SqlWriteResult executeUpdateReturningKeys(JdbcConnectionProvider.JdbcConnectionLease lease,
                                                       SqlRequest request,
                                                       SqlExecutionOptions options) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet generatedKeys = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        try {
            connection = lease.connection();
            statement = connection.prepareStatement(request.sql(), Statement.RETURN_GENERATED_KEYS);
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, request.parameters());
            JdbcStatementControl.requireNotInterrupted(statement);
            long affectedRows;
            try {
                affectedRows = statement.executeLargeUpdate();
            } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
                affectedRows = statement.executeUpdate();
            }
            generatedKeys = statement.getGeneratedKeys();
            List<DynamicRow> keys = JdbcResultSetReader.readGeneratedKeys(generatedKeys, options);
            outcomeConfirmed = true;
            return new SqlWriteResult(affectedRows, keys);
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            throw error;
        } finally {
            discardConnectionFailure(lease, operationFailure);
            JdbcResources.close(SqlExecutionOperation.UPDATE, outcomeConfirmed, operationFailure,
                                observations, generatedKeys, statement, lease);
        }
    }

    private static void discardConnectionFailure(JdbcConnectionProvider.JdbcConnectionLease lease,
                                                 Throwable operationFailure) {
        if (operationFailure == null) {
            return;
        }
        RuntimeException translated = RdbExceptionTranslator.translate(operationFailure);
        if (translated instanceof RdbException rdbError && rdbError.kind() == RdbErrorKind.CONNECTION) {
            lease.discardAfterUncertainTransaction(operationFailure);
        }
    }
}
