package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.directVirtualMachineError;

import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.rdb.mapping.RowMapper;
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
    private final RdbDialect dialect;
    private final JdbcExecutionObservationSupport observations;
    private JdbcSqlExecutor(JdbcConnectionProvider connections,
                            SqlExecutionOptions defaultOptions,
                            SqlExecutionObserver observer,
                            RdbDialect dialect) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "sql execution options must not be null");
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.dialect = dialect;
        this.observations = JdbcExecutionObservationSupport.create(observer);
    }

    /** 创建使用保守执行保护和空 observer 的原生 JDBC 执行器。 */
    public static JdbcSqlExecutor create(DataSource dataSource) {
        return new JdbcSqlExecutor(new JdbcConnectionProvider(dataSource, JdbcTransactionParticipant.none()),
                                   SqlExecutionOptions.safeDefaults(), SqlExecutionObserver.noop(), null);
    }

    /** 启动装配已确定方言时创建执行器；该内部入口避免在 SQL 热路径重新读取连接元数据。 */
    @InternalApi
    public static JdbcSqlExecutor create(DataSource dataSource, RdbDialect dialect) {
        return new JdbcSqlExecutor(new JdbcConnectionProvider(dataSource, JdbcTransactionParticipant.none()),
                                   SqlExecutionOptions.safeDefaults(), SqlExecutionObserver.noop(),
                                   Objects.requireNonNull(dialect, "RDB dialect must not be null"));
    }

    /** 返回共享同一 DataSource、使用新默认保护的不可变执行器。 */
    public JdbcSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return new JdbcSqlExecutor(connections, options, observer, dialect);
    }

    /** 返回共享同一 DataSource、把执行事实交给新 observer 的不可变执行器。 */
    public JdbcSqlExecutor withObserver(SqlExecutionObserver observer) {
        return new JdbcSqlExecutor(connections, defaultOptions, observer, dialect);
    }

    /**
     * 接入上层已经绑定的 JDBC 事务连接。参与者按调用读取当前事务，执行器实例仍可并发共享。
     */
    public JdbcSqlExecutor withTransactionParticipant(JdbcTransactionParticipant participant) {
        return new JdbcSqlExecutor(
                connections.withTransactionParticipant(participant), defaultOptions, observer, dialect);
    }

    /**
     * 返回 JDBC 专有能力入口。它和当前执行器共享 DataSource、执行保护、观测器及外部事务参与者。
     */
    public JdbcAdvancedOperations advanced() {
        return new JdbcAdvancedOperations(connections, defaultOptions, observations, this);
    }

    @InternalApi
    @Override
    public java.util.Optional<com.flying.orm.rdb.transaction.JdbcTransactionContext> currentTransaction() {
        return connections.currentTransaction();
    }

    @Override
    public List<DynamicRow> query(SqlRequest request) {
        return query(request, defaultOptions);
    }

    @Override
    public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return Collections.unmodifiableList(queryOwned(request, options, 0));
    }

    private List<DynamicRow> queryOwned(SqlRequest request,
                                        SqlExecutionOptions options,
                                        int rowLimit) {
        if (rowLimit < 0) {
            throw new IllegalArgumentException("query row limit must not be negative");
        }
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> executionParameters = snapshotExecutionParameters(safeRequest);
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        String executionSql = executionSql(safeRequest);
        SqlStatementType statementType = observations.statementType(
                safeRequest, safeOptions.maxRows() > 0 || safeOptions.maxResultBytes() > 0);
        long startedAt = observations.startedAt();
        List<DynamicRow> rows = new ArrayList<>();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            executeQuery(lease, executionSql, executionParameters, statementType, safeOptions, rows, rowLimit);
            observations.success(SqlExecutionOperation.QUERY, safeRequest, executionParameters, statementType,
                                 rows.size(), startedAt, transactionSource);
            return rows;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.QUERY, safeRequest, executionParameters, statementType,
                                 rows.size(), startedAt, translated, transactionSource);
            throw translated;
        }
    }

    @InternalApi
    @Override
    public <T> List<T> queryMapped(SqlRequest request,
                                   SqlExecutionOptions options,
                                   RowMapper<T> mapper,
                                   int rowLimit) {
        if (rowLimit < 0) {
            throw new IllegalArgumentException("mapped query row limit must not be negative");
        }
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        List<DynamicRow> rows = queryOwned(
                request, options == null ? defaultOptions : options, rowLimit);
        return mapOwnedRows(rows, safeMapper);
    }

    @Override
    public long rowsUpdated(SqlRequest request) {
        return rowsUpdated(request, defaultOptions);
    }

    @Override
    public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> executionParameters = snapshotExecutionParameters(safeRequest);
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        String executionSql = executionSql(safeRequest);
        SqlStatementType statementType = observations.statementType(safeRequest, false);
        long startedAt = observations.startedAt();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            long rows = executeUpdate(lease, executionSql, executionParameters, safeOptions);
            observations.success(SqlExecutionOperation.UPDATE, safeRequest, executionParameters, statementType,
                                 rows, startedAt, transactionSource);
            return rows;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.UPDATE, safeRequest, executionParameters, statementType,
                                 0L, startedAt, translated, transactionSource);
            throw translated;
        }
    }

    @Override
    public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return rowsUpdatedReturningKeys(request, options, null);
    }

    @Override
    @InternalApi
    public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request,
                                                   SqlExecutionOptions options,
                                                   String generatedKeyColumn) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> executionParameters = snapshotExecutionParameters(safeRequest);
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        String executionSql = executionSql(safeRequest);
        SqlStatementType statementType = observations.statementType(safeRequest, false);
        long startedAt = observations.startedAt();
        SqlTransactionSource transactionSource = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            transactionSource = lease.transactionSource();
            SqlWriteResult result = executeUpdateReturningKeys(
                    lease, executionSql, executionParameters, safeOptions, generatedKeyColumn);
            observations.success(SqlExecutionOperation.UPDATE, safeRequest, executionParameters, statementType,
                                 result.affectedRows(), startedAt, transactionSource);
            return result;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.UPDATE, safeRequest, executionParameters, statementType,
                                 0L, startedAt, translated, transactionSource);
            throw translated;
        }
    }

    @InternalApi
    @Override
    public SqlWriteResult atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        return new JdbcProtectedWriteExecutor(connections, observations).execute(work, options);
    }

    /** 请求已拥有普通参数；这里只递归冻结带类型包装器内部仍可变的 LOB 载荷。 */
    static List<Object> snapshotExecutionParameters(SqlRequest request) {
        List<Object> source = request.parameters();
        List<Object> snapshot = null;
        for (int index = 0; index < source.size(); index++) {
            Object value = source.get(index);
            Object owned = snapshotParameter(value);
            if (snapshot != null) {
                snapshot.add(owned);
            } else if (owned != value) {
                snapshot = new ArrayList<>(source.size());
                snapshot.addAll(source.subList(0, index));
                snapshot.add(owned);
            }
        }
        return snapshot == null ? source : Collections.unmodifiableList(snapshot);
    }

    private static Object snapshotParameter(Object value) {
        if (value instanceof SqlTypedValue typedValue) {
            Object payload = typedValue.value();
            Object owned = BindableValueSnapshots.immutableValue(payload);
            return owned == payload ? typedValue : new SqlTypedValue(typedValue.kind(), owned);
        }
        return value;
    }

    private void executeQuery(JdbcConnectionProvider.JdbcConnectionLease lease,
                              String executionSql,
                              List<Object> executionParameters,
                              SqlStatementType statementType,
                              SqlExecutionOptions options,
                              List<DynamicRow> result,
                              int rowLimit) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        try {
            connection = lease.connection();
            statement = connection.prepareStatement(executionSql);
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, executionParameters);
            JdbcStatementControl.requireNotInterrupted(statement);
            resultSet = statement.executeQuery();
            JdbcResultSetReader.readQueryRows(
                    resultSet, statement, statementType, options, result, rowLimit);
            outcomeConfirmed = true;
        } catch (SQLException | RuntimeException | Error error) {
            operationFailure = error;
            throw error;
        } finally {
            JdbcResources.close(SqlExecutionOperation.QUERY, outcomeConfirmed, operationFailure,
                                observations, resultSet, statement, lease);
        }
    }

    /** SQL 成功且资源关闭后，复用唯一自有 ArrayList 的槽位保存映射结果。 */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static <T> List<T> mapOwnedRows(List<DynamicRow> rows, RowMapper<T> mapper) {
        List ownedRows = rows;
        for (int index = 0; index < ownedRows.size(); index++) {
            DynamicRow row = (DynamicRow) ownedRows.get(index);
            ownedRows.set(index, mapper.map(row));
        }
        return Collections.unmodifiableList((List<T>) ownedRows);
    }


    private long executeUpdate(JdbcConnectionProvider.JdbcConnectionLease lease,
                               String executionSql,
                               List<Object> executionParameters,
                               SqlExecutionOptions options) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        try {
            connection = lease.connection();
            statement = connection.prepareStatement(executionSql);
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, executionParameters);
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
            JdbcResources.close(SqlExecutionOperation.UPDATE, outcomeConfirmed, operationFailure,
                                observations, statement, lease);
        }
    }

    private SqlWriteResult executeUpdateReturningKeys(JdbcConnectionProvider.JdbcConnectionLease lease,
                                                       String executionSql,
                                                       List<Object> executionParameters,
                                                       SqlExecutionOptions options,
                                                       String generatedKeyColumn) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet generatedKeys = null;
        Throwable operationFailure = null;
        boolean outcomeConfirmed = false;
        boolean writeCompleted = false;
        long affectedRows = 0L;
        try {
            connection = lease.connection();
            statement = generatedKeyColumn == null
                    ? connection.prepareStatement(executionSql, Statement.RETURN_GENERATED_KEYS)
                    : connection.prepareStatement(executionSql, new String[]{generatedKeyColumn});
            JdbcStatementOptions.apply(statement, options);
            JdbcStatementBinder.bind(statement, executionParameters);
            JdbcStatementControl.requireNotInterrupted(statement);
            try {
                affectedRows = statement.executeLargeUpdate();
            } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
                affectedRows = statement.executeUpdate();
            }
            writeCompleted = true;
            generatedKeys = statement.getGeneratedKeys();
            List<DynamicRow> keys = JdbcResultSetReader.readGeneratedKeys(generatedKeys, options);
            outcomeConfirmed = true;
            return new SqlWriteResult(affectedRows, keys);
        } catch (SQLException | RuntimeException | Error error) {
            VirtualMachineError fatal = directVirtualMachineError(error);
            if (fatal != null) {
                operationFailure = fatal;
                throw fatal;
            }
            if (writeCompleted && !(error instanceof GeneratedKeyReadException)) {
                GeneratedKeyReadException resolution = new GeneratedKeyReadException(affectedRows, error);
                operationFailure = resolution;
                throw resolution;
            }
            operationFailure = error;
            throw error;
        } finally {
            JdbcResources.close(SqlExecutionOperation.UPDATE, outcomeConfirmed, operationFailure,
                                observations, generatedKeys, statement, lease);
        }
    }

    void requireSingle(SqlRequest request) {
        executionSql(request);
    }

    private String executionSql(SqlRequest request) {
        return SqlExecutionStatements.canonical(request, dialect == null ? "" : dialect.name());
    }

}
