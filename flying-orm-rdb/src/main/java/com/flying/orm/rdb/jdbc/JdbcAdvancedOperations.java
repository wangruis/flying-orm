package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * JDBC 才有的高级能力入口。
 *
 * <p>普通 CRUD 不需要接触这里。只有确实需要滚动 {@link ResultSet}、读取 {@link DatabaseMetaData}，或调用
 * 保存点等标准 JDBC 连接能力时才使用它。每次调用仍从 {@link JdbcConnectionProvider} 取得租约：外部事务连接
 * 只借用，自有连接在回调结束后归还连接池。</p>
 *
 * <p>{@link #transaction(JdbcCallback)} 会把当前事务连接的受保护视图交给回调。保存点仍可正常创建、回滚和释放，
 * 但提交、整笔回滚、关闭、切换自动提交以及 {@code unwrap} 到原始连接都会被拒绝。这样即使回调代码写错，
 * 也不会越过上层事务管理器结束或归还它持有的连接。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcAdvancedOperations {
    private final JdbcConnectionProvider connections;
    private final SqlExecutionOptions defaultOptions;
    private final JdbcExecutionObservationSupport observations;
    private final JdbcSqlExecutor requestValidator;

    JdbcAdvancedOperations(JdbcConnectionProvider connections,
                           SqlExecutionOptions defaultOptions,
                           JdbcExecutionObservationSupport observations,
                           JdbcSqlExecutor requestValidator) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "sql execution options must not be null");
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
        this.requestValidator = Objects.requireNonNull(requestValidator, "jdbc request validator must not be null");
    }
    /** 用执行器默认保护创建只读、可滚动的结果集。 */
    public <T> T scroll(SqlRequest request, JdbcCallback<ResultSet, T> callback) {
        return scroll(request, defaultOptions, callback);
    }
    /**
     * 创建只读、可滚动的结果集并在回调结束后关闭 ResultSet、Statement 和自有连接。
     *
     * <p>timeout、fetchSize、maxRows 会在执行前写入 Statement；maxResultBytes 与 LOB 上限适用于 ORM 把
     * 行物化成 DynamicRow 的路径，回调直接读取 JDBC 值时由调用方按自身读取方式控制内存。</p>
     */
    public <T> T scroll(SqlRequest request,
                        SqlExecutionOptions options,
                        JdbcCallback<ResultSet, T> callback) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> executionParameters = JdbcSqlExecutor.snapshotExecutionParameters(safeRequest);
        requestValidator.requireSingle(safeRequest);
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        JdbcCallback<ResultSet, T> safeCallback = Objects.requireNonNull(callback, "result set callback must not be null");
        long startedAt = observations.startedAt();
        SqlStatementType statementType = observations.statementType(safeRequest, false);
        SqlTransactionSource source = SqlTransactionSource.AUTO_COMMIT;
        try {
            JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire();
            source = lease.transactionSource();
            T result = executeScrollable(lease, safeRequest, executionParameters, safeOptions, safeCallback);
            observations.success(SqlExecutionOperation.QUERY, safeRequest, executionParameters,
                                 statementType, 0L, startedAt, source);
            return result;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.QUERY, safeRequest, executionParameters,
                                 statementType, 0L, startedAt, translated, source);
            throw translated;
        }
    }
    /**
     * 在当前 JDBC 租约上读取数据库元数据。元数据访问没有 SQL 文本，不会被伪装成 SQL 成功观测事件。
     */
    public <T> T metadata(JdbcCallback<DatabaseMetaData, T> callback) {
        return withConnection(Objects.requireNonNull(callback, "metadata callback must not be null"),
                              lease -> lease.externalTransaction() == null
                                      ? lease.connection().getMetaData()
                                      : JdbcExternalTransactionConnectionView.metadata(
                                              lease.connection().getMetaData()));
    }
    /**
     * 在当前 JDBC 租约上执行保存点等连接级操作。SQL 必须继续通过 {@link JdbcSqlExecutor} 执行，
     * 这样参数绑定、超时、观测和事务所有权保护不会被回调绕开。
     */
    public <T> T transaction(JdbcCallback<Connection, T> callback) {
        return withConnection(Objects.requireNonNull(callback, "transaction callback must not be null"),
                              lease -> {
                                  if (lease.externalTransaction() == null) {
                                      throw new IllegalStateException(
                                              "JDBC transaction operations require an external transaction connection");
                                  }
                                   return JdbcExternalTransactionConnectionView.connection(lease.connection());
                              });
    }
    private <S, T> T withConnection(JdbcCallback<S, T> callback,
                                    JdbcCallback<JdbcConnectionProvider.JdbcConnectionLease, S> value) {
        JdbcConnectionProvider.JdbcConnectionLease lease = null;
        Throwable failure = null;
        try {
            lease = connections.acquire();
            return callback.apply(value.apply(lease));
        } catch (SQLException | RuntimeException error) {
            failure = error;
            throw RdbExceptionTranslator.translate(error);
        } catch (Error error) {
            failure = error;
            throw error;
        } finally {
            if (lease != null) {
                closeConnectionLease(lease, failure);
            }
        }
    }
    /**
     * 元数据和连接回调本身没有 SQL 请求，不能借用 QUERY 观测去伪造一条 SQL 事件。
     * 自有连接的关闭失败仍会被稳定翻译；回调本来已失败时则作为 suppressed 保留，优先保住根因。
     */
    private static void closeConnectionLease(JdbcConnectionProvider.JdbcConnectionLease lease,
                                             Throwable failure) {
        try {
            lease.close();
        } catch (SQLException | RuntimeException | Error cleanupError) {
            if (failure instanceof VirtualMachineError failureFatal) {
                suppress(failureFatal, cleanupError);
                throw failureFatal;
            }
            if (cleanupError instanceof VirtualMachineError cleanupFatal) {
                suppress(cleanupFatal, failure);
                throw cleanupFatal;
            }
            if (failure != null) {
                suppress(failure, cleanupError);
                return;
            }
            throw RdbExceptionTranslator.translate(cleanupError);
        }
    }
    private <T> T executeScrollable(JdbcConnectionProvider.JdbcConnectionLease lease,
                                    SqlRequest request,
                                    List<Object> executionParameters,
                                    SqlExecutionOptions options,
                                    JdbcCallback<ResultSet, T> callback) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Throwable failure = null;
        boolean outcomeConfirmed = false;
        try {
            statement = lease.connection().prepareStatement(request.sql(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                             ResultSet.CONCUR_READ_ONLY);
            JdbcStatementOptions.applyForScrollableCursor(statement, options);
            JdbcStatementBinder.bind(statement, executionParameters);
            JdbcStatementControl.requireNotInterrupted(statement);
            resultSet = statement.executeQuery();
            ResultSet callbackResultSet = lease.externalTransaction() == null
                    ? resultSet
                    : JdbcExternalTransactionConnectionView.resultSet(resultSet);
            T result = callback.apply(callbackResultSet);
            outcomeConfirmed = true;
            return result;
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            JdbcResources.close(SqlExecutionOperation.QUERY, outcomeConfirmed, failure, observations,
                                resultSet, statement, lease);
        }
    }
    /** 可抛出 SQLException 的 JDBC 回调，避免为了高级入口把受检驱动异常藏进 Lambda 包装异常。 */
    @FunctionalInterface
    public interface JdbcCallback<S, T> {
        T apply(S source) throws SQLException;
    }

}
