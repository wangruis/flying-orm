package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlStatementType;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * JDBC 才有的高级能力入口。
 *
 * <p>普通 CRUD 不需要接触这里。只有确实需要滚动 {@link ResultSet} 或读取 {@link DatabaseMetaData}
 * 时才使用它。回调直接使用驱动提供的 JDBC 对象；滚动查询结束后关闭结果集和语句，每次调用结束后
 * 通过 {@link JdbcConnectionAccess} 释放连接使用范围。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcAdvancedOperations {
    private final SqlExecutionOptions defaultOptions;
    private final JdbcExecutionObservationSupport observations;
    private final JdbcSqlExecutor requestValidator;

    JdbcAdvancedOperations(SqlExecutionOptions defaultOptions,
                           JdbcExecutionObservationSupport observations,
                           JdbcSqlExecutor requestValidator) {
        this.defaultOptions = Objects.requireNonNull(defaultOptions, "sql execution options must not be null");
        this.observations = Objects.requireNonNull(observations, "jdbc observations must not be null");
        this.requestValidator = Objects.requireNonNull(requestValidator, "jdbc request validator must not be null");
    }
    /** 用执行器默认保护创建只读、可滚动的结果集。 */
    public <T> T scroll(SqlRequest request, JdbcCallback<ResultSet, T> callback) {
        return scroll(request, defaultOptions, callback);
    }
    /**
     * 创建只读、可滚动的结果集，并在回调结束后关闭 ResultSet、Statement，再释放连接使用范围。
     *
     * <p>fetchSize、maxRows 会在执行前写入 Statement；maxResultBytes 与 LOB 上限适用于 ORM 把
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

        try {
            java.sql.Connection connection = requestValidator.acquire(safeRequest);

            T result = executeScrollable(connection, safeRequest, executionParameters, safeOptions, safeCallback);
            observations.success(SqlExecutionOperation.QUERY, safeRequest, executionParameters,
                                 statementType, 0L, startedAt);
            return result;
        } catch (SQLException | RuntimeException error) {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            observations.failure(SqlExecutionOperation.QUERY, safeRequest, executionParameters,
                                 statementType, 0L, startedAt, translated);
            throw translated;
        }
    }
    /**
     * 在上层提供的 JDBC 连接上读取数据库元数据。元数据访问没有 SQL 文本，不会被伪装成 SQL 成功观测事件。
     */
    public <T> T metadata(JdbcCallback<DatabaseMetaData, T> callback) {
        return withConnection(Objects.requireNonNull(callback, "metadata callback must not be null"),
                              java.sql.Connection::getMetaData);
    }
    private <S, T> T withConnection(JdbcCallback<S, T> callback,
                                    JdbcCallback<java.sql.Connection, S> value) {
        java.sql.Connection connection = null;
        Throwable failure = null;
        try {
            connection = requestValidator.acquire(null);
            return callback.apply(value.apply(connection));
        } catch (SQLException | RuntimeException error) {
            failure = error;
            throw RdbExceptionTranslator.translate(error);
        } catch (Error error) {
            failure = error;
            throw error;
        } finally {
            if (connection != null) {
                releaseConnection(connection, failure);
            }
        }
    }
    /**
     * 元数据回调本身没有 SQL 请求，不能借用 QUERY 观测去伪造一条 SQL 事件。
     * 连接释放失败仍会被稳定翻译；回调本来已失败时则作为 suppressed 保留，优先保住根因。
     */
    private void releaseConnection(java.sql.Connection connection,
                                             Throwable failure) {
        try {
            requestValidator.release(connection, null, SqlExecutionOperation.QUERY, failure == null, failure);
        } catch (RuntimeException | Error cleanupError) {
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
    private <T> T executeScrollable(java.sql.Connection connection,
                                    SqlRequest request,
                                    List<Object> executionParameters,
                                    SqlExecutionOptions options,
                                    JdbcCallback<ResultSet, T> callback) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        Throwable failure = null;
        boolean outcomeConfirmed = false;
        try {
            statement = connection.prepareStatement(request.sql(), ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                             ResultSet.CONCUR_READ_ONLY);
            JdbcStatementOptions.applyForScrollableCursor(statement, options);
            JdbcStatementBinder.bind(statement, executionParameters);
            JdbcStatementControl.requireNotInterrupted(statement);
            resultSet = statement.executeQuery();
            T result = callback.apply(resultSet);
            outcomeConfirmed = true;
            return result;
        } catch (SQLException | RuntimeException | Error error) {
            failure = error;
            throw error;
        } finally {
            requestValidator.release(connection, request, SqlExecutionOperation.QUERY, outcomeConfirmed, failure,
                                resultSet, statement);
        }
    }
    /** 可抛出 SQLException 的 JDBC 回调，避免为了高级入口把受检驱动异常藏进 Lambda 包装异常。 */
    @FunctionalInterface
    public interface JdbcCallback<S, T> {
        T apply(S source) throws SQLException;
    }

}
