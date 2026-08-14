package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;

import java.util.List;
import java.util.Objects;

/**
 * JDBC 同步调用的轻量观测协作器。observer 的普通故障只丢观测；异常图中的 JVM 致命错误保持原终止语义。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class JdbcExecutionObservationSupport {

    private final SqlExecutionObserver observer;
    private final boolean parametersRequired;
    private final boolean transactionSourceRequired;

    private JdbcExecutionObservationSupport(SqlExecutionObserver observer) {
        this.observer = SqlExecutionObservers.safe(
                Objects.requireNonNull(observer, "sql execution observer must not be null"));
        this.parametersRequired = this.observer.requiresParameterValues();
        this.transactionSourceRequired = this.observer.requiresTransactionSource();
    }

    static JdbcExecutionObservationSupport create(SqlExecutionObserver observer) {
        return new JdbcExecutionObservationSupport(observer);
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 long rows,
                 long startedAt,
                 SqlTransactionSource transactionSource) {
        publish(operation, request, rows, startedAt, SqlExecutionStatus.SUCCESS, null, transactionSource);
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 long rows,
                 long startedAt,
                 Throwable error,
                 SqlTransactionSource transactionSource) {
        SqlExecutionStatus status = SqlFailureCategory.classify(error) == SqlFailureCategory.CANCELLED
                ? SqlExecutionStatus.CANCELLED
                : SqlExecutionStatus.ERROR;
        publish(operation, request, rows, startedAt, status, error, transactionSource);
    }

    void cleanupFailure(SqlExecutionOperation operation, boolean outcomeConfirmed, Throwable error) {
        cleanupFailure(operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, outcomeConfirmed, error);
    }

    /** 发布指定阶段的脱敏清理故障；普通观测器异常不得改写已经确定的数据库结果。 */
    void cleanupFailure(SqlExecutionOperation operation,
                        ResourceCleanupObservation.Phase phase,
                        boolean outcomeConfirmed,
                        Throwable error) {
        try {
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, phase, outcomeConfirmed, error));
        } catch (RuntimeException ignored) {
            // 清理观测和普通 SQL 观测一样，不能反向污染业务结果。
        }
    }

    private void publish(SqlExecutionOperation operation,
                         SqlRequest request,
                         long rows,
                         long startedAt,
                         SqlExecutionStatus status,
                         Throwable error,
                         SqlTransactionSource transactionSource) {
        try {
            SqlFailureCategory category = error == null
                    ? SqlFailureCategory.NONE
                    : SqlFailureCategory.classify(error);
            SqlExecutionObservation observation = new SqlExecutionObservation(
                    operation, SqlExecutionBackend.JDBC, SqlStatementType.fromSql(request.sql()),
                    status, category, request.sql(),
                    request.parameters().size(), 0, rows, System.nanoTime() - startedAt, error);
            List<Object> parameters = parametersRequired ? request.parameters() : List.of();
            if (parametersRequired && transactionSourceRequired) {
                observer.onExecution(observation, parameters, transactionSource);
            } else if (parametersRequired) {
                observer.onExecution(observation, parameters);
            } else if (transactionSourceRequired) {
                observer.onExecution(observation, transactionSource);
            } else {
                observer.onExecution(observation);
            }
        } catch (RuntimeException ignored) {
            // 日志或指标代码不能反过来改变 SQL 的成功、失败和取消语义。
        }
    }
}
