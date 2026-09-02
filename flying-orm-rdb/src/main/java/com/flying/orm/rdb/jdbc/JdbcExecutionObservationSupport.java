package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;

import java.util.List;
import java.util.Objects;

/**
 * JDBC 同步调用的轻量观测协作器。observer 在装配边界完成一次故障隔离，事件组装错误仍原样传播。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class JdbcExecutionObservationSupport {

    private final SqlExecutionObserver observer;
    private final boolean enabled;
    private final boolean parametersRequired;
    private final boolean transactionSourceRequired;

    private JdbcExecutionObservationSupport(SqlExecutionObserver observer) {
        this.observer = SqlExecutionObservers.safe(
                Objects.requireNonNull(observer, "sql execution observer must not be null"));
        this.enabled = this.observer.enabled();
        this.parametersRequired = this.observer.requiresParameterValues();
        this.transactionSourceRequired = this.observer.requiresTransactionSource();
    }

    static JdbcExecutionObservationSupport create(SqlExecutionObserver observer) {
        return new JdbcExecutionObservationSupport(observer);
    }

    long startedAt() {
        return enabled ? System.nanoTime() : 0L;
    }

    SqlStatementType statementType(SqlRequest request, boolean executionRequiresType) {
        SqlRequest safeRequest = Objects.requireNonNull(
                request, "sql observation request must not be null");
        return enabled || executionRequiresType
                ? SqlStatementType.fromSql(safeRequest.sql()) : SqlStatementType.UNKNOWN;
    }

    boolean enabled() {
        return enabled;
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 long rows,
                 long startedAt,
                 SqlTransactionSource transactionSource) {
        if (enabled) {
            success(operation, request, SqlStatementType.fromSql(request.sql()), rows, startedAt, transactionSource);
        }
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 SqlTransactionSource transactionSource) {
        success(operation, request, request.parameters(), statementType, rows, startedAt, transactionSource);
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 List<Object> executionParameters,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 SqlTransactionSource transactionSource) {
        if (enabled) {
            publish(operation, request, executionParameters, statementType, rows, startedAt,
                    SqlExecutionStatus.SUCCESS, SqlFailureCategory.NONE, null, transactionSource);
        }
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 long rows,
                 long startedAt,
                 Throwable error,
                 SqlTransactionSource transactionSource) {
        if (!enabled) {
            return;
        }
        failure(operation, request, SqlStatementType.fromSql(request.sql()),
                rows, startedAt, error, transactionSource);
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 Throwable error,
                 SqlTransactionSource transactionSource) {
        failure(operation, request, request.parameters(), statementType, rows, startedAt, error, transactionSource);
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 List<Object> executionParameters,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 Throwable error,
                 SqlTransactionSource transactionSource) {
        if (!enabled) {
            return;
        }
        SqlFailureCategory category = SqlFailureCategory.classify(error);
        SqlExecutionStatus status = category == SqlFailureCategory.CANCELLED
                ? SqlExecutionStatus.CANCELLED
                : SqlExecutionStatus.ERROR;
        long observedRows = error instanceof GeneratedKeyReadException generatedKeyFailure
                ? Math.max(rows, generatedKeyFailure.affectedRows())
                : rows;
        publish(operation, request, executionParameters, statementType, observedRows, startedAt,
                status, category, error, transactionSource);
    }

    void cleanupFailure(SqlExecutionOperation operation, boolean outcomeConfirmed, Throwable error) {
        cleanupFailure(operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, outcomeConfirmed, error);
    }

    /** 发布指定阶段的脱敏清理故障；普通观测器异常不得改写已经确定的数据库结果。 */
    void cleanupFailure(SqlExecutionOperation operation,
                        ResourceCleanupObservation.Phase phase,
                        boolean outcomeConfirmed,
                        Throwable error) {
        if (!enabled) {
            return;
        }
        observer.onResourceCleanup(new ResourceCleanupObservation(
                operation, phase, outcomeConfirmed, error));
    }

    private void publish(SqlExecutionOperation operation,
                         SqlRequest request,
                         List<Object> executionParameters,
                         SqlStatementType statementType,
                         long rows,
                         long startedAt,
                         SqlExecutionStatus status,
                         SqlFailureCategory category,
                         Throwable error,
                         SqlTransactionSource transactionSource) {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                operation, SqlExecutionBackend.JDBC, statementType,
                status, category, request.sql(),
                request.parameters().size(), 0, rows, System.nanoTime() - startedAt, error);
        List<Object> parameters = parametersRequired ? executionParameters : List.of();
        if (parametersRequired && transactionSourceRequired) {
            observer.onExecution(observation, parameters, transactionSource);
        } else if (parametersRequired) {
            observer.onExecution(observation, parameters);
        } else if (transactionSourceRequired) {
            observer.onExecution(observation, transactionSource);
        } else {
            observer.onExecution(observation);
        }
    }
}
