package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
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

    private JdbcExecutionObservationSupport(SqlExecutionObserver observer) {
        this.observer = SqlExecutionObservers.safe(
                Objects.requireNonNull(observer, "sql execution observer must not be null"));
        this.enabled = this.observer.enabled();
        this.parametersRequired = this.observer.requiresParameterValues();
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
                 long startedAt) {
        if (enabled) {
            success(operation, request, SqlStatementType.fromSql(request.sql()), rows, startedAt);
        }
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt) {
        success(operation, request, OwnedBindableValues.ownedValues(request.parameters()),
                statementType, rows, startedAt);
    }

    void success(SqlExecutionOperation operation,
                 SqlRequest request,
                 List<Object> executionParameters,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt) {
        if (enabled) {
            publish(operation, request, executionParameters, statementType, rows, startedAt,
                    SqlExecutionStatus.SUCCESS, SqlFailureCategory.NONE, null);
        }
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 long rows,
                 long startedAt,
                 Throwable error) {
        if (!enabled) {
            return;
        }
        failure(operation, request, SqlStatementType.fromSql(request.sql()),
                rows, startedAt, error);
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 Throwable error) {
        failure(operation, request, OwnedBindableValues.ownedValues(request.parameters()),
                statementType, rows, startedAt, error);
    }

    void failure(SqlExecutionOperation operation,
                 SqlRequest request,
                 List<Object> executionParameters,
                 SqlStatementType statementType,
                 long rows,
                 long startedAt,
                 Throwable error) {
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
                status, category, error);
    }

    void cleanupFailure(SqlExecutionOperation operation, boolean outcomeConfirmed, Throwable error) {
        cleanupFailure(operation, ResourceCleanupObservation.Phase.SESSION_CLEANUP, outcomeConfirmed, error);
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
                         Throwable error) {
        SqlExecutionObservation observation = new SqlExecutionObservation(
                operation, SqlExecutionBackend.JDBC, statementType,
                status, category, request.sql(),
                executionParameters.size(), 0, rows, System.nanoTime() - startedAt, error);
        List<Object> parameters = parametersRequired
                ? OwnedBindableValues.defensiveView(executionParameters) : List.of();
        if (parametersRequired) {
            observer.onExecution(observation, parameters);
        } else {
            observer.onExecution(observation);
        }
    }
}
