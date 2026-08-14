package com.flying.orm.rdb.observation;

import java.time.Duration;
import java.util.Objects;

/**
 * 一次 SQL 执行的观测结果。这里不放参数值，避免日志里混进业务敏感数据。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record SqlExecutionObservation(SqlExecutionOperation operation,
                                      SqlExecutionBackend backend,
                                      SqlStatementType statementType,
                                      SqlExecutionStatus status,
                                      SqlFailureCategory failureCategory,
                                      String sql,
                                      int parameterCount,
                                      int batchSize,
                                      long rows,
                                      long durationNanos,
                                      Throwable error) {

    public SqlExecutionObservation {
        operation = Objects.requireNonNull(operation, "sql execution operation must not be null");
        backend = Objects.requireNonNull(backend, "sql execution backend must not be null");
        statementType = Objects.requireNonNull(statementType, "sql statement type must not be null");
        status = Objects.requireNonNull(status, "sql execution status must not be null");
        failureCategory = Objects.requireNonNull(failureCategory, "sql failure category must not be null");
        sql = Objects.requireNonNull(sql, "sql text must not be null");
        if (parameterCount < 0) {
            throw new IllegalArgumentException("sql parameter count must not be negative");
        }
        if (batchSize < 0) {
            throw new IllegalArgumentException("sql batch size must not be negative");
        }
        if (rows < 0) {
            throw new IllegalArgumentException("sql rows must not be negative");
        }
        if (durationNanos < 0) {
            throw new IllegalArgumentException("sql duration nanos must not be negative");
        }
        // status 是执行结论，failureCategory/error 是原因。三者如果互相打架，上层监控可能把失败记成成功，
        // 所以在事件发布前直接拒绝，不让每个 observer 再猜一次。
        switch (status) {
            case SUCCESS -> {
                if (failureCategory != SqlFailureCategory.NONE || error != null) {
                    throw new IllegalArgumentException("successful sql observation cannot include failure details");
                }
            }
            case ERROR -> {
                if (failureCategory == SqlFailureCategory.NONE || error == null) {
                    throw new IllegalArgumentException("failed sql observation must include error and failure category");
                }
            }
            case CANCELLED -> {
                if (failureCategory != SqlFailureCategory.NONE
                        && failureCategory != SqlFailureCategory.CANCELLED) {
                    throw new IllegalArgumentException("cancelled sql observation cannot include unrelated failure category");
                }
                if (error != null && failureCategory == SqlFailureCategory.NONE) {
                    throw new IllegalArgumentException("cancelled sql observation error must use cancelled failure category");
                }
            }
        }
    }

    /**
     * 保留 V1 手工构造事件的源码兼容性。执行器自己不会走这里，而是始终写明 JDBC 或 R2DBC。
     */
    public SqlExecutionObservation(SqlExecutionOperation operation,
                                   SqlStatementType statementType,
                                   SqlExecutionStatus status,
                                   SqlFailureCategory failureCategory,
                                   String sql,
                                   int parameterCount,
                                   int batchSize,
                                   long rows,
                                   long durationNanos,
                                   Throwable error) {
        this(operation, SqlExecutionBackend.UNKNOWN, statementType, status, failureCategory, sql,
             parameterCount, batchSize, rows, durationNanos, error);
    }

    public Duration duration() {
        return Duration.ofNanos(durationNanos);
    }

    public SqlExecutionResultKind resultKind() {
        return SqlExecutionResultKind.fromSql(status, failureCategory);
    }
}
