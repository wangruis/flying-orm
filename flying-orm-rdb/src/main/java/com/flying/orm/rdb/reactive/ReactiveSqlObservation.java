package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** 一次响应式 SQL 订阅的计数和唯一终态。 */
final class ReactiveSqlObservation {

    private final SqlExecutionObserver observer;

    private final boolean needsParameterValues;

    private final boolean needsTransactionSource;

    private final Request request;

    private final long startedAt = System.nanoTime();

    private final AtomicBoolean terminal = new AtomicBoolean();

    private final AtomicLong rows = new AtomicLong();

    private ReactiveSqlObservation(SqlExecutionObserver observer,
                                   boolean needsParameterValues,
                                   boolean needsTransactionSource,
                                   Request request) {
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.needsParameterValues = needsParameterValues;
        this.needsTransactionSource = needsTransactionSource;
        this.request = Objects.requireNonNull(request, "sql observation request must not be null");
    }

    static ReactiveSqlObservation start(SqlExecutionObserver observer,
                                        boolean needsParameterValues,
                                        boolean needsTransactionSource,
                                        Request request) {
        return new ReactiveSqlObservation(observer, needsParameterValues, needsTransactionSource, request);
    }

    void incrementRows() {
        rows.incrementAndGet();
    }

    void success(SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.SUCCESS, null, rows.get(), request.batchSize(), transactionSource);
    }

    void success(long affectedRows, SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.SUCCESS, null, affectedRows, request.batchSize(), transactionSource);
    }

    void success(long affectedRows, int batchSize, SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.SUCCESS, null, affectedRows, batchSize, transactionSource);
    }

    void error(Throwable error, SqlTransactionSource transactionSource) {
        Throwable safeError = Objects.requireNonNull(error, "sql observation error must not be null");
        long observedRows = safeError instanceof GeneratedKeyReadException generatedKeyFailure
                ? Math.max(rows.get(), generatedKeyFailure.affectedRows())
                : rows.get();
        publish(SqlExecutionStatus.ERROR, safeError, observedRows, request.batchSize(), transactionSource);
    }

    void error(long affectedRows,
               int batchSize,
               Throwable error,
               SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.ERROR, Objects.requireNonNull(error, "sql observation error must not be null"),
                affectedRows, batchSize, transactionSource);
    }

    void cancelled(SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.CANCELLED, null, rows.get(), request.batchSize(), transactionSource);
    }

    void cancelled(long affectedRows, int batchSize, SqlTransactionSource transactionSource) {
        publish(SqlExecutionStatus.CANCELLED, null, affectedRows, batchSize, transactionSource);
    }

    private void publish(SqlExecutionStatus status,
                         Throwable error,
                         long observedRows,
                         int batchSize,
                         SqlTransactionSource transactionSource) {
        if (!terminal.compareAndSet(false, true)) {
            return;
        }
        SqlFailureCategory category = error == null ? SqlFailureCategory.NONE : SqlFailureCategory.classify(error);
        SqlExecutionObservation observation = new SqlExecutionObservation(
                request.operation(),
                SqlExecutionBackend.R2DBC,
                SqlStatementType.fromSql(request.sql()),
                status,
                category,
                request.sql(),
                request.parameterCount(),
                batchSize,
                observedRows,
                System.nanoTime() - startedAt,
                error);
        notifyObserver(observation, Objects.requireNonNull(
                transactionSource, "sql transaction source must not be null"));
    }

    private void notifyObserver(SqlExecutionObservation observation,
                                SqlTransactionSource transactionSource) {
        List<Object> parameters = request.parameters() == null ? List.of() : request.parameters();
        if (needsParameterValues && needsTransactionSource) {
            observer.onExecution(observation, parameters, transactionSource);
        } else if (needsParameterValues) {
            observer.onExecution(observation, parameters);
        } else if (needsTransactionSource) {
            observer.onExecution(observation, transactionSource);
        } else {
            observer.onExecution(observation);
        }
    }

    /** 一条 SQL 在所有订阅间不变的观测元数据。 */
    record Request(SqlExecutionOperation operation,
                   String sql,
                   int parameterCount,
                   int batchSize,
                   List<Object> parameters) {

        Request {
            operation = Objects.requireNonNull(operation, "sql execution operation must not be null");
            sql = Objects.requireNonNull(sql, "sql text must not be null");
            if (parameterCount < 0) {
                throw new IllegalArgumentException("sql parameter count must not be negative");
            }
            if (batchSize < 0) {
                throw new IllegalArgumentException("sql batch size must not be negative");
            }
        }
    }
}
