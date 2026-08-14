package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * 负责普通 SQL 的响应式观测，并把批量观测交给专用协作类。
 *
 * <p>计时、行数和事务来源都在订阅时计算，同一个实例可以被并发复用。observer 的普通异常只会丢掉观测；
 * 异常图中的 JVM 致命错误仍保持 Reactor 的立即冒泡语义。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ReactiveSqlExecutionObservationSupport {

    private final SqlExecutionObserver sqlObserver;

    private final boolean needsParameterValues;

    private final boolean needsTransactionSource;

    private final ReactiveTransactionSourceResolver transactionSources;

    private final ReactiveBatchExecutionObservationSupport batches;

    private ReactiveSqlExecutionObservationSupport(SqlExecutionObserver sqlObserver,
                                                   BatchExecutionObserver batchObserver,
                                                   Supplier<Mono<R2dbcTransactionContext>> currentTransaction) {
        this.sqlObserver = SqlExecutionObservers.safe(
                Objects.requireNonNull(sqlObserver, "sql execution observer must not be null"));
        this.needsParameterValues = this.sqlObserver.requiresParameterValues();
        this.needsTransactionSource = this.sqlObserver.requiresTransactionSource();
        this.transactionSources = new ReactiveTransactionSourceResolver(currentTransaction);
        this.batches = new ReactiveBatchExecutionObservationSupport(
                this,
                BatchExecutionObserver.composite(
                        BatchExecutionObserver.noop(),
                        Objects.requireNonNull(batchObserver, "batch execution observer must not be null")),
                transactionSources);
    }

    static ReactiveSqlExecutionObservationSupport create(
            SqlExecutionObserver sqlObserver,
            BatchExecutionObserver batchObserver,
            Supplier<Mono<R2dbcTransactionContext>> currentTransaction) {
        return new ReactiveSqlExecutionObservationSupport(sqlObserver, batchObserver, currentTransaction);
    }

    static BatchExecutionObserver combineBatchObservers(BatchExecutionObserver first,
                                                         BatchExecutionObserver second) {
        return BatchExecutionObserver.composite(first, second);
    }

    <T> Flux<T> observeFlux(SqlExecutionOperation operation,
                            String sql,
                            int parameterCount,
                            int batchSize,
                            List<Object> parameters,
                            Flux<T> source) {
        return Flux.defer(() -> {
            long startedAt = System.nanoTime();
            AtomicLong rows = new AtomicLong();
            AtomicBoolean observed = new AtomicBoolean();
            if (!needsTransactionSource) {
                return observeFluxSource(observed, operation, sql, parameterCount, batchSize, parameters,
                                         source, rows, startedAt, SqlTransactionSource.AUTO_COMMIT);
            }
            return transactionSources.resolve(SqlTransactionSource.AUTO_COMMIT)
                    .flatMapMany(resolution -> observeFluxSource(
                            observed, operation, sql, parameterCount, batchSize, parameters,
                            resolution.bind(source), rows, startedAt, resolution.source()))
                    .doOnError(error -> observe(observed, operation, sql, parameterCount, batchSize,
                                                rows.get(), startedAt, SqlExecutionStatus.ERROR, error,
                                                parameters, SqlTransactionSource.AUTO_COMMIT))
                    .doOnCancel(() -> observe(observed, operation, sql, parameterCount, batchSize,
                                              rows.get(), startedAt, SqlExecutionStatus.CANCELLED, null,
                                              parameters, SqlTransactionSource.AUTO_COMMIT));
        });
    }

    Mono<Long> observeMono(SqlExecutionOperation operation,
                           String sql,
                           int parameterCount,
                           int batchSize,
                           List<Object> parameters,
                           Mono<Long> source) {
        return observeMono(operation, sql, parameterCount, batchSize, parameters, source,
                           rows -> rows == null ? 0L : rows);
    }

    /** 让生成键等复合写入结果复用完全相同的计时、事务来源和异常观测，不重复订阅数据库 Publisher。 */
    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            String sql,
                            int parameterCount,
                            int batchSize,
                            List<Object> parameters,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows) {
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            if (!needsTransactionSource) {
                return observeMonoSource(observed, operation, sql, parameterCount, batchSize, parameters,
                                         source, affectedRows, startedAt, SqlTransactionSource.AUTO_COMMIT);
            }
            return transactionSources.resolve(SqlTransactionSource.AUTO_COMMIT)
                    .flatMap(resolution -> observeMonoSource(
                            observed, operation, sql, parameterCount, batchSize, parameters,
                            resolution.bind(source), affectedRows, startedAt, resolution.source()))
                    .doOnError(error -> observe(observed, operation, sql, parameterCount, batchSize,
                                                0L, startedAt, SqlExecutionStatus.ERROR, error, parameters,
                                                SqlTransactionSource.AUTO_COMMIT))
                    .doOnCancel(() -> observe(observed, operation, sql, parameterCount, batchSize,
                                              0L, startedAt, SqlExecutionStatus.CANCELLED, null, parameters,
                                              SqlTransactionSource.AUTO_COMMIT));
        });
    }

    Mono<BatchWriteResult> observeBatchResult(BatchWriteRequest request, Mono<BatchWriteResult> source) {
        return batches.observeResult(request, source);
    }

    Flux<BatchChunkResult> observeBatchChunks(BatchWriteRequest request, Flux<BatchChunkResult> source) {
        return batches.observeChunks(request, source);
    }

    void observeRecovery(BatchExecutionObservation observation) {
        batches.observeRecovery(observation);
    }

    private <T> Flux<T> observeFluxSource(AtomicBoolean observed,
                                          SqlExecutionOperation operation,
                                          String sql,
                                          int parameterCount,
                                          int batchSize,
                                          List<Object> parameters,
                                          Flux<T> source,
                                          AtomicLong rows,
                                          long startedAt,
                                          SqlTransactionSource transactionSource) {
        return source.doOnNext(ignored -> rows.incrementAndGet())
                     .doOnComplete(() -> observe(observed, operation, sql, parameterCount, batchSize,
                                                rows.get(), startedAt, SqlExecutionStatus.SUCCESS, null,
                                                parameters, transactionSource))
                     .doOnError(error -> observe(observed, operation, sql, parameterCount, batchSize,
                                                rows.get(), startedAt, SqlExecutionStatus.ERROR, error,
                                                parameters, transactionSource))
                     .doOnCancel(() -> observe(observed, operation, sql, parameterCount, batchSize,
                                               rows.get(), startedAt, SqlExecutionStatus.CANCELLED, null,
                                               parameters, transactionSource));
    }

    private <T> Mono<T> observeMonoSource(AtomicBoolean observed,
                                          SqlExecutionOperation operation,
                                          String sql,
                                          int parameterCount,
                                          int batchSize,
                                          List<Object> parameters,
                                          Mono<T> source,
                                          ToLongFunction<T> affectedRows,
                                          long startedAt,
                                          SqlTransactionSource transactionSource) {
        return source.doOnSuccess(rows -> observe(observed, operation, sql, parameterCount, batchSize,
                                                  rows == null ? 0L : affectedRows.applyAsLong(rows), startedAt,
                                                  SqlExecutionStatus.SUCCESS, null, parameters, transactionSource))
                     .doOnError(error -> observe(observed, operation, sql, parameterCount, batchSize,
                                                0L, startedAt, SqlExecutionStatus.ERROR, error,
                                                parameters, transactionSource))
                     .doOnCancel(() -> observe(observed, operation, sql, parameterCount, batchSize,
                                               0L, startedAt, SqlExecutionStatus.CANCELLED, null,
                                               parameters, transactionSource));
    }

    void observe(AtomicBoolean observed,
                 SqlExecutionOperation operation,
                 String sql,
                 int parameterCount,
                 int batchSize,
                 long rows,
                 long startedAt,
                 SqlExecutionStatus status,
                 Throwable error,
                 List<Object> parameters,
                 SqlTransactionSource transactionSource) {
        if (!observed.compareAndSet(false, true)) {
            return;
        }
        SqlFailureCategory category = error == null ? SqlFailureCategory.NONE : SqlFailureCategory.classify(error);
        try {
            SqlExecutionObservation observation = new SqlExecutionObservation(operation,
                                                                                SqlExecutionBackend.R2DBC,
                                                                                SqlStatementType.fromSql(sql),
                                                                                status,
                                                                                category,
                                                                                sql,
                                                                                parameterCount,
                                                                                batchSize,
                                                                                rows,
                                                                                System.nanoTime() - startedAt,
                                                                                error);
            if (needsParameterValues && needsTransactionSource) {
                sqlObserver.onExecution(observation,
                                        parameters == null ? List.of() : parameters,
                                        transactionSource);
            } else if (needsParameterValues) {
                sqlObserver.onExecution(observation, parameters == null ? List.of() : parameters);
            } else if (needsTransactionSource) {
                sqlObserver.onExecution(observation, transactionSource);
            } else {
                sqlObserver.onExecution(observation);
            }
        } catch (RuntimeException ignored) {
            // 观察器的普通故障不影响 SQL 执行主链路；JVM 致命错误由安全包装器原样传播。
        }
    }
}
