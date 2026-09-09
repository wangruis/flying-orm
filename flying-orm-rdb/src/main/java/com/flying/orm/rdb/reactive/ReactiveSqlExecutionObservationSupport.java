package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

/**
 * 负责普通 SQL 的响应式观测，并把批量观测交给专用协作类。
 *
 * <p>计时、行数和事务来源都在订阅时计算，同一个实例可以被并发复用。observer 在装配边界完成一次
 * 普通异常隔离；直接抛出的 {@link Error} 仍保持 Reactor 的立即冒泡语义。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ReactiveSqlExecutionObservationSupport {

    private final SqlExecutionObserver sqlObserver;

    private final boolean sqlEnabled;

    private final boolean needsParameterValues;

    private final boolean needsTransactionSource;

    private final ReactiveTransactionSourceResolver transactionSources;

    private final ReactiveBatchExecutionObservationSupport batches;

    private ReactiveSqlExecutionObservationSupport(SqlExecutionObserver sqlObserver,
                                                   BatchExecutionObserver batchObserver,
                                                   Supplier<Mono<R2dbcTransactionContext>> currentTransaction) {
        this.sqlObserver = SqlExecutionObservers.safe(
                Objects.requireNonNull(sqlObserver, "sql execution observer must not be null"));
        this.sqlEnabled = this.sqlObserver.enabled();
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

    boolean enabled() {
        return sqlEnabled;
    }

    <T> Flux<T> observeFlux(SqlExecutionOperation operation,
                            SqlRequest request,
                            int batchSize,
                            Flux<T> source) {
        return observeFlux(operation, request, batchSize, source,
                           SqlExecutionOptions.safeDefaults());
    }

    <T> Flux<T> observeFlux(SqlExecutionOperation operation,
                            SqlRequest request,
                            int batchSize,
                            Flux<T> source,
                            SqlExecutionOptions options) {
        return observeFlux(operation, request, request.parameters(), batchSize, source, options);
    }

    <T> Flux<T> observeFlux(SqlExecutionOperation operation,
                            SqlRequest request,
                            List<Object> executionParameters,
                            int batchSize,
                            Flux<T> source,
                            SqlExecutionOptions options) {
        Objects.requireNonNull(options, "sql execution options must not be null");
        if (!sqlEnabled) {
            return Objects.requireNonNull(source, "sql observation source must not be null");
        }
        ReactiveSqlObservation.Request observationRequest = request(
                operation, request, executionParameters, batchSize);
        return Flux.deferContextual(context -> {
            ReactiveSqlObservation observation = start(observationRequest);
            if (!needsTransactionSource) {
                return observeFluxSource(observation, source, SqlTransactionSource.AUTO_COMMIT)
                        .doOnError(error -> observation.error(error, SqlTransactionSource.AUTO_COMMIT))
                        .doOnCancel(() -> observation.cancelled(SqlTransactionSource.AUTO_COMMIT));
            }
            AtomicReference<SqlTransactionSource> transactionSource = new AtomicReference<>(
                    SqlTransactionSource.AUTO_COMMIT);
            return transactionSources.resolve(SqlTransactionSource.AUTO_COMMIT)
                    .flatMapMany(resolution -> {
                        transactionSource.set(resolution.source());
                        return observeFluxSource(
                                observation, resolution.bind(source), resolution.source());
                    })
                    .doOnError(error -> observation.error(error, transactionSource.get()))
                    .doOnCancel(() -> observation.cancelled(transactionSource.get()));
        });
    }

    Mono<Long> observeMono(SqlExecutionOperation operation,
                           SqlRequest request,
                           int batchSize,
                           Mono<Long> source) {
        return observeMono(operation, request, batchSize, source,
                           rows -> rows == null ? 0L : rows, SqlExecutionOptions.safeDefaults());
    }

    Mono<Long> observeMono(SqlExecutionOperation operation,
                           SqlRequest request,
                           int batchSize,
                           Mono<Long> source,
                           SqlExecutionOptions options) {
        return observeMono(operation, request, batchSize, source,
                           rows -> rows == null ? 0L : rows, options);
    }

    /** 让生成键等复合写入结果复用完全相同的计时、事务来源和异常观测，不重复订阅数据库 Publisher。 */
    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            SqlRequest request,
                            int batchSize,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows) {
        return observeMono(operation, request, batchSize, source,
                           affectedRows, SqlExecutionOptions.safeDefaults());
    }

    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            SqlRequest request,
                            int batchSize,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows,
                            SqlExecutionOptions options) {
        return observeMono(operation, request, request.parameters(), batchSize, source, affectedRows, options);
    }

    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            SqlRequest request,
                            List<Object> executionParameters,
                            int batchSize,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows,
                            SqlExecutionOptions options) {
        Objects.requireNonNull(options, "sql execution options must not be null");
        if (!sqlEnabled) {
            return Objects.requireNonNull(source, "sql observation source must not be null");
        }
        ReactiveSqlObservation.Request observationRequest = request(
                operation, request, executionParameters, batchSize);
        return Mono.deferContextual(context -> {
            ReactiveSqlObservation observation = start(observationRequest);
            if (!needsTransactionSource) {
                return observeMonoSource(observation, source, affectedRows, SqlTransactionSource.AUTO_COMMIT)
                        .doOnError(error -> observation.error(error, SqlTransactionSource.AUTO_COMMIT))
                        .doOnCancel(() -> observation.cancelled(SqlTransactionSource.AUTO_COMMIT));
            }
            AtomicReference<SqlTransactionSource> transactionSource = new AtomicReference<>(
                    SqlTransactionSource.AUTO_COMMIT);
            return transactionSources.resolve(SqlTransactionSource.AUTO_COMMIT)
                    .flatMap(resolution -> {
                        transactionSource.set(resolution.source());
                        return observeMonoSource(
                                observation, resolution.bind(source), affectedRows, resolution.source());
                    })
                    .doOnError(error -> observation.error(error, transactionSource.get()))
                    .doOnCancel(() -> observation.cancelled(transactionSource.get()));
        });
    }

    Mono<BatchWriteResult> observeBatchResult(BatchWriteRequest request, Mono<BatchWriteResult> source) {
        return batches.observeResult(request, source);
    }

    Mono<BatchExecutionEvidence> observeBatchEvidence(Mono<BatchExecutionEvidence> source) {
        return batches.observeEvidence(source);
    }

    Mono<BatchWriteResult> observeBatchResult(
            BatchWriteRequest request,
            Mono<BatchWriteResult> source,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        return batches.observeResult(request, source, resolution);
    }

    Mono<BatchWriteResult> observeBatchResult(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Mono<BatchWriteResult>> sourceFactory) {
        return batches.observeResult(request, resolutions, sourceFactory);
    }

    Flux<BatchChunkResult> observeBatchChunks(BatchWriteRequest request, Flux<BatchChunkResult> source) {
        return batches.observeChunks(request, source);
    }

    Flux<BatchChunkResult> observeBatchChunks(
            BatchWriteRequest request,
            Flux<BatchChunkResult> source,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        return batches.observeChunks(request, source, resolution);
    }

    Flux<BatchChunkResult> observeBatchChunks(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Flux<BatchChunkResult>> sourceFactory) {
        return batches.observeChunks(request, resolutions, sourceFactory);
    }

    void observeRecovery(BatchExecutionObservation observation) {
        batches.observeRecovery(observation);
    }

    private <T> Flux<T> observeFluxSource(ReactiveSqlObservation observation,
                                          Flux<T> source,
                                          SqlTransactionSource transactionSource) {
        return source.doOnNext(ignored -> observation.incrementRows())
                     .doOnComplete(() -> observation.success(transactionSource));
    }

    private <T> Mono<T> observeMonoSource(ReactiveSqlObservation observation,
                                          Mono<T> source,
                                          ToLongFunction<T> affectedRows,
                                          SqlTransactionSource transactionSource) {
        return source.doOnSuccess(rows -> observation.success(
                             rows == null ? 0L : affectedRows.applyAsLong(rows), transactionSource));
    }

    ReactiveSqlObservation start(ReactiveSqlObservation.Request request) {
        return new ReactiveSqlObservation(
                sqlObserver, needsParameterValues, needsTransactionSource, request);
    }

    private static ReactiveSqlObservation.Request request(SqlExecutionOperation operation,
                                                          SqlRequest request,
                                                          List<Object> executionParameters,
                                                          int batchSize) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "SQL request must not be null");
        List<Object> safeParameters = Objects.requireNonNull(
                executionParameters, "execution parameters must not be null");
        return new ReactiveSqlObservation.Request(
                operation,
                safeRequest.sql(),
                safeParameters.size(),
                batchSize,
                safeParameters);
    }

    /** 一次响应式 SQL 订阅的行数、耗时和唯一终态，只能由当前观测支持创建。 */
    static final class ReactiveSqlObservation {

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
            this.observer = Objects.requireNonNull(
                    observer, "sql execution observer must not be null");
            this.needsParameterValues = needsParameterValues;
            this.needsTransactionSource = needsTransactionSource;
            this.request = Objects.requireNonNull(
                    request, "sql observation request must not be null");
        }

        void incrementRows() {
            rows.incrementAndGet();
        }

        void success(SqlTransactionSource transactionSource) {
            publish(SqlExecutionStatus.SUCCESS, null, rows.get(),
                    request.batchSize(), transactionSource);
        }

        void success(long affectedRows, SqlTransactionSource transactionSource) {
            publish(SqlExecutionStatus.SUCCESS, null, affectedRows,
                    request.batchSize(), transactionSource);
        }

        void success(long affectedRows,
                     int batchSize,
                     SqlTransactionSource transactionSource) {
            publish(SqlExecutionStatus.SUCCESS, null, affectedRows, batchSize, transactionSource);
        }

        void error(Throwable error, SqlTransactionSource transactionSource) {
            Throwable safeError = Objects.requireNonNull(
                    error, "sql observation error must not be null");
            long observedRows = safeError instanceof GeneratedKeyReadException generatedKeyFailure
                    ? Math.max(rows.get(), generatedKeyFailure.affectedRows())
                    : rows.get();
            publish(SqlExecutionStatus.ERROR, safeError, observedRows,
                    request.batchSize(), transactionSource);
        }

        void error(long affectedRows,
                   int batchSize,
                   Throwable error,
                   SqlTransactionSource transactionSource) {
            publish(SqlExecutionStatus.ERROR, Objects.requireNonNull(
                    error, "sql observation error must not be null"),
                    affectedRows, batchSize, transactionSource);
        }

        void cancelled(SqlTransactionSource transactionSource) {
            publish(SqlExecutionStatus.CANCELLED, null, rows.get(),
                    request.batchSize(), transactionSource);
        }

        void cancelled(long affectedRows,
                       int batchSize,
                       SqlTransactionSource transactionSource) {
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
            SqlFailureCategory category = error == null
                    ? SqlFailureCategory.NONE : SqlFailureCategory.classify(error);
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
            List<Object> parameters = request.parameters() == null
                    ? List.of() : request.parameters();
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
                operation = Objects.requireNonNull(
                        operation, "sql execution operation must not be null");
                sql = Objects.requireNonNull(sql, "sql text must not be null");
                if (parameterCount < 0) {
                    throw new IllegalArgumentException(
                            "sql parameter count must not be negative");
                }
                if (batchSize < 0) {
                    throw new IllegalArgumentException(
                            "sql batch size must not be negative");
                }
            }
        }
    }
}
