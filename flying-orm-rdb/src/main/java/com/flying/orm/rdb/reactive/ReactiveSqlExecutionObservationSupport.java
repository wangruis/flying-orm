package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.observation.SqlStatementType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

/**
 * 负责普通 SQL 的响应式观测，并把批量观测交给专用协作类。
 *
 * <p>计时和行数在订阅时计算，同一个实例可以被并发复用。observer 在装配边界完成一次
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



    private final ReactiveBatchExecutionObservationSupport batches;

    private ReactiveSqlExecutionObservationSupport(SqlExecutionObserver sqlObserver,
                                                   BatchExecutionObserver batchObserver) {
        this.sqlObserver = SqlExecutionObservers.safe(
                Objects.requireNonNull(sqlObserver, "sql execution observer must not be null"));
        this.sqlEnabled = this.sqlObserver.enabled();
        this.needsParameterValues = this.sqlObserver.requiresParameterValues();
        this.batches = new ReactiveBatchExecutionObservationSupport(
                this,
                BatchExecutionObserver.composite(
                        BatchExecutionObserver.noop(),
                        Objects.requireNonNull(batchObserver, "batch execution observer must not be null")));
    }

    static ReactiveSqlExecutionObservationSupport create(
            SqlExecutionObserver sqlObserver,
            BatchExecutionObserver batchObserver) {
        return new ReactiveSqlExecutionObservationSupport(sqlObserver, batchObserver);
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
        return observeFlux(operation, request, OwnedBindableValues.ownedValues(request.parameters()),
                batchSize, source, options);
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
        return Flux.defer(() -> {
            ReactiveSqlObservation observation = start(observationRequest);
            return source.doOnNext(ignored -> observation.incrementRows())
                    .doOnComplete(observation::success)
                    .doOnError(observation::error)
                    .doOnCancel(observation::cancelled);
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

    /** 让生成键等复合写入结果复用完全相同的计时和异常观测，不重复订阅数据库 Publisher。 */
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
        return observeMono(operation, request, OwnedBindableValues.ownedValues(request.parameters()),
                batchSize, source, affectedRows, options);
    }

    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            SqlRequest request,
                            List<Object> executionParameters,
                            int batchSize,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows,
                            SqlExecutionOptions options) {
        return observeMono(operation, request, executionParameters, batchSize,
                           source, affectedRows, () -> 0L, options);
    }

    <T> Mono<T> observeMono(SqlExecutionOperation operation,
                            SqlRequest request,
                            List<Object> executionParameters,
                            int batchSize,
                            Mono<T> source,
                            ToLongFunction<T> affectedRows,
                            LongSupplier confirmedRows,
                            SqlExecutionOptions options) {
        Objects.requireNonNull(options, "sql execution options must not be null");
        LongSupplier safeConfirmedRows = Objects.requireNonNull(
                confirmedRows, "confirmed SQL rows supplier must not be null");
        if (!sqlEnabled) {
            return Objects.requireNonNull(source, "sql observation source must not be null");
        }
        ReactiveSqlObservation.Request observationRequest = request(
                operation, request, executionParameters, batchSize);
        return Mono.defer(() -> {
            ReactiveSqlObservation observation = start(observationRequest);
            return source.doOnSuccess(value -> observation.success(
                            value == null ? 0L : affectedRows.applyAsLong(value)))
                    .doOnError(error -> observation.error(
                            confirmedRows(error, safeConfirmedRows.getAsLong()), batchSize, error))
                    .doOnCancel(() -> observation.cancelled(safeConfirmedRows.getAsLong(), batchSize));
        });
    }

    private static long confirmedRows(Throwable error, long rows) {
        return error instanceof GeneratedKeyReadException generatedKeyFailure
                ? Math.max(rows, generatedKeyFailure.affectedRows()) : rows;
    }

    Mono<BatchExecutionEvidence> observeBatchResult(BatchWriteRequest request,
                                                    Mono<BatchExecutionEvidence> source) {
        return batches.observeResult(request, source);
    }

    ReactiveSqlObservation start(ReactiveSqlObservation.Request request) {
        return new ReactiveSqlObservation(
                sqlObserver, needsParameterValues, request);
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
            private final Request request;
        private final long startedAt = System.nanoTime();
        private final AtomicBoolean terminal = new AtomicBoolean();
        private final AtomicLong rows = new AtomicLong();

        private ReactiveSqlObservation(SqlExecutionObserver observer,
                                       boolean needsParameterValues,
                                       Request request) {
            this.observer = Objects.requireNonNull(
                    observer, "sql execution observer must not be null");
            this.needsParameterValues = needsParameterValues;
                this.request = Objects.requireNonNull(
                    request, "sql observation request must not be null");
        }

        void incrementRows() {
            rows.incrementAndGet();
        }

        void success() {
            publish(SqlExecutionStatus.SUCCESS, null, rows.get(),
                    request.batchSize());
        }

        void success(long affectedRows) {
            publish(SqlExecutionStatus.SUCCESS, null, affectedRows,
                    request.batchSize());
        }

        void success(long affectedRows,
                     int batchSize) {
            publish(SqlExecutionStatus.SUCCESS, null, affectedRows, batchSize);
        }

        void error(Throwable error) {
            Throwable safeError = Objects.requireNonNull(
                    error, "sql observation error must not be null");
            long observedRows = safeError instanceof GeneratedKeyReadException generatedKeyFailure
                    ? Math.max(rows.get(), generatedKeyFailure.affectedRows())
                    : rows.get();
            publish(SqlExecutionStatus.ERROR, safeError, observedRows,
                    request.batchSize());
        }

        void error(long affectedRows,
                   int batchSize,
                   Throwable error) {
            publish(SqlExecutionStatus.ERROR, Objects.requireNonNull(
                    error, "sql observation error must not be null"),
                    affectedRows, batchSize);
        }

        void cancelled() {
            publish(SqlExecutionStatus.CANCELLED, null, rows.get(),
                    request.batchSize());
        }

        void cancelled(long affectedRows,
                       int batchSize) {
            publish(SqlExecutionStatus.CANCELLED, null, affectedRows, batchSize);
        }

        private void publish(SqlExecutionStatus status,
                             Throwable error,
                             long observedRows,
                             int batchSize) {
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
            notifyObserver(observation);
        }

        private void notifyObserver(SqlExecutionObservation observation) {
            if (needsParameterValues) {
                List<Object> requestParameters = request.parameters();
                List<Object> parameters = requestParameters == null
                        ? List.of() : OwnedBindableValues.defensiveView(requestParameters);
                observer.onExecution(observation, parameters);
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
