package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 批量执行的分片、汇总和普通 SQL 摘要观测。
 *
 * <p>批量没有外部事务时一定由 flying-orm 管理事务，因此本地来源为 {@code INTERNAL}；检测到外部事务时
 * 自动改为 {@code EXTERNAL}。事务解析服从上层事务管理器，不受 ORM 的 SQL 时限取消；这里不保存 rows
 * Publisher，也不会为了日志重新订阅输入。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ReactiveBatchExecutionObservationSupport {

    private final ReactiveSqlExecutionObservationSupport sql;

    private final BatchExecutionObserver observer;

    private final boolean sqlEnabled;

    private final boolean batchEnabled;

    private final boolean enabled;

    private final ReactiveTransactionSourceResolver transactionSources;

    ReactiveBatchExecutionObservationSupport(ReactiveSqlExecutionObservationSupport sql,
                                             BatchExecutionObserver observer,
                                             ReactiveTransactionSourceResolver transactionSources) {
        this.sql = Objects.requireNonNull(sql, "sql observation support must not be null");
        this.observer = BatchExecutionObserver.composite(
                BatchExecutionObserver.noop(),
                Objects.requireNonNull(observer, "batch execution observer must not be null"));
        this.sqlEnabled = this.sql.enabled();
        this.batchEnabled = this.observer.enabled();
        this.enabled = sqlEnabled || batchEnabled;
        this.transactionSources = Objects.requireNonNull(transactionSources,
                                                         "transaction source resolver must not be null");
    }

    Mono<BatchWriteResult> observeResult(BatchWriteRequest request, Mono<BatchWriteResult> source) {
        Mono<BatchWriteResult> safeSource = Objects.requireNonNull(
                source, "batch result observation source must not be null");
        if (!enabled) {
            return safeSource;
        }
        return observeResult(request,
                transactionSources.resolve(SqlTransactionSource.INTERNAL),
                ignored -> safeSource,
                true);
    }

    /** evidence 只走显式旁路，不把它伪装成 legacy BatchWriteResult 事件。 */
    Mono<BatchExecutionEvidence> observeEvidence(Mono<BatchExecutionEvidence> source) {
        Mono<BatchExecutionEvidence> safeSource = Objects.requireNonNull(
                source, "batch evidence observation source must not be null");
        if (!batchEnabled) {
            return safeSource;
        }
        return safeSource
                .doOnSuccess(evidence -> {
                    if (evidence != null) {
                        observer.onExecutionEvidence(evidence);
                    }
                })
                .doOnError(BatchExecutionEvidenceException.class,
                        failure -> observer.onExecutionEvidence(failure.evidence()));
    }

    Mono<BatchWriteResult> observeResult(
            BatchWriteRequest request,
            Mono<BatchWriteResult> source,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        Mono<BatchWriteResult> safeSource = Objects.requireNonNull(
                source, "batch result observation source must not be null");
        if (!enabled) {
            return safeSource;
        }
        return observeResult(request,
                Mono.just(Objects.requireNonNull(
                        resolution, "transaction resolution must not be null")),
                ignored -> safeSource,
                false);
    }

    Mono<BatchWriteResult> observeResult(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Mono<BatchWriteResult>> sourceFactory) {
        return observeResult(request, resolutions, sourceFactory, true);
    }

    private Mono<BatchWriteResult> observeResult(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Mono<BatchWriteResult>> sourceFactory,
            boolean bindResolution) {
        Objects.requireNonNull(request, "batch write request must not be null");
        Mono<ReactiveTransactionSourceResolver.Resolution> safeResolutions = Objects.requireNonNull(
                resolutions, "transaction resolutions must not be null");
        Function<ReactiveTransactionSourceResolver.Resolution, Mono<BatchWriteResult>> safeSourceFactory =
                Objects.requireNonNull(sourceFactory, "batch result source factory must not be null");
        if (!enabled) {
            return safeResolutions.flatMap(safeSourceFactory);
        }
        return Mono.deferContextual(context -> {
            long startedAt = System.nanoTime();
            ReactiveSqlObservation sqlObservation = sqlEnabled ? sql.start(sqlRequest(request)) : null;
            BatchExecutionObservation.BatchWriteRequestView batchView = batchEnabled ? batchView(request) : null;
            AtomicReference<SqlTransactionSource> transactionSource = new AtomicReference<>(
                    SqlTransactionSource.INTERNAL);
            Mono<ReactiveTransactionSourceResolver.Resolution> observedResolutions = safeResolutions
                    .doOnError(error -> {
                        observeSqlError(sqlObservation, error, SqlTransactionSource.INTERNAL);
                        if (batchEnabled) {
                            observe(BatchExecutionObservation.failedSummary(
                                            batchView, System.nanoTime() - startedAt, error),
                                    SqlTransactionSource.INTERNAL);
                        }
                    });
            return observedResolutions
                    .flatMap(resolution -> {
                        transactionSource.set(resolution.source());
                        Mono<BatchWriteResult> source = Mono.defer(() -> safeSourceFactory.apply(resolution));
                        return (bindResolution ? resolution.bind(source) : source)
                            .doOnSuccess(result -> observeSqlResult(
                                    sqlObservation, result, resolution.source()))
                            .doOnSuccess(result -> {
                                if (batchEnabled) {
                                    observeResult(result,
                                                  batchView,
                                                  System.nanoTime() - startedAt,
                                                  resolution.source());
                                }
                            })
                            .doOnError(error -> {
                                observeSqlError(sqlObservation, error, resolution.source());
                                if (batchEnabled) {
                                    observeError(error,
                                                 batchView,
                                                 System.nanoTime() - startedAt,
                                                 resolution.source());
                                }
                            });
                    })
                    .doOnCancel(() -> observeSqlCancelled(
                            sqlObservation, 0L, 0, transactionSource.get()));
        });
    }

    private static void observeSqlResult(ReactiveSqlObservation observation,
                                         BatchWriteResult result,
                                         SqlTransactionSource transactionSource) {
        if (observation == null) {
            return;
        }
        if (result == null) {
            observation.success(0L, 0, transactionSource);
            return;
        }
        observeSqlStatus(observation,
                         result.status(),
                         result.affectedRows(),
                         cappedInt(result.inputCount()),
                         transactionSource);
    }

    private static void observeSqlStatus(ReactiveSqlObservation observation,
                                         BatchWriteResult.Status status,
                                         long affectedRows,
                                         int inputCount,
                                         SqlTransactionSource transactionSource) {
        if (observation == null) {
            return;
        }
        switch (status) {
            case COMMITTED, ENLISTED -> observation.success(
                    affectedRows, inputCount, transactionSource);
            case PARTIAL, ROLLED_BACK, UNKNOWN -> observation.error(
                    affectedRows,
                    inputCount,
                    new IllegalStateException("batch execution completed with " + status),
                    transactionSource);
        }
    }

    Flux<BatchChunkResult> observeChunks(BatchWriteRequest request, Flux<BatchChunkResult> source) {
        Flux<BatchChunkResult> safeSource = Objects.requireNonNull(
                source, "batch chunk observation source must not be null");
        if (!enabled) {
            return safeSource;
        }
        return observeChunks(request,
                transactionSources.resolve(SqlTransactionSource.INTERNAL),
                ignored -> safeSource,
                true);
    }

    Flux<BatchChunkResult> observeChunks(
            BatchWriteRequest request,
            Flux<BatchChunkResult> source,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        Flux<BatchChunkResult> safeSource = Objects.requireNonNull(
                source, "batch chunk observation source must not be null");
        if (!enabled) {
            return safeSource;
        }
        return observeChunks(request,
                Mono.just(Objects.requireNonNull(
                        resolution, "transaction resolution must not be null")),
                ignored -> safeSource,
                false);
    }

    Flux<BatchChunkResult> observeChunks(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Flux<BatchChunkResult>> sourceFactory) {
        return observeChunks(request, resolutions, sourceFactory, true);
    }

    private Flux<BatchChunkResult> observeChunks(
            BatchWriteRequest request,
            Mono<ReactiveTransactionSourceResolver.Resolution> resolutions,
            Function<ReactiveTransactionSourceResolver.Resolution, Flux<BatchChunkResult>> sourceFactory,
            boolean bindResolution) {
        Objects.requireNonNull(request, "batch write request must not be null");
        Mono<ReactiveTransactionSourceResolver.Resolution> safeResolutions = Objects.requireNonNull(
                resolutions, "transaction resolutions must not be null");
        Function<ReactiveTransactionSourceResolver.Resolution, Flux<BatchChunkResult>> safeSourceFactory =
                Objects.requireNonNull(sourceFactory, "batch chunk source factory must not be null");
        if (!enabled) {
            return safeResolutions.flatMapMany(safeSourceFactory);
        }
        return Flux.deferContextual(context -> {
            long startedAt = System.nanoTime();
            BatchObservationAccumulator summary = new BatchObservationAccumulator(request.options().mode());
            ReactiveSqlObservation sqlObservation = sqlEnabled ? sql.start(sqlRequest(request)) : null;
            BatchExecutionObservation.BatchWriteRequestView batchView = batchEnabled ? batchView(request) : null;
            AtomicReference<SqlTransactionSource> transactionSource = new AtomicReference<>(
                    SqlTransactionSource.INTERNAL);
            Mono<ReactiveTransactionSourceResolver.Resolution> observedResolutions = safeResolutions
                    .doOnError(error -> {
                        observeSqlError(sqlObservation, error, SqlTransactionSource.INTERNAL);
                        if (batchEnabled) {
                            observe(summary.failedSummary(
                                            batchView, System.nanoTime() - startedAt, error),
                                    SqlTransactionSource.INTERNAL);
                        }
                    });
            return observedResolutions
                    .flatMapMany(resolution -> {
                        transactionSource.set(resolution.source());
                        Flux<BatchChunkResult> source = Flux.defer(() -> safeSourceFactory.apply(resolution));
                        return (bindResolution ? resolution.bind(source) : source)
                            .doOnNext(chunk -> {
                                summary.add(chunk);
                                if (batchEnabled) {
                                    observe(BatchExecutionObservation.chunk(
                                            batchView, chunk, System.nanoTime() - startedAt), resolution.source());
                                }
                            })
                            .doOnComplete(() -> {
                                observeSqlStatus(sqlObservation,
                                                 summary.status(),
                                                 summary.affectedRows(),
                                                 cappedInt(summary.inputCount()),
                                                 resolution.source());
                                if (batchEnabled) {
                                    observe(summary.summary(
                                            batchView, System.nanoTime() - startedAt), resolution.source());
                                }
                            })
                            .doOnError(error -> {
                                if (batchEnabled) {
                                    observe(summary.failedSummary(
                                            batchView, System.nanoTime() - startedAt, error), resolution.source());
                                }
                                observeSqlError(sqlObservation, summary.affectedRows(),
                                                cappedInt(summary.inputCount()), error,
                                                resolution.source());
                            });
                    })
                    .doOnCancel(() -> observeSqlCancelled(
                            sqlObservation, summary.affectedRows(),
                            cappedInt(summary.inputCount()), transactionSource.get()));
        });
    }

    /** UNKNOWN 恢复查询走独立连接，不借用原批量事务，因此按自动提交记录。 */
    void observeRecovery(BatchExecutionObservation observation) {
        observe(observation, SqlTransactionSource.AUTO_COMMIT);
    }

    private void observeResult(BatchWriteResult result,
                               BatchExecutionObservation.BatchWriteRequestView batchView,
                               long durationNanos,
                               SqlTransactionSource transactionSource) {
        if (result == null) {
            return;
        }
        result.chunks().forEach(chunk -> observe(
                BatchExecutionObservation.chunk(batchView, chunk, durationNanos), transactionSource));
        observe(BatchExecutionObservation.summary(batchView, result, durationNanos), transactionSource);
    }

    private void observeError(Throwable error,
                              BatchExecutionObservation.BatchWriteRequestView batchView,
                              long durationNanos,
                              SqlTransactionSource transactionSource) {
        if (error instanceof BatchWriteException batchError) {
            observeResult(batchError.result(), batchView, durationNanos, transactionSource);
            return;
        }
        observe(BatchExecutionObservation.failedSummary(batchView, durationNanos, error), transactionSource);
    }

    private void observe(BatchExecutionObservation observation, SqlTransactionSource transactionSource) {
        if (batchEnabled) {
            observer.onExecution(observation, transactionSource);
        }
    }

    private static void observeSqlError(ReactiveSqlObservation observation,
                                        Throwable error,
                                        SqlTransactionSource source) {
        observeSqlError(observation, errorAffectedRows(error), errorInputCount(error), error, source);
    }

    private static void observeSqlError(ReactiveSqlObservation observation,
                                        long affectedRows,
                                        int inputCount,
                                        Throwable error,
                                        SqlTransactionSource source) {
        if (observation != null) {
            observation.error(affectedRows, inputCount, error, source);
        }
    }

    private static void observeSqlCancelled(ReactiveSqlObservation observation,
                                            long affectedRows,
                                            int inputCount,
                                            SqlTransactionSource source) {
        if (observation != null) {
            observation.cancelled(affectedRows, inputCount, source);
        }
    }

    private static BatchExecutionObservation.BatchWriteRequestView batchView(BatchWriteRequest request) {
        return new BatchExecutionObservation.BatchWriteRequestView(request.sql(),
                                                                  request.options().mode(),
                                                                  request.parameterCount(),
                                                                  SqlExecutionBackend.R2DBC);
    }

    private static int cappedInt(long value) {
        return value > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) value;
    }

    private static long errorAffectedRows(Throwable error) {
        return error instanceof BatchWriteException batchError
                ? batchError.result().affectedRows()
                : 0L;
    }

    private static int errorInputCount(Throwable error) {
        return error instanceof BatchWriteException batchError
                ? cappedInt(batchError.result().inputCount())
                : 0;
    }

    private static ReactiveSqlObservation.Request sqlRequest(BatchWriteRequest request) {
        return new ReactiveSqlObservation.Request(SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                                  request.sql(),
                                                  request.parameterCount(),
                                                  0,
                                                  null);
    }

}
