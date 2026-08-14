package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 批量执行的分片、汇总和普通 SQL 摘要观测。
 *
 * <p>批量没有外部事务时一定由 flying-orm 管理事务，因此本地来源为 {@code INTERNAL}；检测到外部事务时
 * 自动改为 {@code EXTERNAL}。这里不保存 rows Publisher，也不会为了日志重新订阅输入。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class ReactiveBatchExecutionObservationSupport {

    private final ReactiveSqlExecutionObservationSupport sql;

    private final BatchExecutionObserver observer;

    private final ReactiveTransactionSourceResolver transactionSources;

    ReactiveBatchExecutionObservationSupport(ReactiveSqlExecutionObservationSupport sql,
                                             BatchExecutionObserver observer,
                                             ReactiveTransactionSourceResolver transactionSources) {
        this.sql = Objects.requireNonNull(sql, "sql observation support must not be null");
        this.observer = Objects.requireNonNull(observer, "batch execution observer must not be null");
        this.transactionSources = Objects.requireNonNull(transactionSources,
                                                         "transaction source resolver must not be null");
    }

    Mono<BatchWriteResult> observeResult(BatchWriteRequest request, Mono<BatchWriteResult> source) {
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            BatchExecutionObservation.BatchWriteRequestView batchView = batchView(request);
            return transactionSources.resolve(SqlTransactionSource.INTERNAL)
                    .flatMap(resolution -> resolution.bind(source)
                            .doOnSuccess(result -> sql.observe(
                                    observed,
                                    SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                    request.sql(),
                                    request.parameterCount(),
                                    result == null ? 0 : cappedInt(result.inputCount()),
                                    result == null ? 0L : result.affectedRows(),
                                    startedAt,
                                    SqlExecutionStatus.SUCCESS,
                                    null,
                                    null,
                                    resolution.source()))
                            .doOnSuccess(result -> observeResult(result,
                                                                 batchView,
                                                                 System.nanoTime() - startedAt,
                                                                 resolution.source()))
                            .doOnError(error -> {
                                sql.observe(observed,
                                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                            request.sql(),
                                            request.parameterCount(),
                                            0,
                                            0L,
                                            startedAt,
                                            SqlExecutionStatus.ERROR,
                                            error,
                                            null,
                                            resolution.source());
                                observeError(error,
                                             batchView,
                                             System.nanoTime() - startedAt,
                                             resolution.source());
                            })
                            .doOnCancel(() -> sql.observe(
                                    observed,
                                    SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                    request.sql(),
                                    request.parameterCount(),
                                    0,
                                    0L,
                                    startedAt,
                                    SqlExecutionStatus.CANCELLED,
                                    null,
                                    null,
                                    resolution.source())))
                    .doOnError(error -> sql.observe(
                            observed,
                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                            request.sql(),
                            request.parameterCount(),
                            0,
                            0L,
                            startedAt,
                            SqlExecutionStatus.ERROR,
                            error,
                            null,
                            SqlTransactionSource.INTERNAL))
                    .doOnCancel(() -> sql.observe(
                            observed,
                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                            request.sql(),
                            request.parameterCount(),
                            0,
                            0L,
                            startedAt,
                            SqlExecutionStatus.CANCELLED,
                            null,
                            null,
                            SqlTransactionSource.INTERNAL));
        });
    }

    Flux<BatchChunkResult> observeChunks(BatchWriteRequest request, Flux<BatchChunkResult> source) {
        return Flux.defer(() -> {
            long startedAt = System.nanoTime();
            BatchObservationAccumulator summary = new BatchObservationAccumulator(request.options().mode());
            AtomicBoolean observed = new AtomicBoolean();
            BatchExecutionObservation.BatchWriteRequestView batchView = batchView(request);
            return transactionSources.resolve(SqlTransactionSource.INTERNAL)
                    .flatMapMany(resolution -> resolution.bind(source)
                            .doOnNext(chunk -> {
                                summary.add(chunk);
                                observe(BatchExecutionObservation.chunk(
                                        batchView, chunk, System.nanoTime() - startedAt), resolution.source());
                            })
                            .doOnComplete(() -> {
                                sql.observe(observed,
                                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                            request.sql(),
                                            request.parameterCount(),
                                            cappedInt(summary.inputCount()),
                                            summary.affectedRows(),
                                            startedAt,
                                            SqlExecutionStatus.SUCCESS,
                                            null,
                                            null,
                                            resolution.source());
                                observe(summary.summary(batchView, System.nanoTime() - startedAt), resolution.source());
                            })
                            .doOnError(error -> {
                                sql.observe(observed,
                                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                            request.sql(),
                                            request.parameterCount(),
                                            cappedInt(summary.inputCount()),
                                            summary.affectedRows(),
                                            startedAt,
                                            SqlExecutionStatus.ERROR,
                                            error,
                                            null,
                                            resolution.source());
                                observe(BatchExecutionObservation.failedSummary(
                                        batchView, System.nanoTime() - startedAt, error), resolution.source());
                            })
                            .doOnCancel(() -> sql.observe(
                                    observed,
                                    SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                                    request.sql(),
                                    request.parameterCount(),
                                    cappedInt(summary.inputCount()),
                                    summary.affectedRows(),
                                    startedAt,
                                    SqlExecutionStatus.CANCELLED,
                                    null,
                                    null,
                                    resolution.source())))
                    .doOnError(error -> sql.observe(
                            observed,
                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                            request.sql(),
                            request.parameterCount(),
                            cappedInt(summary.inputCount()),
                            summary.affectedRows(),
                            startedAt,
                            SqlExecutionStatus.ERROR,
                            error,
                            null,
                            SqlTransactionSource.INTERNAL))
                    .doOnCancel(() -> sql.observe(
                            observed,
                            SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                            request.sql(),
                            request.parameterCount(),
                            cappedInt(summary.inputCount()),
                            summary.affectedRows(),
                            startedAt,
                            SqlExecutionStatus.CANCELLED,
                            null,
                            null,
                            SqlTransactionSource.INTERNAL));
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
        try {
            observer.onExecution(observation, transactionSource);
        } catch (RuntimeException ignored) {
            // 批量观察器的普通故障不影响 SQL 执行主链路；JVM 致命错误由安全包装器原样传播。
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
}
