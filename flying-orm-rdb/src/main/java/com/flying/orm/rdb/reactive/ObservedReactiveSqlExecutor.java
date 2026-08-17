package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 给自定义 ReactiveSqlExecutor 补上普通 SQL 和批量观测。
 *
 * <p>R2dbcSqlExecutor 自己走内置观测路径，其他执行器通过接口的 {@code withObserver/withObservers}
 * 得到本装饰器。它只记录信号，不改变 SQL、事务和生成键结果；observer 的普通异常不会影响数据库操作，
 * 但异常图中的 JVM 致命错误仍立即冒泡。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
final class ObservedReactiveSqlExecutor implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {

    private final ReactiveSqlExecutor delegate;

    private final ReactiveSqlExecutionObservationSupport observationSupport;

    private ObservedReactiveSqlExecutor(ReactiveSqlExecutor delegate,
                                        SqlExecutionObserver observer,
                                        BatchExecutionObserver batchObserver) {
        this.delegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
        this.observationSupport = ReactiveSqlExecutionObservationSupport.create(
                observer, batchObserver, delegate::currentTransaction);
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, SqlExecutionObserver observer) {
        return new ObservedReactiveSqlExecutor(delegate, observer, BatchExecutionObserver.noop());
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate,
                                      SqlExecutionObserver observer,
                                      BatchExecutionObserver batchObserver) {
        return new ObservedReactiveSqlExecutor(delegate, observer, batchObserver);
    }

    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return delegate.currentTransaction();
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                           safeRequest.sql(),
                           safeRequest.parameters().size(),
                           0,
                           safeRequest.parameters(),
                           Flux.defer(() -> delegate.query(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                           safeRequest.sql(),
                           safeRequest.parameters().size(),
                            0,
                            safeRequest.parameters(),
                            Flux.defer(() -> delegate.query(safeRequest, safeOptions)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)),
                            safeOptions);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                           safeRequest.sql(),
                           safeRequest.parameters().size(),
                           0,
                           safeRequest.parameters(),
                           Mono.defer(() -> delegate.rowsUpdated(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                           safeRequest.sql(),
                           safeRequest.parameters().size(),
                            0,
                            safeRequest.parameters(),
                            Mono.defer(() -> delegate.rowsUpdated(safeRequest, safeOptions)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)),
                            safeOptions);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate.rowsUpdatedReturningKeys(safeRequest, safeOptions)
                                                              .onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest.sql(), safeRequest.parameters().size(), 0,
                                               safeRequest.parameters(), source,
                                               SqlWriteResult::affectedRows,
                                               safeOptions);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                         SqlExecutionOptions options,
                                                         String generatedKeyColumn) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        String safeColumn = Objects.requireNonNull(generatedKeyColumn,
                                                   "generated key column must not be null");
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate.rowsUpdatedReturningKeys(
                safeRequest, safeOptions, safeColumn).onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest.sql(), safeRequest.parameters().size(), 0,
                                               safeRequest.parameters(), source,
                                               SqlWriteResult::affectedRows,
                                               safeOptions);
    }

    /** 原子保护写入作为一个事务工作单元观测，侧索引内部参数不单独暴露。 */
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate.atomicProtectedWrite(safeWork, safeOptions)
                .onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(
                SqlExecutionOperation.UPDATE,
                safeWork.writeRequest().sql(),
                safeWork.writeRequest().parameters().size(),
                0,
                safeWork.writeRequest().parameters(),
                source,
                SqlWriteResult::affectedRows,
                safeOptions);
    }

    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                SqlExecutionOptions options) {
        if (delegate instanceof ConnectionScopedReactiveSqlExecutor scoped) {
            return scoped.executeInConnection(sequence, options);
        }
        return Mono.error(new UnsupportedOperationException(
                "wrapped reactive SQL executor does not support connection-scoped execution"));
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchResult(safeRequest,
                                                     Mono.defer(() -> delegate.writeBatch(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "protected batch request must not be null");
        return observationSupport.observeBatchResult(
                safeRequest,
                Mono.defer(() -> delegate.writeProtectedBatch(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchChunks(safeRequest,
                                                      Flux.defer(() -> delegate.writeBatchChunks(safeRequest)
                                                   .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "protected batch request must not be null");
        return observationSupport.observeBatchChunks(
                safeRequest,
                Flux.defer(() -> delegate.writeProtectedBatchChunks(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        BatchChunkResult.RecoveryToken safeToken = Objects.requireNonNull(token,
                                                                          "batch recovery token must not be null");
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            return delegate.resolveUnknown(safeToken)
                           .doOnSuccess(resolution -> observationSupport.observeRecovery(
                                   BatchExecutionObservation.recovery(
                                   resolution,
                                   System.nanoTime() - startedAt,
                                   null)))
                           .onErrorMap(ReactiveSqlExecutionProtection::translate)
                           .doOnError(error -> observationSupport.observeRecovery(
                                   BatchExecutionObservation.recovery(
                                   BatchResolution.unknown(safeToken),
                                   System.nanoTime() - startedAt,
                                   error)));
        });
    }

}
