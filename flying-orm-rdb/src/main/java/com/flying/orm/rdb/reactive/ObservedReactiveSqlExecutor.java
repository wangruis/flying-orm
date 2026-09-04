package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.Deque;
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
final class ObservedReactiveSqlExecutor extends ForwardingReactiveSqlExecutor {

    private final ReactiveSqlExecutionObservationSupport observationSupport;
    private final SqlExecutionObserver observer;
    private final BatchExecutionObserver batchObserver;

    private ObservedReactiveSqlExecutor(ReactiveSqlExecutor delegate,
                                        SqlExecutionObserver observer,
                                        BatchExecutionObserver batchObserver) {
        super(delegate);
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.batchObserver = Objects.requireNonNull(
                batchObserver, "batch execution observer must not be null");
        this.observationSupport = ReactiveSqlExecutionObservationSupport.create(
                this.observer, this.batchObserver, delegate::currentTransaction);
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, SqlExecutionObserver observer) {
        return create(delegate, observer, BatchExecutionObserver.noop());
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate,
                                      SqlExecutionObserver observer,
                                      BatchExecutionObserver batchObserver) {
        ReactiveSqlExecutor safeDelegate = Objects.requireNonNull(
                delegate, "reactive sql executor must not be null");
        SqlExecutionObserver configuredSql = Objects.requireNonNull(
                observer, "sql execution observer must not be null");
        BatchExecutionObserver configuredBatch = Objects.requireNonNull(
                batchObserver, "batch execution observer must not be null");
        Deque<ObservedReactiveSqlExecutor> existingObservers = new ArrayDeque<>();
        safeDelegate = ForwardingReactiveSqlExecutor.withoutPolicy(
                safeDelegate,
                ObservedReactiveSqlExecutor.class,
                existing -> existingObservers.addFirst((ObservedReactiveSqlExecutor) existing));
        SqlExecutionObserver previousSql = SqlExecutionObserver.noop();
        BatchExecutionObserver previousBatch = BatchExecutionObserver.noop();
        for (ObservedReactiveSqlExecutor existing : existingObservers) {
            previousSql = SqlExecutionObservers.composite(previousSql, existing.observer);
            previousBatch = BatchExecutionObserver.composite(previousBatch, existing.batchObserver);
        }
        configuredSql = SqlExecutionObservers.composite(previousSql, configuredSql);
        configuredBatch = BatchExecutionObserver.composite(previousBatch, configuredBatch);
        if (!configuredSql.enabled() && !configuredBatch.enabled()) {
            return safeDelegate;
        }
        return ForwardingReactiveSqlExecutor.preservingScopedCapability(
                safeDelegate,
                new ObservedReactiveSqlExecutor(safeDelegate, configuredSql, configuredBatch));
    }

    @Override
    ForwardingReactiveSqlExecutor redecoratePolicy(ReactiveSqlExecutor delegate) {
        return new ObservedReactiveSqlExecutor(delegate, observer, batchObserver);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                           safeRequest,
                           0,
                           Flux.defer(() -> delegate().query(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                           safeRequest,
                            0,
                            Flux.defer(() -> delegate().query(safeRequest, safeOptions)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)),
                            safeOptions);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                           safeRequest,
                           0,
                           Mono.defer(() -> delegate().rowsUpdated(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                           safeRequest,
                            0,
                            Mono.defer(() -> delegate().rowsUpdated(safeRequest, safeOptions)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)),
                            safeOptions);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate().rowsUpdatedReturningKeys(safeRequest, safeOptions)
                                                              .onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest, 0, source,
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
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate().rowsUpdatedReturningKeys(
                safeRequest, safeOptions, safeColumn).onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest, 0, source,
                                               SqlWriteResult::affectedRows,
                                               safeOptions);
    }

    /** 原子保护写入作为一个事务工作单元观测，侧索引内部参数不单独暴露。 */
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        Mono<SqlWriteResult> source = Mono.defer(() -> delegate().atomicProtectedWrite(safeWork, safeOptions)
                .onErrorMap(ReactiveSqlExecutionProtection::translate));
        return observationSupport.observeMono(
                SqlExecutionOperation.UPDATE,
                safeWork.writeRequest(),
                0,
                source,
                SqlWriteResult::affectedRows,
                safeOptions);
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchResult(safeRequest,
                                                     Mono.defer(() -> delegate().writeBatch(safeRequest)
                                                    .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "protected batch request must not be null");
        return observationSupport.observeBatchResult(
                safeRequest,
                Mono.defer(() -> delegate().writeProtectedBatch(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "batch evidence request must not be null");
        return observationSupport.observeBatchEvidence(Mono.defer(
                () -> delegate().writeBatchEvidence(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatchEvidence(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "protected batch evidence request must not be null");
        return observationSupport.observeBatchEvidence(Mono.defer(
                () -> delegate().writeProtectedBatchEvidence(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchChunks(safeRequest,
                                                      Flux.defer(() -> delegate().writeBatchChunks(safeRequest)
                                                   .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(
                request, "protected batch request must not be null");
        return observationSupport.observeBatchChunks(
                safeRequest,
                Flux.defer(() -> delegate().writeProtectedBatchChunks(safeRequest)
                        .onErrorMap(ReactiveSqlExecutionProtection::translate)));
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        BatchChunkResult.RecoveryToken safeToken = Objects.requireNonNull(token,
                                                                          "batch recovery token must not be null");
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            return delegate().resolveUnknown(safeToken)
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
