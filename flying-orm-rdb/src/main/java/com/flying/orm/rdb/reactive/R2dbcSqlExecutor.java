package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 基于 R2DBC SPI 的核心 SQL 执行器。查询、更新、批量写入、连接获取和关闭都留在 Publisher 链中，
 * 本类不会调用 {@code block()}，因此可以在 Reactor 事件循环和高并发服务中直接使用。
 *
 * <p>SQL 渲染层统一产出问号占位符，本执行器在创建 Statement 前按驱动元数据改写 bind marker，
 * 再按原参数顺序绑定。每次订阅独占自己的连接和局部计数器；执行器本身只保存线程安全或只读依赖，
 * 可以作为单例共享。</p>
 *
 * <p>连接排队和获取超时由上层连接池治理；连接可用后，执行保护限制整次 SQL 执行、返回行数、结果内存和 LOB。
 * 普通批量更新走一个 Statement，
 * 带 ATOMIC/INDEPENDENT、回执恢复和乐观锁语义的批量请求交给 {@link R2dbcBatchWriter}。</p>
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class R2dbcSqlExecutor implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {
    private final ConnectionFactory connectionFactory;
    private final R2dbcBindMarkers bindMarkers;
    /**
     * 连接、Statement 与收尾动作统一从这里经过，查询、更新和同连接序列不再各自维护资源语义。
     */
    private final R2dbcExecutionSession executionSession;
    /**
     * setup、work、cleanup 在同一连接上的顺序编排由专用协作类处理，主执行器只保留入口职责。
     */
    private final R2dbcSequenceExecutor sequenceExecutor;
    private final R2dbcGeneratedKeyWriter generatedKeyWriter;
    private final R2dbcBatchWriter batchWriter;
    private final R2dbcBatchRecoveryResolver recoveryResolver;
    private final SqlExecutionObserver observer;
    private final BatchExecutionObserver batchObserver;
    private final ReactiveSqlExecutionObservationSupport observationSupport;
    private final SqlExecutionOptions executionOptions;
    private final BatchMemoryLimits batchMemoryLimits;
    private final R2dbcTransactionParticipant transactionParticipant;
    private R2dbcSqlExecutor(ConnectionFactory connectionFactory) {
        this(connectionFactory,
             R2dbcBindMarkers.from(connectionFactory),
             SqlExecutionObserver.noop(),
             BatchExecutionObserver.noop(),
             SqlExecutionOptions.safeDefaults(),
             BatchMemoryLimits.defaults(),
             R2dbcTransactionParticipant.none());
    }
    private R2dbcSqlExecutor(ConnectionFactory connectionFactory,
                             R2dbcBindMarkers bindMarkers,
                             SqlExecutionObserver observer,
                             BatchExecutionObserver batchObserver,
                             SqlExecutionOptions executionOptions,
                             BatchMemoryLimits batchMemoryLimits,
                             R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connection factory must not be null");
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.batchObserver = Objects.requireNonNull(batchObserver, "batch execution observer must not be null");
        this.executionOptions = Objects.requireNonNull(executionOptions, "sql execution options must not be null");
        this.batchMemoryLimits = Objects.requireNonNull(
                batchMemoryLimits, "batch memory limits must not be null");
        this.transactionParticipant = ResolvedTransactionParticipant.wrap(Objects.requireNonNull(
                transactionParticipant, "transaction participant must not be null"));
        // 驱动能力只在首次装配时识别一次；不可变派生执行器复用同一 marker family。
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "bind marker adapter must not be null");
        this.observationSupport = ReactiveSqlExecutionObservationSupport.create(
                observer, batchObserver, this::currentTransaction);
        this.executionSession = new R2dbcExecutionSession(connectionFactory,
                                                           bindMarkers,
                                                           observer,
                                                           this.transactionParticipant);
        this.sequenceExecutor = new R2dbcSequenceExecutor(executionSession, observationSupport);
        this.generatedKeyWriter = new R2dbcGeneratedKeyWriter(executionSession);
        BatchReceiptStore receiptStore = new BatchReceiptStore(
                connectionFactory, bindMarkers, observer);
        this.batchWriter = new R2dbcBatchWriter(
                connectionFactory, receiptStore, bindMarkers, observer, batchObserver,
                this.transactionParticipant);
        this.recoveryResolver = new R2dbcBatchRecoveryResolver(receiptStore, observationSupport);
    }
    /**
     * 创建 R2DBC SQL 执行器。
     *
     * @param connectionFactory R2DBC 连接工厂
     * @return R2DBC SQL 执行器
     */
    public static R2dbcSqlExecutor create(ConnectionFactory connectionFactory) {
        return new R2dbcSqlExecutor(connectionFactory);
    }
    /**
     * 接入上层事务绑定。参与者在每次订阅时读取当前事务，执行器本身仍然不可变并可并发共享。
     */
    public R2dbcSqlExecutor withTransactionParticipant(R2dbcTransactionParticipant participant) {
        return new R2dbcSqlExecutor(connectionFactory, bindMarkers, observer, batchObserver, executionOptions,
                                    batchMemoryLimits, Objects.requireNonNull(
                                            participant, "transaction participant must not be null"));
    }
    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.defer(() -> Objects.requireNonNull(
                transactionParticipant.currentTransaction(), "current transaction publisher must not be null"));
    }
    /**
     * 追加普通 SQL 观测器，并保留已经设置的执行保护和批量观测器。
     *
     * <p>R2DBC 内核直接保存合并后的只读 observer，不再套一层通用代理。这样查询、写入以及同连接
     * SQL 序列共用一条观测路径，热路径也少一次 Publisher 转发。</p>
     *
     * @param additionalObserver 要追加的 SQL 观测器
     * @return 配置完成的新执行器，当前实例保持不变
     */
    @Override
    public R2dbcSqlExecutor withObserver(SqlExecutionObserver additionalObserver) {
        SqlExecutionObserver safeObserver = Objects.requireNonNull(additionalObserver,
                                                                   "sql execution observer must not be null");
        return copy(SqlExecutionObservers.composite(observer, safeObserver), batchObserver, executionOptions);
    }
    /**
     * 追加批量分片观测器。两个 observer 会分别执行；普通故障不会挡住另一个，也不会改变数据库结果，
     * 异常图中的 JVM 致命错误仍原样传播。
     *
     * @param additionalObserver 要追加的批量观测器
     * @return 配置完成的新执行器
     */
    @Override
    public R2dbcSqlExecutor withBatchObserver(BatchExecutionObserver additionalObserver) {
        BatchExecutionObserver safeObserver = Objects.requireNonNull(additionalObserver,
                                                                     "batch execution observer must not be null");
        return copy(observer,
                    ReactiveSqlExecutionObservationSupport.combineBatchObservers(batchObserver, safeObserver),
                    executionOptions);
    }
    /**
     * 一次追加普通 SQL 和批量观测器，语义等同于连续调用两个单独方法，但只创建一个执行器副本。
     */
    @Override
    public R2dbcSqlExecutor withObservers(SqlExecutionObserver additionalObserver,
                                          BatchExecutionObserver additionalBatchObserver) {
        SqlExecutionObserver safeObserver = Objects.requireNonNull(additionalObserver,
                                                                   "sql execution observer must not be null");
        BatchExecutionObserver safeBatchObserver = Objects.requireNonNull(
                additionalBatchObserver, "batch execution observer must not be null");
        return copy(SqlExecutionObservers.composite(observer, safeObserver),
                    ReactiveSqlExecutionObservationSupport.combineBatchObservers(batchObserver, safeBatchObserver),
                    executionOptions);
    }
    /**
     * 替换无参执行方法使用的默认保护。显式传给 query/rowsUpdated 的选项仍然优先。
     */
    @Override
    public R2dbcSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return copy(observer,
                    batchObserver,
                    Objects.requireNonNull(options, "sql execution options must not be null"));
    }

    /** 批量硬上限只保存在批量入口，不给普通 SQL 增加转发装饰器。 */
    @Override
    public R2dbcSqlExecutor withBatchMemoryLimits(BatchMemoryLimits limits) {
        return new R2dbcSqlExecutor(connectionFactory,
                                    bindMarkers,
                                    observer,
                                    batchObserver,
                                    executionOptions,
                                    Objects.requireNonNull(limits, "batch memory limits must not be null"),
                                    transactionParticipant);
    }
    private R2dbcSqlExecutor copy(SqlExecutionObserver configuredObserver,
                                  BatchExecutionObserver configuredBatchObserver,
                                  SqlExecutionOptions configuredExecutionOptions) {
        return new R2dbcSqlExecutor(connectionFactory,
                                    bindMarkers,
                                    configuredObserver,
                                    configuredBatchObserver,
                                    configuredExecutionOptions,
                                    batchMemoryLimits,
                                    transactionParticipant);
    }
    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return query(request, executionOptions);
    }
    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        // usingWhen 把连接生命周期绑到订阅上；完成、失败或取消都会触发异步 close。
        Flux<DynamicRow> source = executionSession.withPreparedStatement(
                safeRequest, executionParameters, safeOptions, SqlExecutionOperation.QUERY,
                (statement, largeObjects) -> Flux.from(statement.execute())
                        .concatMap(result -> R2dbcExecutionSession.mapRows(
                                result, safeOptions, largeObjects), 1),
                rows -> executionSession.protectRows(rows, safeRequest.sql(), safeOptions))
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                                               safeRequest,
                                               executionParameters,
                                               0,
                                               source,
                                               safeOptions);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return rowsUpdated(request, executionOptions);
    }
    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        Mono<Long> source = executionSession.withPreparedStatementMono(
                safeRequest, executionParameters, safeOptions, SqlExecutionOperation.UPDATE,
                (statement, ignored) -> Flux.from(statement.execute())
                        .flatMap(Result::getRowsUpdated)
                        .reduce(0L, R2dbcExecutionCounts::add))
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest,
                                               executionParameters,
                                               0,
                                               source,
                                               rows -> rows == null ? 0L : rows,
                                               safeOptions);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return writeReturningKeys(request, options, null);
    }

    private Mono<SqlWriteResult> writeReturningKeys(SqlRequest request,
                                                    SqlExecutionOptions options,
                                                    String generatedKeyColumn) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        Mono<SqlWriteResult> source = generatedKeyWriter.write(
                safeRequest, executionParameters, safeOptions, generatedKeyColumn)
                                                        .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               safeRequest, executionParameters, 0, source,
                                               SqlWriteResult::affectedRows,
                                               safeOptions);
    }

    @InternalApi
    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                         SqlExecutionOptions options,
                                                         String generatedKeyColumn) {
        return writeReturningKeys(
                request, options, Objects.requireNonNull(
                        generatedKeyColumn, "generated key column must not be null"));
    }
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        Mono<SqlWriteResult> source = new R2dbcProtectedWriteExecutor(new R2dbcBatchConnectionLifecycle(
                connectionFactory, observer, transactionParticipant), executionSession, bindMarkers)
                .execute(safeWork, safeOptions);
        return observationSupport.observeMono(
                SqlExecutionOperation.UPDATE,
                safeWork.writeRequest(),
                0,
                source,
                SqlWriteResult::affectedRows,
                safeOptions);
    }

    /**
     * 整组 SQL 只创建一次连接。setup 和 work 顺序执行；完成、失败或取消都会尝试 cleanup，
     * 然后关闭连接。逐条 work 仍走普通 SQL 观测器，因此监控能看到每条 DDL，而不是只有一个模糊汇总。
     */
    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                SqlExecutionOptions options) {
        SqlExecutionSequence safeSequence = Objects.requireNonNull(sequence,
                                                                    "SQL execution sequence must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return sequenceExecutor.execute(safeSequence, safeOptions)
                               .onErrorMap(ReactiveSqlExecutionProtection::translate);
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            batchMemoryLimits.check(safeRequest.options());
            return observationSupport.observeBatchResult(
                    safeRequest,
                    batchWriter.resolveTransaction(),
                    resolution -> batchWriter.write(safeRequest, resolution)
                            .onErrorMap(ReactiveSqlExecutionProtection::translate));
        });
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        return writeBatch(request);
    }

    @Override
    public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "batch evidence request must not be null");
            batchMemoryLimits.check(safeRequest.options());
            return batchWriter.resolveTransaction()
                    .flatMap(resolution -> batchWriter.writeEvidence(safeRequest, resolution))
                    .onErrorMap(ReactiveSqlExecutionProtection::translate);
        });
    }

    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatchEvidence(BatchWriteRequest request) {
        return writeBatchEvidence(request);
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            batchMemoryLimits.check(safeRequest.options());
            return observationSupport.observeBatchChunks(
                    safeRequest,
                    batchWriter.resolveTransaction(),
                    resolution -> batchWriter.writeChunks(safeRequest, resolution)
                            .onErrorMap(ReactiveSqlExecutionProtection::translate));
        });
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        return writeBatchChunks(request);
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        return recoveryResolver.resolveUnknown(token);
    }
}
