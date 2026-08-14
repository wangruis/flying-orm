package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.isolation.IsolationContexts;
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

import java.util.Objects;

/**
 * 基于 R2DBC SPI 的核心 SQL 执行器。查询、更新、批量写入、连接获取和关闭都留在 Publisher 链中，
 * 本类不会调用 {@code block()}，因此可以在 Reactor 事件循环和高并发服务中直接使用。
 *
 * <p>SQL 渲染层统一产出问号占位符，本执行器在创建 Statement 前按驱动元数据改写 bind marker，
 * 再按原参数顺序绑定。每次订阅独占自己的连接和局部计数器；执行器本身只保存线程安全或只读依赖，
 * 可以作为单例共享。</p>
 *
 * <p>执行保护分成连接获取超时、整次执行超时和最大返回行数。普通批量更新走一个 Statement，
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
    private final R2dbcConnectionInvalidator connectionInvalidator;
    private final R2dbcTransactionParticipant transactionParticipant;
    private R2dbcSqlExecutor(ConnectionFactory connectionFactory) {
        this(connectionFactory,
             SqlExecutionObserver.noop(),
             BatchExecutionObserver.noop(),
             SqlExecutionOptions.safeDefaults(),
             R2dbcConnectionInvalidator.failClosed(),
             R2dbcTransactionParticipant.none());
    }
    private R2dbcSqlExecutor(ConnectionFactory connectionFactory,
                             SqlExecutionObserver observer,
                             BatchExecutionObserver batchObserver,
                             SqlExecutionOptions executionOptions,
                             R2dbcConnectionInvalidator connectionInvalidator,
                             R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connection factory must not be null");
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.batchObserver = Objects.requireNonNull(batchObserver, "batch execution observer must not be null");
        this.executionOptions = Objects.requireNonNull(executionOptions, "sql execution options must not be null");
        this.connectionInvalidator = Objects.requireNonNull(connectionInvalidator,
                                                             "connection invalidator must not be null");
        this.transactionParticipant = ResolvedTransactionParticipant.wrap(Objects.requireNonNull(
                transactionParticipant, "transaction participant must not be null"));
        // 驱动能力只在装配时识别一次，热路径不重复读取 ConnectionFactory 元数据。
        this.bindMarkers = R2dbcBindMarkers.from(connectionFactory);
        this.observationSupport = ReactiveSqlExecutionObservationSupport.create(
                observer, batchObserver, this.transactionParticipant::currentTransaction);
        this.executionSession = new R2dbcExecutionSession(connectionFactory,
                                                           bindMarkers,
                                                           observer,
                                                           connectionInvalidator,
                                                           this.transactionParticipant);
        this.sequenceExecutor = new R2dbcSequenceExecutor(executionSession, observationSupport);
        this.generatedKeyWriter = new R2dbcGeneratedKeyWriter(executionSession);
        BatchReceiptStore receiptStore = new BatchReceiptStore(
                connectionFactory, bindMarkers, observer, connectionInvalidator);
        this.batchWriter = new R2dbcBatchWriter(
                connectionFactory, receiptStore, bindMarkers, observer, batchObserver, connectionInvalidator,
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
     * 配置异常连接物理淘汰能力。连接池部署应提供与池实现匹配的 invalidator，使批量取消 rollback
     * 失败等状态不确定路径不会把污染连接重新放回池中；当前执行器保持不变。
     *
     * @param configuredInvalidator 普通关闭与异常物理淘汰的明确边界
     * @return 使用新失效能力的执行器副本
     */
    public R2dbcSqlExecutor withConnectionInvalidator(R2dbcConnectionInvalidator configuredInvalidator) {
        return new R2dbcSqlExecutor(connectionFactory,
                                    observer,
                                    batchObserver,
                                    executionOptions,
                                    Objects.requireNonNull(configuredInvalidator,
                                                           "connection invalidator must not be null"),
                                    transactionParticipant);
    }
    /**
     * 接入上层事务绑定。参与者在每次订阅时读取当前事务，执行器本身仍然不可变并可并发共享。
     */
    public R2dbcSqlExecutor withTransactionParticipant(R2dbcTransactionParticipant participant) {
        return new R2dbcSqlExecutor(connectionFactory, observer, batchObserver, executionOptions,
                                    connectionInvalidator, Objects.requireNonNull(
                                            participant, "transaction participant must not be null"));
    }
    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.deferContextual(context -> transactionParticipant.currentTransaction(
                IsolationContexts.currentDatabaseKey(context)));
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
    private R2dbcSqlExecutor copy(SqlExecutionObserver configuredObserver,
                                  BatchExecutionObserver configuredBatchObserver,
                                  SqlExecutionOptions configuredExecutionOptions) {
        return new R2dbcSqlExecutor(connectionFactory,
                                    configuredObserver,
                                    configuredBatchObserver,
                                    configuredExecutionOptions,
                                    connectionInvalidator,
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
        // usingWhen 把连接生命周期绑到订阅上；完成、失败或取消都会触发异步 close。
        Flux<DynamicRow> source = executionSession.withStatement(
                safeRequest,
                safeOptions,
                SqlExecutionOperation.QUERY,
                (statement, largeObjects) -> Flux.from(statement.execute())
                                 .concatMap(result -> R2dbcExecutionSession.mapRows(
                                         result, safeOptions, largeObjects), 1),
                rows -> executionSession.protectRows(rows, safeRequest.sql(), safeOptions))
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeFlux(SqlExecutionOperation.QUERY,
                                              safeRequest.sql(),
                                              safeRequest.parameters().size(),
                                              0,
                                              safeRequest.parameters(),
                                              source);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return rowsUpdated(request, executionOptions);
    }
    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        Mono<Long> source = executionSession.withStatementMono(
                safeRequest,
                options,
                SqlExecutionOperation.UPDATE,
                (statement, ignored) -> Flux.from(statement.execute())
                                 .flatMap(Result::getRowsUpdated)
                                 .reduce(0L, R2dbcExecutionCounts::add))
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                              safeRequest.sql(),
                                              safeRequest.parameters().size(),
                                              0,
                                              safeRequest.parameters(),
                                              source);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        Mono<SqlWriteResult> source = generatedKeyWriter.write(safeRequest, options)
                                                        .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                              safeRequest.sql(), safeRequest.parameters().size(), 0,
                                              safeRequest.parameters(), source,
                                              SqlWriteResult::affectedRows);
    }
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        Mono<SqlWriteResult> source = new R2dbcProtectedWriteExecutor(new R2dbcBatchConnectionLifecycle(
                connectionFactory, observer, connectionInvalidator, transactionParticipant), executionSession)
                .execute(safeWork, safeOptions);
        return observationSupport.observeMono(
                SqlExecutionOperation.UPDATE,
                safeWork.writeRequest().sql(),
                safeWork.writeRequest().parameters().size(),
                0,
                safeWork.writeRequest().parameters(),
                source,
                SqlWriteResult::affectedRows);
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
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchResult(
                safeRequest,
                batchWriter.write(safeRequest).onErrorMap(ReactiveSqlExecutionProtection::translate));
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        return writeBatch(request);
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return observationSupport.observeBatchChunks(
                safeRequest,
                batchWriter.writeChunks(safeRequest).onErrorMap(ReactiveSqlExecutionProtection::translate));
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
