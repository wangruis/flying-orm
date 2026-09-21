package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
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
import io.r2dbc.spi.Connection;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.reactivestreams.Publisher;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基于 R2DBC SPI 的核心 SQL 执行器。查询、更新、批量写入、连接获取和上层释放都留在 Publisher 链中，
 * 本类不会调用 {@code block()}，因此可以在 Reactor 事件循环和高并发服务中直接使用。
 *
 * <p>SQL 渲染层统一产出问号占位符，本执行器在创建 Statement 前按显式方言改写 bind marker，
 * 再按原参数顺序绑定。每次订阅借用上层端口提供的连接并持有局部计数器；执行器本身只保存线程安全或只读依赖，
 * 可以作为单例共享。</p>
 *
 * <p>执行和清理时限由上层拥有；本执行器保留返回行数、结果内存和 LOB 容量保护。
 * 普通批量依次执行有界输入缓冲，上层决定多语句一致性。</p>
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class R2dbcSqlExecutor implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {
    private final R2dbcConnectionAccess connectionAccess;
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
    private final SqlExecutionObserver observer;
    private final BatchExecutionObserver batchObserver;
    private final ReactiveSqlExecutionObservationSupport observationSupport;
    private final SqlExecutionOptions executionOptions;
    private final BatchMemoryLimits batchMemoryLimits;
    private final Connection boundConnection;
    private R2dbcSqlExecutor(R2dbcConnectionAccess connectionAccess, RdbDialect dialect) {
        this(connectionAccess,
             R2dbcBindMarkers.from(dialect),
             SqlExecutionObserver.noop(),
             BatchExecutionObserver.noop(),
             SqlExecutionOptions.safeDefaults(),
             BatchMemoryLimits.defaults(),
             null);
    }
    private R2dbcSqlExecutor(R2dbcConnectionAccess connectionAccess,
                             R2dbcBindMarkers bindMarkers,
                             SqlExecutionObserver observer,
                             BatchExecutionObserver batchObserver,
                             SqlExecutionOptions executionOptions,
                             BatchMemoryLimits batchMemoryLimits,
                             Connection boundConnection) {
        this.connectionAccess = Objects.requireNonNull(connectionAccess, "connection access must not be null");
        this.observer = SqlExecutionObservers.safe(Objects.requireNonNull(observer, "sql execution observer must not be null"));
        this.batchObserver = Objects.requireNonNull(batchObserver, "batch execution observer must not be null");
        this.executionOptions = Objects.requireNonNull(executionOptions, "sql execution options must not be null");
        this.batchMemoryLimits = Objects.requireNonNull(
                batchMemoryLimits, "batch memory limits must not be null");
        this.boundConnection = boundConnection;
        // 显式方言只在首次装配时编译一次；不可变派生执行器复用同一 marker family。
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "bind marker adapter must not be null");
        this.observationSupport = ReactiveSqlExecutionObservationSupport.create(
                observer, batchObserver);
        this.executionSession = new R2dbcExecutionSession(connectionAccess,
                                                           bindMarkers,
                                                           this.observer,
                                                           this.boundConnection);
        this.sequenceExecutor = new R2dbcSequenceExecutor(executionSession, observationSupport);
        this.generatedKeyWriter = new R2dbcGeneratedKeyWriter(executionSession);
        this.batchWriter = new R2dbcBatchWriter(
                executionSession, bindMarkers, observer, batchObserver);
    }
    /**
     * 创建 R2DBC SQL 执行器。
     *
     * @param connectionAccess 上层 R2DBC 连接获取与释放端口
     * @param dialect 上层显式选择的数据库方言
     * @return R2DBC SQL 执行器
     */
    public static R2dbcSqlExecutor create(R2dbcConnectionAccess connectionAccess, RdbDialect dialect) {
        return new R2dbcSqlExecutor(connectionAccess, dialect);
    }
    @InternalApi
    @Override
    public <T> Mono<T> withConnection(SqlRequest firstRequest,
            Function<ReactiveSqlExecutor, Mono<T>> work) {
        Objects.requireNonNull(firstRequest, "first request must not be null");
        Objects.requireNonNull(work, "connection work must not be null");
        return executionSession.withConnection(firstRequest,
                resource -> work.apply(new R2dbcSqlExecutor(connectionAccess, bindMarkers, observer,
                        batchObserver, executionOptions, batchMemoryLimits, resource.connection())),
                SqlExecutionOperation.UPDATE);
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
     * 追加普通批量观测器。两个 observer 会分别执行；普通故障不会挡住另一个，也不会改变数据库结果，
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
        return copy(observer, batchObserver,
                    Objects.requireNonNull(options, "sql execution options must not be null"));
    }

    /** 批量硬上限只保存在批量入口，不给普通 SQL 增加转发装饰器。 */
    @Override
    public R2dbcSqlExecutor withBatchMemoryLimits(BatchMemoryLimits limits) {
        return new R2dbcSqlExecutor(connectionAccess,
                                    bindMarkers,
                                    observer,
                                    batchObserver,
                                    executionOptions,
                                    Objects.requireNonNull(limits, "batch memory limits must not be null"),
                                    boundConnection);
    }
    private R2dbcSqlExecutor copy(SqlExecutionObserver configuredObserver,
                                  BatchExecutionObserver configuredBatchObserver,
                                  SqlExecutionOptions configuredExecutionOptions) {
        return new R2dbcSqlExecutor(connectionAccess,
                                    bindMarkers,
                                    configuredObserver,
                                    configuredBatchObserver,
                                    configuredExecutionOptions,
                                    batchMemoryLimits,
                                    boundConnection);
    }
    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return query(request, executionOptions);
    }
    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        // usingWhen 把连接生命周期绑到订阅上；完成、失败或取消都会触发上层异步 release。
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
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        if (!observationSupport.enabled()) {
            return executionSession.withPreparedStatementResource(
                    safeRequest, executionParameters, SqlExecutionOperation.UPDATE,
                    (statement, ignored) -> Flux.from(statement.execute())
                            .flatMap(Result::getRowsUpdated)
                            .reduce(0L, R2dbcExecutionCounts::add))
                    .onErrorMap(ReactiveSqlExecutionProtection::translate);
        }
        return Mono.defer(() -> observedRowsUpdated(
                safeRequest, safeOptions, executionParameters));
    }

    private Mono<Long> observedRowsUpdated(SqlRequest request,
                                           SqlExecutionOptions options,
                                           List<Object> executionParameters) {
        AtomicLong confirmedRows = new AtomicLong();
        Mono<Long> source = executionSession.withPreparedStatementResource(
                request, executionParameters, SqlExecutionOperation.UPDATE,
                (statement, ignored) -> R2dbcExecutionCounts.sum(
                        Flux.from(statement.execute()).flatMap(Result::getRowsUpdated), confirmedRows::set))
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               request,
                                               executionParameters,
                                               0,
                                               source,
                                               rows -> rows == null ? 0L : rows,
                                               confirmedRows::get,
                                               options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return writeReturningKeys(request, options, null);
    }

    private Mono<SqlWriteResult> writeReturningKeys(SqlRequest request,
                                                    SqlExecutionOptions options,
                                                    String generatedKeyColumn) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(safeRequest);
        if (!observationSupport.enabled()) {
            return generatedKeyWriter.write(
                    safeRequest, executionParameters, safeOptions, generatedKeyColumn)
                    .onErrorMap(ReactiveSqlExecutionProtection::translate);
        }
        return Mono.defer(() -> observedWriteReturningKeys(
                safeRequest, safeOptions, executionParameters, generatedKeyColumn));
    }

    private Mono<SqlWriteResult> observedWriteReturningKeys(SqlRequest request,
                                                             SqlExecutionOptions options,
                                                             List<Object> executionParameters,
                                                             String generatedKeyColumn) {
        AtomicLong confirmedRows = new AtomicLong();
        Mono<SqlWriteResult> source = generatedKeyWriter.write(
                request, executionParameters, options, generatedKeyColumn, confirmedRows::set)
                .onErrorMap(ReactiveSqlExecutionProtection::translate);
        return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                               request, executionParameters, 0, source,
                                               SqlWriteResult::affectedRows,
                                               confirmedRows::get,
                                               options);
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
    public Mono<SqlWriteResult> protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        R2dbcProtectedWriteExecutor executor = new R2dbcProtectedWriteExecutor(executionSession, bindMarkers);
        if (!observationSupport.enabled()) {
            return executor.execute(safeWork, safeOptions);
        }
        return Mono.defer(() -> observedProtectedWrite(executor, safeWork, safeOptions));
    }

    private Mono<SqlWriteResult> observedProtectedWrite(R2dbcProtectedWriteExecutor executor,
                                                        ProtectedWriteWork work,
                                                        SqlExecutionOptions options) {
        AtomicLong confirmedRows = new AtomicLong();
        Mono<SqlWriteResult> source = executor.execute(work, options, confirmedRows::set);
        List<Object> executionParameters = R2dbcExecutionSession.snapshotExecutionParameters(
                work.writeRequest());
        return observationSupport.observeMono(
                SqlExecutionOperation.UPDATE,
                work.writeRequest(),
                executionParameters,
                0,
                source,
                SqlWriteResult::affectedRows,
                confirmedRows::get,
                options);
    }

    /**
     * 整组 SQL 只借用一次上层连接。setup 和 work 顺序执行；完成、失败或取消都会尝试 cleanup，
     * 然后调用上层释放端口。逐条 work 仍走普通 SQL 观测器，因此监控能看到每条 DDL，而不是只有一个模糊汇总。
     */
    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                SqlExecutionOptions options) {
        SqlExecutionSequence safeSequence = Objects.requireNonNull(sequence,
                                                                    "SQL execution sequence must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        return sequenceExecutor.execute(safeSequence, safeOptions)
                               .onErrorMap(ReactiveSqlExecutionProtection::translate);
    }

    @InternalApi
    @Override
    public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch request must not be null");
            batchMemoryLimits.check(safeRequest.options());
            return batchWriter.write(safeRequest, rowCompleted);
        });
    }

    @InternalApi
    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return writeBatch(request, rowCompleted);
    }
}
