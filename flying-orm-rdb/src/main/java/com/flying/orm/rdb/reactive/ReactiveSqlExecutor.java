package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * ReactiveSqlExecutor 是关系型数据库的响应式 SQL 执行契约，直接返回 Reactor 类型。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface ReactiveSqlExecutor {

    /**
     * 返回当前订阅正在参与的外部事务；没有外部事务时为空。
     *
     * <p>Schema 等高层能力用它在执行前判断操作能否加入上层事务。装饰器必须原样透传，不能因为增加日志、
     * 默认保护或批量内存限制就把事务上下文吃掉。自定义执行器若支持外部事务，也必须覆盖这个方法。</p>
     */
    default Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.empty();
    }

    /**
     * 给任意响应式执行器包一层观测 hook。已经原生支持观测的执行器可以直接用自己的工厂方法。
     *
     * @param observer SQL 执行观察者
     * @return 带观测能力的执行器
     */
    default ReactiveSqlExecutor withObserver(SqlExecutionObserver observer) {
        return ObservedReactiveSqlExecutor.create(this, observer);
    }

    /**
     * 给任意响应式执行器包一层批量细粒度观测 hook。
     *
     * @param observer 批量执行观察者
     * @return 带批量观测能力的执行器
     */
    default ReactiveSqlExecutor withBatchObserver(BatchExecutionObserver observer) {
        return ObservedReactiveSqlExecutor.create(this, SqlExecutionObserver.noop(), observer);
    }

    /**
     * 同时接普通 SQL 观测和批量细粒度观测。
     *
     * @param observer      SQL 执行观察者
     * @param batchObserver 批量执行观察者
     * @return 带观测能力的执行器
     */
    default ReactiveSqlExecutor withObservers(SqlExecutionObserver observer, BatchExecutionObserver batchObserver) {
        return ObservedReactiveSqlExecutor.create(this, observer, batchObserver);
    }

    /**
     * 给任意执行器套一层默认执行保护。调用方显式传 options 时，显式值优先。
     *
     * @param options 默认执行保护
     * @return 带默认执行保护的执行器
     */
    default ReactiveSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return DefaultOptionsReactiveSqlExecutor.create(this, options);
    }

    /**
     * 给批量入口加客户端级硬上限。它和单次 {@code BatchWriteOptions} 不冲突：单次配置负责选择本次预算，
     * 这里负责阻止某次调用把进程允许的最大内存或并发临时放大。
     */
    default ReactiveSqlExecutor withBatchMemoryLimits(BatchMemoryLimits limits) {
        return BatchMemoryLimitedReactiveSqlExecutor.create(this, limits);
    }

    /**
     * 执行查询 SQL 并返回行数据流。
     *
     * @param request SQL 请求
     * @return 行数据流
     */
    Flux<DynamicRow> query(SqlRequest request);

    /**
     * 带执行保护的查询。超时会取消上游订阅，返回行数或累计估算字节超过上限会直接报错。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 行数据流
     */
    default Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return protectRows(query(safeRequest), safeRequest.sql(), options);
    }

    /**
     * 执行写入 SQL 并返回影响行数。
     *
     * @param request SQL 请求
     * @return 影响行数
     */
    Mono<Long> rowsUpdated(SqlRequest request);

    /**
     * 带执行保护的写入。写入没有返回行集，所以只应用 timeout。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 影响行数
     */
    default Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        Objects.requireNonNull(request, "sql request must not be null");
        return protectMono(rowsUpdated(request), options);
    }

    /**
     * 执行写入并读取同一个 Statement 产生的数据库生成键。
     *
     * <p>普通自定义执行器可以沿用默认实现，此时只返回影响行数；原生 R2DBC 执行器会覆盖这个方法并调用
     * {@code Statement.returnGeneratedValues()}。Repository 发现实体声明了数据库生成主键时会检查结果中确实
     * 存在键值，因此不支持生成键的驱动不会被静默当成成功回填。</p>
     *
     * @param request 写入 SQL 请求
     * @param options 执行保护
     * @return 影响行数和数据库生成键
     */
    default Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return rowsUpdated(request, options).map(rows -> new SqlWriteResult(rows, java.util.List.of()));
    }

    /** ORM 内部受保护字段写工作单元；只有能够控制同一连接事务的原生执行器可以覆盖。 */
    default Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support atomic protected writes"));
    }

    /**
     * 执行带分片语义的批量写入，默认明确报错，真正实现由 R2DBC 批量 writer 提供。
     *
     * @param request 批量写入请求
     * @return 批量写入结果
     */
    default Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return Mono.error(new UnsupportedOperationException("reactive sql executor does not support chunked batch writes"));
    }

    /** 执行含 CONTAINS 侧索引维护的批量；普通自定义执行器不能静默降级为只写业务表。 */
    default Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support protected batch writes"));
    }

    /**
     * 执行独立分片批量写入并逐个发出分片结果。
     *
     * @param request 批量写入请求
     * @return 分片结果流
     */
    default Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return Flux.error(new UnsupportedOperationException("reactive sql executor does not support independent batch writes"));
    }

    /** INDEPENDENT 受保护批量的分片入口；默认实现保持 fail-closed。 */
    default Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        return Flux.error(new UnsupportedOperationException(
                "reactive sql executor does not support protected batch writes"));
    }

    /**
     * 查询 UNKNOWN 批量结果的后续确认状态。
     *
     * @param token 恢复令牌
     * @return 确认结果
     */
    default Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        Objects.requireNonNull(token, "batch recovery token must not be null");
        return Mono.error(new UnsupportedOperationException("reactive sql executor does not support batch recovery"));
    }

    private static <T> Mono<T> protectMono(Mono<T> source, SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                 "sql execution options must not be null");
        if (safeOptions.timeout().isZero()) {
            return source;
        }
        return source.timeout(safeOptions.timeout())
                     .onErrorMap(TimeoutException.class,
                                 error -> new SqlExecutionTimeoutException(safeOptions.timeout(), error));
    }

    private static Flux<DynamicRow> protectRows(Flux<DynamicRow> source,
                                                String sql,
                                                SqlExecutionOptions options) {
        return ReactiveSqlExecutionProtection.protectRows(
                source,
                sql,
                Objects.requireNonNull(options, "sql execution options must not be null"),
                BatchMemoryBudget::estimateRowBytes);
    }

}
