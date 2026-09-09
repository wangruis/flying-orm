package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

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
     * 给任意执行器套一层默认结果保护。调用方显式传 options 时，显式值优先。
     * 响应式执行时限由上层拥有；非零 SQL timeout 明确拒绝，不在 ORM 内创建计时器。
     *
     * @param options 默认执行保护
     * @return 带默认执行保护的执行器
     */
    default ReactiveSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return DefaultOptionsReactiveSqlExecutor.create(
                this, ReactiveSqlExecutionProtection.requireSupportedOptions(options));
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
     * 带结果保护的查询。保留返回行数和累计估算字节上限；非零 SQL timeout 明确拒绝，
     * 执行时限与取消策略由调用方拥有。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 行数据流
     */
    default Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = ReactiveSqlExecutionProtection.requireSupportedOptions(options);
        return protectRows(query(safeRequest), safeRequest.sql(), safeOptions);
    }

    /**
     * 执行写入 SQL 并返回影响行数。
     *
     * @param request SQL 请求
     * @return 影响行数
     */
    Mono<Long> rowsUpdated(SqlRequest request);

    /**
     * 带执行选项的写入。非零 SQL timeout 明确拒绝，不为 Publisher 创建 ORM timer。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 影响行数
     */
    default Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        ReactiveSqlExecutionProtection.requireSupportedOptions(options);
        return rowsUpdated(safeRequest);
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

    /**
     * ORM 内部按已校验的物理列名读取数据库生成键；不支持列名选择的自定义执行器继续使用原有入口。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @param generatedKeyColumn 已校验的生成键物理列名
     * @return 写入结果
     */
    @InternalApi
    default Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                          SqlExecutionOptions options,
                                                          String generatedKeyColumn) {
        String column = Objects.requireNonNull(generatedKeyColumn,
                                               "generated key column must not be null").trim();
        if (column.isEmpty()) {
            return Mono.error(new IllegalArgumentException("generated key column must not be blank"));
        }
        return rowsUpdatedReturningKeys(request, options);
    }

    /** ORM 内部受保护字段工作单元，必须使用上层提供的外部事务连接。 */
    default Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support atomic protected writes"));
    }

    /**
     * 执行带分片语义的批量写入。非空 ATOMIC 必须参与外部事务；
     * 只有明确的 INDEPENDENT 模式允许各片自有局部事务。默认实现明确拒绝。
     *
     * @param request 批量写入请求
     * @return 批量写入结果
     */
    default Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return Mono.error(new UnsupportedOperationException("reactive sql executor does not support chunked batch writes"));
    }

    /** 在外部事务中执行 ATOMIC 批量并返回 SQL 证据，不把参与事实伪装为已提交。 */
    default Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
        Objects.requireNonNull(request, "batch evidence request must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support batch execution evidence"));
    }

    /** 执行含侧索引维护的证据批量；默认实现禁止静默漏写侧索引。 */
    default Mono<BatchExecutionEvidence> writeProtectedBatchEvidence(BatchWriteRequest request) {
        Objects.requireNonNull(request, "protected batch evidence request must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support protected batch execution evidence"));
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
     * 保留兼容签名；事务恢复由上层拥有，ORM 明确拒绝执行。
     *
     * @param token 恢复令牌
     * @return 确认结果
     */
    default Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        Objects.requireNonNull(token, "batch recovery token must not be null");
        return Mono.error(new UnsupportedOperationException("batch recovery must be controlled by the caller"));
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
