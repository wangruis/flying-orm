package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.reactivestreams.Publisher;

/**
 * ReactiveSqlExecutor 是关系型数据库的响应式 SQL 执行契约，直接返回 Reactor 类型。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public interface ReactiveSqlExecutor {

    /** Executes internal multi-statement work on one supplied connection. */
    @InternalApi
    default <T> Mono<T> withConnection(SqlRequest firstRequest,
            Function<ReactiveSqlExecutor, Mono<T>> work) {
        Objects.requireNonNull(firstRequest, "first request must not be null");
        Objects.requireNonNull(work, "connection work must not be null");
        return Mono.error(new UnsupportedOperationException("reactive executor does not support same-connection work"));
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
     * 响应式执行时限由上层拥有，ORM 只接收结果容量和驱动预取配置。
     *
     * @param options 默认执行保护
     * @return 带默认执行保护的执行器
     */
    default ReactiveSqlExecutor withDefaultExecutionOptions(SqlExecutionOptions options) {
        return DefaultOptionsReactiveSqlExecutor.create(
                this, Objects.requireNonNull(options, "sql execution options must not be null"));
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
     * 带结果保护的查询。保留返回行数和累计估算字节上限；
     * 执行时限与取消策略由调用方拥有。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 行数据流
     */
    default Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
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
     * 带执行容量选项的写入，不为 Publisher 创建 ORM timer。
     *
     * @param request SQL 请求
     * @param options 执行保护选项
     * @return 影响行数
     */
    default Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        return rowsUpdated(safeRequest);
    }

    /**
     * 执行写入并读取同一个 Statement 产生的数据库生成键。
     *
     * <p>原生 R2DBC 执行器会覆盖这个方法并调用 {@code Statement.returnGeneratedValues()}。普通自定义
     * 执行器没有实现生成键能力时，默认实现在执行 SQL 前明确拒绝，不能先完成不可逆写入再返回空键。</p>
     *
     * @param request 写入 SQL 请求
     * @param options 执行保护
     * @return 影响行数和数据库生成键
     */
    default Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        Objects.requireNonNull(request, "sql request must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support generated keys"));
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

    /** ORM 内部受保护字段工作单元，使用同一条上层提供的连接。 */
    default Mono<SqlWriteResult> protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        Objects.requireNonNull(work, "protected write work must not be null");
        Objects.requireNonNull(options, "sql execution options must not be null");
        return Mono.error(new UnsupportedOperationException(
                "reactive sql executor does not support protected writes"));
    }

    default Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request) {
        return writeBatch(request, null);
    }

    /** Called after a row's SQL, keys, auxiliary work and owned resource cleanup complete. */
    @InternalApi
    default Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        Objects.requireNonNull(request, "batch write request must not be null");
        return Mono.error(new UnsupportedOperationException("reactive executor does not support batch writes"));
    }

    default Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
        return writeBatch(request);
    }

    default Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request) {
        return writeProtectedBatch(request, null);
    }

    @InternalApi
    default Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        Objects.requireNonNull(request, "protected batch request must not be null");
        return Mono.error(new UnsupportedOperationException("reactive executor does not support protected batch writes"));
    }

    default Mono<BatchExecutionEvidence> writeProtectedBatchEvidence(BatchWriteRequest request) {
        return writeProtectedBatch(request);
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
