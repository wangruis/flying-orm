package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 只在批量入口做硬上限校验，普通查询和更新原样转发。校验放进 defer，保证每次订阅都检查，
 * 同时在检查失败时不会订阅输入 Publisher，也不会让下游执行器申请数据库连接。
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
final class BatchMemoryLimitedReactiveSqlExecutor implements ReactiveSqlExecutor,
                                                               ConnectionScopedReactiveSqlExecutor {

    private final ReactiveSqlExecutor delegate;
    private final BatchMemoryLimits limits;

    private BatchMemoryLimitedReactiveSqlExecutor(ReactiveSqlExecutor delegate, BatchMemoryLimits limits) {
        this.delegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
        this.limits = Objects.requireNonNull(limits, "batch memory limits must not be null");
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, BatchMemoryLimits limits) {
        return new BatchMemoryLimitedReactiveSqlExecutor(delegate, limits);
    }

    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return delegate.currentTransaction();
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return delegate.query(request);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return delegate.query(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return delegate.rowsUpdated(request);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdated(request, options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdatedReturningKeys(request, options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                         SqlExecutionOptions options,
                                                         String generatedKeyColumn) {
        return delegate.rowsUpdatedReturningKeys(request, options, generatedKeyColumn);
    }

    /** 字段保护写入不是批量输入；保留底层同连接事务能力而不套用批量行数上限。 */
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        return delegate.atomicProtectedWrite(work, options);
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            limits.check(safeRequest.options());
            return delegate.writeBatch(safeRequest);
        });
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "protected batch request must not be null");
            limits.check(safeRequest.options());
            return delegate.writeProtectedBatch(safeRequest);
        });
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            limits.check(safeRequest.options());
            return delegate.writeBatchChunks(safeRequest);
        });
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "protected batch request must not be null");
            limits.check(safeRequest.options());
            return delegate.writeProtectedBatchChunks(safeRequest);
        });
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        return delegate.resolveUnknown(token);
    }

    /**
     * 透传同连接 SQL 序列能力，使统一批量内存保护不会意外剥离 DDL 会话锁超时保护。
     *
     * <p>底层执行器不具备该能力时继续 fail-closed，绝不拆成多次普通更新伪装同一连接。</p>
     *
     * @param sequence 必须在同一连接执行的 setup/work/cleanup 序列
     * @param options  SQL 执行与资源保护选项
     * @return 结构化序列执行结果
     */
    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                 SqlExecutionOptions options) {
        if (delegate instanceof ConnectionScopedReactiveSqlExecutor scoped) {
            return scoped.executeInConnection(sequence, options);
        }
        return Mono.error(new IllegalStateException(
                "wrapped reactive SQL executor does not support connection-scoped execution"));
    }
}
