package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
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
final class BatchMemoryLimitedReactiveSqlExecutor extends ForwardingReactiveSqlExecutor {

    private final BatchMemoryLimits limits;

    private BatchMemoryLimitedReactiveSqlExecutor(ReactiveSqlExecutor delegate, BatchMemoryLimits limits) {
        super(delegate);
        this.limits = Objects.requireNonNull(limits, "batch memory limits must not be null");
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, BatchMemoryLimits limits) {
        ReactiveSqlExecutor safeDelegate = Objects.requireNonNull(
                delegate, "reactive sql executor must not be null");
        safeDelegate = ForwardingReactiveSqlExecutor.withoutPolicy(
                safeDelegate, BatchMemoryLimitedReactiveSqlExecutor.class, ignored -> {
                });
        return ForwardingReactiveSqlExecutor.preservingScopedCapability(
                safeDelegate, new BatchMemoryLimitedReactiveSqlExecutor(safeDelegate, limits));
    }

    @Override
    ForwardingReactiveSqlExecutor redecoratePolicy(ReactiveSqlExecutor delegate) {
        return new BatchMemoryLimitedReactiveSqlExecutor(delegate, limits);
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeBatch(safeRequest);
        });
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "protected batch request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeProtectedBatch(safeRequest);
        });
    }

    @Override
    public Mono<BatchExecutionEvidence> writeBatchEvidence(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "batch evidence request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeBatchEvidence(safeRequest);
        });
    }

    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatchEvidence(BatchWriteRequest request) {
        return Mono.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "protected batch evidence request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeProtectedBatchEvidence(safeRequest);
        });
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeBatchChunks(safeRequest);
        });
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            BatchWriteRequest safeRequest = Objects.requireNonNull(
                    request, "protected batch request must not be null");
            limits.check(safeRequest.options());
            return delegate().writeProtectedBatchChunks(safeRequest);
        });
    }

}
