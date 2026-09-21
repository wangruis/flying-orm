package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.LongFunction;
import org.reactivestreams.Publisher;

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
    public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return Mono.defer(() -> {
            limits.check(Objects.requireNonNull(request, "batch request must not be null").options());
            return delegate().writeBatch(request, rowCompleted);
        });
    }

    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return Mono.defer(() -> {
            limits.check(Objects.requireNonNull(request, "batch request must not be null").options());
            return delegate().writeProtectedBatch(request, rowCompleted);
        });
    }
}
