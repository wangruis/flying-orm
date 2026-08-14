package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 把外部 ATOMIC 的 ENLISTED 结果和事务结束后的最终批量观测接起来。
 *
 * <p>这里只保留不可变的分片结果快照，不保存参数、输入流或实体。快照数量已经受批量结果上限保护，
 * 会在外部事务结束并执行回调后随事务同步对象一起释放。观察器的普通故障被隔离，不能反过来改变数据库事务；
 * 异常图中的 JVM 致命错误仍原样传播。</p>
 */
final class R2dbcExternalBatchCompletion {

    private final R2dbcBatchResultAssembler results;
    private final BatchExecutionObserver observer;

    R2dbcExternalBatchCompletion(R2dbcBatchResultAssembler results, BatchExecutionObserver observer) {
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
        this.observer = Objects.requireNonNull(observer, "batch execution observer must not be null");
    }

    Mono<BatchWriteResult> enlist(R2dbcBatchConnectionHandle resource,
                                  BatchWriteRequest request,
                                  List<BatchChunkResult> executed) {
        R2dbcBatchConnectionHandle safeResource = Objects.requireNonNull(resource,
                                                                         "batch connection must not be null");
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        List<BatchChunkResult> snapshot = List.copyOf(Objects.requireNonNull(
                executed, "executed batch chunks must not be null"));
        BatchWriteResult enlisted = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                          results.enlistedChunks(snapshot));
        BatchExecutionObservation.BatchWriteRequestView view =
                new BatchExecutionObservation.BatchWriteRequestView(
                        safeRequest.sql(), safeRequest.options().mode(), safeRequest.parameterCount());
        if (register(safeResource, view, safeRequest.completion(), snapshot)) {
            return Mono.just(enlisted);
        }
        // 没有完成通知时无法等待最终提交，但必须立即释放 Repository 暂存的实体引用。
        BatchWriteResult unavailable = finalResult(snapshot, TransactionOutcome.UNKNOWN);
        return invokeCompletion(safeRequest.completion(), unavailable)
                .onErrorResume(error -> {
                    VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(error);
                    return fatal == null ? Mono.empty() : Mono.error(fatal);
                })
                .thenReturn(enlisted);
    }

    private boolean register(R2dbcBatchConnectionHandle resource,
                             BatchExecutionObservation.BatchWriteRequestView view,
                             BatchWriteCompletion completion,
                             List<BatchChunkResult> executed) {
        long registeredAt = System.nanoTime();
        AtomicBoolean notified = new AtomicBoolean();
        try {
            return resource.externalTransaction().completion().register(outcome -> {
                if (!notified.compareAndSet(false, true)) {
                    return Mono.empty();
                }
                BatchWriteResult finalResult = finalResult(executed, outcome);
                return Mono.fromRunnable(() -> notifyObserver(
                                   view, finalResult, System.nanoTime() - registeredAt))
                           .then(invokeCompletion(completion, finalResult));
            });
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 完成通知属于观测协作；注册器异常不能把已经执行成功的 SQL 改成失败。
            return false;
        }
    }

    private BatchWriteResult finalResult(List<BatchChunkResult> executed,
                                         TransactionOutcome outcome) {
        TransactionOutcome safeOutcome = outcome == null ? TransactionOutcome.UNKNOWN : outcome;
        return switch (safeOutcome) {
            case COMMITTED -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, executed);
            case ROLLED_BACK -> BatchWriteResult.from(
                    BatchWriteOptions.Mode.ATOMIC, results.rolledBackExecutedChunks(executed));
            case UNKNOWN -> {
                R2dbcTransactionParticipationException error = new R2dbcTransactionParticipationException(
                        R2dbcTransactionParticipationException.Reason.EXTERNAL_TRANSACTION_OUTCOME_UNKNOWN);
                yield BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                            results.unknownChunks(executed, error));
            }
        };
    }

    private Mono<Void> invokeCompletion(BatchWriteCompletion completion, BatchWriteResult result) {
        return Mono.defer(() -> Mono.from(Objects.requireNonNull(
                completion.afterCompletion(result), "batch completion publisher must not be null")));
    }

    private void notifyObserver(BatchExecutionObservation.BatchWriteRequestView view,
                                BatchWriteResult result,
                                long durationNanos) {
        try {
            observer.onExecution(BatchExecutionObservation.summary(view, result, durationNanos));
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 外部事务已经结束，observer 失败只能丢失观测，不能污染事务管理器的完成流程。
        }
    }
}
