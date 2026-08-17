package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * INDEPENDENT 批量写入协调器。
 *
 * <p>每个分片独立获取连接、开启事务并提交。某个分片失败不会回滚已经提交的其他分片，返回结果始终保留
 * 原始 chunkIndex 和 startOffset，调用方可以准确区分成功、失败、冲突和 UNKNOWN。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcIndependentBatchWriter {

    private final R2dbcBatchWriterChunks chunks;
    private final BatchReceiptStore receiptStore;
    private final R2dbcBatchReceiptSupport receipts;
    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcBatchResultAssembler results;
    private final R2dbcBatchReceiptConfirmer confirmer;

    R2dbcIndependentBatchWriter(R2dbcBatchWriterChunks chunks,
                                BatchReceiptStore receiptStore,
                                R2dbcBatchReceiptSupport receipts,
                                R2dbcBatchConnectionLifecycle connections,
                                R2dbcBatchResultAssembler results) {
        this.chunks = Objects.requireNonNull(chunks, "batch chunk writer must not be null");
        this.receiptStore = Objects.requireNonNull(receiptStore, "batch receipt store must not be null");
        this.receipts = Objects.requireNonNull(receipts, "batch receipt support must not be null");
        this.connections = Objects.requireNonNull(connections, "batch connection lifecycle must not be null");
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
        this.confirmer = new R2dbcBatchReceiptConfirmer(receiptStore);
    }

    Mono<BatchWriteResult> write(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        return Mono.defer(() -> {
            // 每次订阅创建自己的列表，重试或重复订阅不会串结果。单个订阅的 onNext 是串行信号，无需加锁。
            List<BatchChunkResult> completed = new ArrayList<>();
            return writeChunks(safeRequest)
                    .doOnNext(completed::add)
                    .then(Mono.fromSupplier(() -> BatchWriteResult.from(
                            BatchWriteOptions.Mode.INDEPENDENT, results.sortedChunks(completed))))
                    .onErrorResume(error -> error instanceof BatchWriteException
                            && ReactiveSqlExecutionProtection.findVirtualMachineError(error) != null
                                    ? Mono.error(error)
                                    : Mono.error(new BatchWriteException(
                                            "independent batch stopped",
                                            error,
                                            BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                                                  results.withGlobalFailure(completed, error)))));
        });
    }

    Flux<BatchChunkResult> writeChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        if (safeRequest.options().mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Flux.error(new IllegalArgumentException("batch chunks require independent mode"));
        }
        return Flux.defer(() -> {
            // prefetch=1 配合分片内存预算，最多只保留 concurrency 个正在执行的分片。
            return chunks.chunks(safeRequest)
                            .flatMap(chunk -> executeChunk(safeRequest, chunk)
                                                       .onErrorMap(error -> wrapChunkFailure(chunk, error)),
                                     safeRequest.options().concurrency(),
                                     1);
        });
    }

    private Mono<BatchChunkResult> executeChunk(BatchWriteRequest request,
                                                R2dbcBatchWriterChunks.BatchChunk chunk) {
        if (request.options().recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            return executeChunkWithReceipt(request, chunk);
        }
        return Mono.usingWhen(connections.acquire(request.options()),
                               resource -> {
                                   R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(
                                           request.options().timeout());
                                   return deadline.protect(
                                           Mono.from(resource.connection().beginTransaction())
                                               .doOnSuccess(ignored -> resource.markActive())
                                               .then(chunks.executeChunk(resource, request, chunk))
                                               .flatMap(result -> commit(resource, result, null))
                                               .onErrorResume(error -> resource.state()
                                                       == BatchTransactionState.COMMITTING
                                                               ? Mono.error(error)
                                                               : rollback(resource, chunk, error, null)))
                                                  .onErrorResume(TimeoutException.class,
                                                                 error -> timeout(resource, chunk, error, null));
                               },
                               connections::closeAfterOutcome,
                              (resource, ignored) -> connections.closeAfterOutcome(resource),
                              resource -> connections.cancel(resource, "independent"));
    }

    private Mono<BatchChunkResult> executeChunkWithReceipt(BatchWriteRequest request,
                                                           R2dbcBatchWriterChunks.BatchChunk chunk) {
        String planHash = receipts.planHash(request);
        String payloadHash = receipts.chunkPayloadHash(chunk);
        AtomicReference<BatchChunkResult.RecoveryToken> token = new AtomicReference<>(
                receipts.recoveryToken(request,
                                       chunk.chunkIndex(),
                                       planHash,
                                       payloadHash,
                                       (long) chunk.rows().size(),
                                       null));
        return receiptStore.find(token.get(), request.options().timeout())
                           .map(receipt -> BatchChunkResult.committed(chunk.chunkIndex(),
                                                                      chunk.startOffset(),
                                                                      receipt.exactInputRowCount(),
                                                                      receipt.affectedRows()))
                            .switchIfEmpty(Mono.usingWhen(
                                    connections.acquire(request.options()),
                                    resource -> {
                                        R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(
                                                request.options().timeout());
                                        return executeReceiptTransaction(
                                                resource, request, chunk, planHash, payloadHash, token, deadline);
                                    },
                                   connections::closeAfterOutcome,
                                   (resource, ignored) -> connections.closeAfterOutcome(resource),
                                   resource -> connections.cancel(resource, "independent")))
                            // 事务连接的 usingWhen 清理完成后才查询回执，避免单连接池自锁。
                             .flatMap(result -> confirmer.confirmChunk(request, result))
                             .onErrorResume(failure -> recoverReceiptFailure(request, chunk, token.get(), failure));
    }

    private Mono<BatchChunkResult> executeReceiptTransaction(
            R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            R2dbcBatchWriterChunks.BatchChunk chunk,
            String planHash,
            String payloadHash,
            AtomicReference<BatchChunkResult.RecoveryToken> token,
            R2dbcBatchDeadline deadline) {
        return deadline.protect(Mono.from(resource.connection().beginTransaction())
                    .doOnSuccess(ignored -> resource.markActive())
                   .then(receiptStore.reserve(resource.connection(),
                                              request.options().recovery(),
                                              chunk.chunkIndex(),
                                              planHash)
                                     .onErrorMap(R2dbcBatchReceiptReservationConflict::classify))
                   .then(chunks.executeChunk(resource, request, chunk))
                   .flatMap(result -> completeReceiptAndCommit(
                           resource, request, result, planHash, payloadHash, token))
                    .onErrorResume(error -> resource.state() == BatchTransactionState.COMMITTING
                            ? Mono.error(error)
                            : rollback(resource, chunk, error, token.get())))
                       .onErrorResume(TimeoutException.class,
                                      error -> timeout(resource, chunk, error, token.get()));
    }

    private Mono<BatchChunkResult> completeReceiptAndCommit(
            R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            BatchChunkResult result,
            String planHash,
            String payloadHash,
            AtomicReference<BatchChunkResult.RecoveryToken> token) {
        BatchChunkResult.RecoveryToken completedToken = receipts.recoveryToken(
                request,
                result.chunkIndex(),
                planHash,
                payloadHash,
                (long) result.inputCount(),
                result.affectedRows());
        token.set(completedToken);
        return receiptStore.complete(resource.connection(),
                                     request.options().recovery(),
                                     result.chunkIndex(),
                                     payloadHash,
                                     result.inputCount(),
                                     result.affectedRows())
                           .then(commit(resource, result, completedToken));
    }

    private Mono<BatchChunkResult> commit(R2dbcBatchConnectionHandle resource,
                                          BatchChunkResult result,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        resource.markCommitting();
        return Mono.from(resource.connection().commitTransaction())
                   .doOnSuccess(ignored -> resource.markCommitted())
                   .thenReturn(result)
                   .onErrorResume(error -> {
                       BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                               result, error, recoveryToken);
                       if (ReactiveSqlExecutionProtection.findVirtualMachineError(error) != null) {
                           return Mono.error(new BatchWriteException(
                                   "independent batch commit result is unknown",
                                   error,
                                   BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(unknown))));
                       }
                       return Mono.just(unknown);
                   });
    }

    private Mono<BatchChunkResult> rollback(R2dbcBatchConnectionHandle resource,
                                            R2dbcBatchWriterChunks.BatchChunk chunk,
                                            Throwable error,
                                            BatchChunkResult.RecoveryToken recoveryToken) {
        if (R2dbcBatchReceiptReservationConflict.find(error) != null) {
            return rollbackReservationConflict(resource, error);
        }
        BatchChunkResult failed = error instanceof R2dbcBatchChunkConflictFailure conflict
                ? BatchChunkResult.conflicted(chunk.chunkIndex(),
                                              chunk.startOffset(),
                                              chunk.rows().size(),
                                              conflict.conflicts())
                : BatchChunkResult.failed(chunk.chunkIndex(),
                                          chunk.startOffset(),
                                          chunk.rows().size(),
                                          results.failureCause(error));
        if (resource.state() == BatchTransactionState.NEW) {
            BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                    chunk, error, recoveryToken);
            if (ReactiveSqlExecutionProtection.findVirtualMachineError(error) != null) {
                return Mono.error(new BatchWriteException(
                        "independent batch begin result is unknown",
                        error,
                        BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                              List.of(unknown))));
            }
            return Mono.just(unknown);
        }
        if (ReactiveSqlExecutionProtection.findVirtualMachineError(error) != null) {
            BatchWriteException rolledBack = new BatchWriteException(
                    "independent batch rolled back after fatal operation",
                    error,
                    BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                          List.of(BatchChunkResult.rolledBack(chunk.chunkIndex(),
                                                                             chunk.startOffset(),
                                                                             chunk.rows().size()))));
            return connections.rollback(resource)
                       .onErrorResume(rollbackError -> {
                           BatchWriteException unknown = new BatchWriteException(
                                   "independent batch rollback result is unknown",
                                   rollbackError,
                                   BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                                         List.of(recoveryToken == null
                                                                 ? BatchChunkResult.unknown(chunk.chunkIndex(),
                                                                                            chunk.startOffset(),
                                                                                            chunk.rows().size(),
                                                                                            rollbackError)
                                                                 : BatchChunkResult.unknown(chunk.chunkIndex(),
                                                                                            chunk.startOffset(),
                                                                                            chunk.rows().size(),
                                                                                            rollbackError,
                                                                                            recoveryToken))));
                           unknown.addSuppressed(error);
                           return Mono.error(unknown);
                       })
                       .then(Mono.error(rolledBack));
        }
        return connections.rollback(resource)
                   .thenReturn(failed)
                   .onErrorResume(rollbackError -> {
                       BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                               chunk, rollbackError, recoveryToken);
                       if (ReactiveSqlExecutionProtection.findVirtualMachineError(rollbackError) != null) {
                           BatchWriteException outcome = new BatchWriteException(
                                   "independent batch rollback result is unknown",
                                   rollbackError,
                                   BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(unknown)));
                           outcome.addSuppressed(error);
                           return Mono.error(outcome);
                       }
                       return Mono.just(unknown);
                     });
    }

    private Mono<BatchChunkResult> rollbackReservationConflict(R2dbcBatchConnectionHandle resource,
                                                                Throwable conflict) {
        Mono<Void> rollback = connections.rollback(resource)
                .onErrorResume(rollbackError -> {
                    VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                            conflict, rollbackError);
                    if (fatal != null) {
                        return Mono.error(fatal);
                    }
                    ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(conflict, rollbackError);
                    return Mono.empty();
                });
        return rollback.then(Mono.error(conflict));
    }

    private Mono<BatchChunkResult> recoverReceiptFailure(BatchWriteRequest request,
                                                          R2dbcBatchWriterChunks.BatchChunk chunk,
                                                          BatchChunkResult.RecoveryToken token,
                                                          Throwable failure) {
        VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(failure);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        if (R2dbcBatchReceiptReservationConflict.find(failure) != null) {
            return receiptStore.find(token, request.options().timeout())
                               .map(receipt -> BatchChunkResult.committed(token.chunkIndex(),
                                                                          chunk.startOffset(),
                                                                          Math.toIntExact(receipt.exactInputRowCount()),
                                                                          receipt.affectedRows()))
                               .switchIfEmpty(Mono.error(failure));
        }
        return failure instanceof R2dbcBatchChunkWriteFailure chunkFailure
                ? confirmer.confirmChunkFailure(request, chunkFailure)
                : Mono.error(failure);
    }

    /**
     * 将已持有连接后的总截止超时转换为与事务阶段一致的分片结果。
     *
     * <p>NEW 和 COMMITTING 都没有可确认的事务回执，不能按失败处理；ACTIVE 则仍先等待 rollback 回执，
     * 保持普通执行超时的既有失败语义。</p>
     */
    private Mono<BatchChunkResult> timeout(R2dbcBatchConnectionHandle resource,
                                           R2dbcBatchWriterChunks.BatchChunk chunk,
                                           TimeoutException error,
                                           BatchChunkResult.RecoveryToken recoveryToken) {
        if (resource.state() == BatchTransactionState.NEW
                || resource.state() == BatchTransactionState.COMMITTING) {
            return Mono.error(R2dbcBatchChunkWriteFailure.unknown(chunk, error, recoveryToken));
        }
        return rollback(resource, chunk, error, recoveryToken)
                .flatMap(result -> Mono.error(R2dbcBatchChunkWriteFailure.exact(result, error)));
    }

    private static Throwable wrapChunkFailure(R2dbcBatchWriterChunks.BatchChunk chunk, Throwable error) {
        return error instanceof R2dbcBatchChunkWriteFailure || error instanceof R2dbcBatchChunkConflictFailure
                || error instanceof BatchWriteException
                && ReactiveSqlExecutionProtection.findVirtualMachineError(error) != null
                ? error
                : new R2dbcBatchChunkWriteFailure(chunk, error);
    }
}
