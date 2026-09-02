package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchReceiptMismatchException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    private final R2dbcIndependentBatchFlow flow;

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
        this.flow = new R2dbcIndependentBatchFlow(chunks, results);
    }

    Mono<BatchWriteResult> write(
            BatchWriteRequest request,
            ReactiveTransactionSourceResolver.Resolution resolution,
            String transportSql) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        if (safeRequest.options().recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            return Mono.defer(() -> {
                String planHash = receipts.planHash(safeRequest);
                return flow.write(safeRequest, chunk -> executeChunkWithReceipt(
                        safeRequest, chunk, safeResolution, transportSql, planHash));
            });
        }
        return flow.write(safeRequest,
                          chunk -> executeChunk(safeRequest, chunk, safeResolution, transportSql));
    }

    Flux<BatchChunkResult> writeChunks(
            BatchWriteRequest request,
            ReactiveTransactionSourceResolver.Resolution resolution,
            String transportSql) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        if (safeRequest.options().mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Flux.error(new IllegalArgumentException("batch chunks require independent mode"));
        }
        if (safeRequest.options().recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            return Flux.defer(() -> {
                String planHash = receipts.planHash(safeRequest);
                return flow.writeChunks(safeRequest, chunk -> executeChunkWithReceipt(
                        safeRequest, chunk, safeResolution, transportSql, planHash));
            });
        }
        return flow.writeChunks(safeRequest,
                                chunk -> executeChunk(safeRequest, chunk, safeResolution, transportSql));
    }

    private Mono<BatchChunkResult> executeChunk(BatchWriteRequest request,
                                                 R2dbcBatchWriterChunks.BatchChunk chunk,
                                                 ReactiveTransactionSourceResolver.Resolution resolution,
                                                 String transportSql) {
        return Mono.usingWhen(connections.acquire(request.options(), resolution),
                               resource -> {
                                   R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(
                                           request.options().timeout());
                                   return deadline.protect(
                                           connections.begin(resource)
                                               .then(chunks.executeChunk(resource, request, chunk, transportSql))
                                               .flatMap(result -> commit(request, resource, result, null))
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
                                                            R2dbcBatchWriterChunks.BatchChunk chunk,
                                                            ReactiveTransactionSourceResolver.Resolution resolution,
                                                            String transportSql,
                                                            String planHash) {
        AtomicReference<BatchChunkResult.RecoveryToken> token = new AtomicReference<>();
        return replayReceipt(request, chunk, planHash)
                             .switchIfEmpty(Mono.defer(() -> {
                                 String payloadHash = receipts.chunkPayloadHash(chunk);
                                 token.set(receipts.recoveryToken(request,
                                                                  chunk.chunkIndex(),
                                                                  planHash,
                                                                  payloadHash,
                                                                  (long) chunk.rows().size(),
                                                                  null));
                                 return Mono.usingWhen(
                                         connections.acquire(request.options(), resolution),
                                         resource -> {
                                             R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(
                                                     request.options().timeout());
                                             return executeReceiptTransaction(
                                                     resource,
                                                      request,
                                                      chunk,
                                                      planHash,
                                                     payloadHash,
                                                     token,
                                                     deadline,
                                                     transportSql);
                                         },
                                         connections::closeAfterOutcome,
                                         (resource, ignored) -> connections.closeAfterOutcome(resource),
                                         resource -> connections.cancel(resource, "independent"));
                             }))
                             // 事务连接的 usingWhen 清理完成后才查询回执，避免单连接池自锁。
                             .flatMap(result -> confirmer.confirmChunk(request, result))
                             .onErrorResume(failure -> recoverReceiptFailure(request, chunk, planHash, failure));
    }

    private Mono<BatchChunkResult> replayReceipt(BatchWriteRequest request,
                                                  R2dbcBatchWriterChunks.BatchChunk chunk,
                                                  String planHash) {
        BatchWriteOptions.Recovery recovery = request.options().recovery();
        return receiptStore.findOperation(recovery, chunk.chunkIndex(), request.options().timeout())
                           .flatMap(receipt -> {
                               if (!planHash.equals(receipt.planHash())) {
                                   return Mono.error(new BatchReceiptMismatchException(recovery.operationId()));
                               }
                               String payloadHash = receipts.chunkPayloadHash(chunk);
                               if (!receipt.payloadHash().equals(payloadHash)) {
                                   return Mono.error(new BatchReceiptMismatchException(recovery.operationId()));
                               }
                               BatchChunkResult.RecoveryToken evidence = receipts.recoveryToken(
                                       request,
                                       chunk.chunkIndex(),
                                       receipt.planHash(),
                                       payloadHash,
                                       (long) chunk.rows().size(),
                                       null);
                               BatchReceiptStore.Receipt verified = BatchReceiptStore.requireMatching(
                                       evidence, receipt);
                               return Mono.just(BatchChunkResult.committed(chunk.chunkIndex(),
                                                                           chunk.startOffset(),
                                                                           verified.exactInputRowCount(),
                                                                           verified.affectedRows()));
                           });
    }

    private Mono<BatchChunkResult> executeReceiptTransaction(
            R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            R2dbcBatchWriterChunks.BatchChunk chunk,
            String planHash,
            String payloadHash,
            AtomicReference<BatchChunkResult.RecoveryToken> token,
            R2dbcBatchDeadline deadline,
            String transportSql) {
        return deadline.protect(connections.begin(resource)
                   .then(receiptStore.reserve(resource.connection(),
                                              request.options().recovery(),
                                              chunk.chunkIndex(),
                                              planHash)
                                     .onErrorMap(R2dbcBatchReceiptReservationConflict::classify))
                   .then(chunks.executeChunk(resource, request, chunk, transportSql))
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
                           .then(commit(request, resource, result, completedToken));
    }

    private Mono<BatchChunkResult> commit(BatchWriteRequest request,
                                          R2dbcBatchConnectionHandle resource,
                                          BatchChunkResult result,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        return connections.commit(resource)
                   .thenReturn(result)
                   .doOnNext(committed -> {
                       // commit 已确认而 close 尚未结束时，取消也不能撤销实体的已提交主键。
                       if (request.completion() instanceof BatchChunkCompletion completion) {
                           completion.afterChunk(committed);
                       }
                   })
                   .onErrorResume(error -> {
                       BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                               result, error, recoveryToken);
                       if (findVirtualMachineError(error) != null) {
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
            if (findVirtualMachineError(error) != null) {
                return Mono.error(new BatchWriteException(
                        "independent batch begin result is unknown",
                        error,
                        BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                              List.of(unknown))));
            }
            return Mono.just(unknown);
        }
        if (findVirtualMachineError(error) != null) {
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
                       if (findVirtualMachineError(rollbackError) != null) {
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
                    VirtualMachineError fatal = promoteVirtualMachineError(conflict, rollbackError);
                    if (fatal != null) {
                        return Mono.error(fatal);
                    }
                    addSuppressedIfAcyclic(conflict, rollbackError);
                    return Mono.empty();
                });
        return rollback.then(Mono.error(conflict));
    }

    private Mono<BatchChunkResult> recoverReceiptFailure(BatchWriteRequest request,
                                                          R2dbcBatchWriterChunks.BatchChunk chunk,
                                                          String planHash,
                                                          Throwable failure) {
        VirtualMachineError fatal = findVirtualMachineError(failure);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        if (R2dbcBatchReceiptReservationConflict.find(failure) != null) {
            return replayReceipt(request, chunk, planHash)
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
                || resource.state() == BatchTransactionState.COMMITTING
                || resource.state() == BatchTransactionState.ROLLING_BACK) {
            return Mono.error(R2dbcBatchChunkWriteFailure.unknown(chunk, error, recoveryToken));
        }
        return rollback(resource, chunk, error, recoveryToken)
                .flatMap(result -> Mono.error(R2dbcBatchChunkWriteFailure.exact(result, error)));
    }

}
