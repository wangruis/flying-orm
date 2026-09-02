package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchReceiptMismatchException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ATOMIC 分片在同一连接和事务中串行执行，任一失败都会回滚整批。提交请求发出后如果连接中断，
 * 当前连接已经无法证明事务结果，只能返回 UNKNOWN，绝不能把未知结果伪装成普通失败。</p>
 */
final class R2dbcAtomicBatchWriter {
    private final R2dbcBatchWriterChunks chunks;
    private final BatchReceiptStore receiptStore;
    private final R2dbcBatchReceiptSupport receipts;
    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcBatchResultAssembler results;
    private final R2dbcExternalBatchCompletion externalCompletion;
    private final R2dbcBatchReceiptConfirmer confirmer;

    R2dbcAtomicBatchWriter(R2dbcBatchWriterChunks chunks,
                           BatchReceiptStore receiptStore,
                           R2dbcBatchReceiptSupport receipts,
                           R2dbcBatchConnectionLifecycle connections,
                           R2dbcBatchResultAssembler results,
                           R2dbcExternalBatchCompletion externalCompletion) {
        this.chunks = Objects.requireNonNull(chunks, "batch chunk writer must not be null");
        this.receiptStore = Objects.requireNonNull(receiptStore, "batch receipt store must not be null");
        this.receipts = Objects.requireNonNull(receipts, "batch receipt support must not be null");
        this.connections = Objects.requireNonNull(connections, "batch connection lifecycle must not be null");
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
        this.externalCompletion = Objects.requireNonNull(externalCompletion, "external completion must not be null");
        this.confirmer = new R2dbcBatchReceiptConfirmer(receiptStore);
    }

    Mono<BatchWriteResult> write(BatchWriteRequest request,
                                 ReactiveTransactionSourceResolver.Resolution resolution,
                                 String transportSql) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        return Mono.defer(() -> writeResolved(safeRequest, safeResolution, transportSql));
    }

    private Mono<BatchWriteResult> writeResolved(BatchWriteRequest request,
                                                 ReactiveTransactionSourceResolver.Resolution resolution,
                                                 String transportSql) {
        if (request.options().recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            return writeWithReceipt(request, resolution, transportSql);
        }
        AtomicLong acceptedRows = new AtomicLong();
        // 空输入不拿连接。有了第一个分片后才开始事务，减少连接池无效占用。
        return chunks.chunks(request, acceptedRows::set).switchOnFirst((signal, chunkFlux) -> {
            if (signal.hasError()) {
                return Flux.error(results.failureBeforeTransaction(
                        "atomic batch input failed before a transaction became active",
                        signal.getThrowable(), acceptedRows.get()));
            }
            if (!signal.hasValue()) {
                return Flux.just(BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC));
            }
            return Flux.usingWhen(connections.acquire(request.options(), resolution),
                                  resource -> {
                                      R2dbcBatchDeadline deadline =
                                              R2dbcBatchDeadline.start(request.options().timeout());
                                      return execute(resource, request, chunkFlux, deadline, acceptedRows,
                                                     transportSql);
                                  },
                                  connections::closeAfterOutcome,
                                  (resource, ignored) -> connections.closeAfterOutcome(resource),
                                  resource -> connections.cancel(resource, "atomic"));
        }).single().onErrorMap(TimeoutException.class, error -> results.failureBeforeTransaction(
                "batch write timed out before a transaction became active", error, acceptedRows.get()));
    }

    private Mono<BatchWriteResult> execute(R2dbcBatchConnectionHandle resource,
                                           BatchWriteRequest request,
                                           Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
                                           R2dbcBatchDeadline deadline,
                                           AtomicLong acceptedRows,
                                           String transportSql) {
        List<BatchChunkResult> completed = new ArrayList<>();
        AtomicReference<R2dbcBatchWriterChunks.BatchChunk> activeChunk = new AtomicReference<>();
        return deadline.protect(executeOnConnection(
                               resource, request, chunkFlux, completed, activeChunk, acceptedRows,
                               transportSql))
                       .onErrorResume(TimeoutException.class,
                                      error -> timeout(resource,
                                                       completed,
                                                       R2dbcBatchWriterChunks.timeoutFailure(activeChunk, error),
                                                       null,
                                                       acceptedRows.get()));
    }

    private Mono<BatchWriteResult> executeOnConnection(R2dbcBatchConnectionHandle resource,
                                                       BatchWriteRequest request,
                                                       Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
                                                       List<BatchChunkResult> completed,
                                                       AtomicReference<R2dbcBatchWriterChunks.BatchChunk> activeChunk,
                                                       AtomicLong acceptedRows,
                                                       String transportSql) {
        return connections.begin(resource)
                   // ATOMIC 必须串行且不预取下一片，输入预算仅为当前执行分片预留空间。
                   .thenMany(chunkFlux.concatMap(chunk -> R2dbcBatchWriterChunks.trackActiveChunk(
                           activeChunk,
                           chunk,
                           chunks.executeChunk(resource, request, chunk, transportSql)
                                 .doOnNext(completed::add)), 0))
                   .then(Mono.defer(() -> commit(resource, request, completed, null)))
                   .onErrorResume(error -> error instanceof BatchWriteException
                           ? Mono.error(error)
                           : rollback(resource, completed, error, null, acceptedRows.get()));
    }

    private Mono<BatchWriteResult> writeWithReceipt(BatchWriteRequest request,
            ReactiveTransactionSourceResolver.Resolution resolution,
            String transportSql) {
        AtomicLong acceptedRows = new AtomicLong();
        String planHash = receipts.planHash(request);
        BatchWriteOptions.Recovery recovery = request.options().recovery();
        Mono<BatchWriteResult> replay = receiptStore.findOperation(
                recovery, 0, request.options().timeout())
                .flatMap(receipt -> replayReceipt(request, planHash, receipt, acceptedRows));
        return replay.switchIfEmpty(Mono.defer(() -> writeStreamingWithReceipt(
                request, planHash, acceptedRows, resolution, transportSql)
                // usingWhen 已经先完成原事务连接的归还或淘汰，再允许回执重放获取第二条连接。
                .onErrorResume(failure -> recoverReceiptFailure(request, replay, failure))))
                      .onErrorMap(TimeoutException.class, error -> results.failureBeforeTransaction(
                              "batch write timed out before a transaction became active",
                              error, acceptedRows.get()));
    }

    private Mono<BatchWriteResult> replayReceipt(BatchWriteRequest request,
                                                  String planHash,
                                                  BatchReceiptStore.Receipt receipt,
                                                  AtomicLong acceptedRows) {
        BatchWriteOptions.Recovery recovery = request.options().recovery();
        if (!planHash.equals(receipt.planHash())) {
            return Mono.error(new BatchReceiptMismatchException(recovery.operationId()));
        }
        return receipts.hashPayload(request, chunks, acceptedRows::set)
                       .onErrorMap(error -> results.failureBeforeTransaction(
                               "recoverable atomic batch replay input failed before a transaction became active",
                               error, acceptedRows.get()))
                       .flatMap(payloadHash -> {
            if (!receipt.payloadHash().equals(payloadHash)) {
                return Mono.error(new BatchReceiptMismatchException(recovery.operationId()));
            }
            BatchChunkResult result = BatchChunkResult.committed(0,
                                                                 0,
                                                                 receipt.exactInputRowCount(),
                                                                 receipt.affectedRows());
            return Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(result)));
        });
    }

    private Mono<BatchWriteResult> writeStreamingWithReceipt(BatchWriteRequest request,
                                                              String planHash,
                                                              AtomicLong acceptedRows,
                                                              ReactiveTransactionSourceResolver.Resolution resolution,
                                                              String transportSql) {
        return chunks.chunks(request, acceptedRows::set).switchOnFirst((signal, chunkFlux) -> {
            if (signal.hasError()) {
                return Flux.error(results.failureBeforeTransaction(
                        "recoverable atomic batch input failed before a transaction became active",
                        signal.getThrowable(), acceptedRows.get()));
            }
            if (!signal.hasValue()) {
                return Flux.just(BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC));
            }
            return Flux.usingWhen(connections.acquire(request.options(), resolution),
                                  resource -> {
                                      R2dbcBatchDeadline deadline =
                                              R2dbcBatchDeadline.start(request.options().timeout());
                                       return executeWithReceipt(
                                               resource, request, chunkFlux, planHash, deadline, acceptedRows,
                                               transportSql);
                                  },
                                  connections::closeAfterOutcome,
                                  (resource, ignored) -> connections.closeAfterOutcome(resource),
                                  resource -> connections.cancel(resource, "atomic"));
        }).single();
    }

    private Mono<BatchWriteResult> executeWithReceipt(R2dbcBatchConnectionHandle resource,
                                                      BatchWriteRequest request,
                                                      Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
                                                      String planHash,
                                                      R2dbcBatchDeadline deadline,
                                                      AtomicLong acceptedRows,
                                                      String transportSql) {
        List<BatchChunkResult> completed = new ArrayList<>();
        AtomicReference<BatchChunkResult.RecoveryToken> token = new AtomicReference<>();
        AtomicReference<R2dbcBatchWriterChunks.BatchChunk> activeChunk = new AtomicReference<>();
        return deadline.protect(executeOnConnectionWithReceipt(
                                       resource, request, chunkFlux, planHash, token, completed, activeChunk,
                                       acceptedRows, transportSql))
                       .onErrorResume(TimeoutException.class,
                                      error -> timeout(resource,
                                                       completed,
                                                       R2dbcBatchWriterChunks.timeoutFailure(activeChunk, error),
                                                       token.get(),
                                                       acceptedRows.get()));
    }

    private Mono<BatchWriteResult> executeOnConnectionWithReceipt(R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
            String planHash,
            AtomicReference<BatchChunkResult.RecoveryToken> token,
            List<BatchChunkResult> completed,
            AtomicReference<R2dbcBatchWriterChunks.BatchChunk> activeChunk,
            AtomicLong acceptedRows,
            String transportSql) {
        BatchWriteOptions.Recovery recovery = request.options().recovery();
        Connection connection = resource.connection();
        BatchReceiptDigest digest = receipts.newPayloadDigest();
        return connections.begin(resource)
                   .then(receiptStore.reserve(connection, recovery, 0, planHash)
                                     .onErrorMap(R2dbcBatchReceiptReservationConflict::classify))
                   // reserve 已和业务写入处在同一事务，先暴露操作级令牌；即使首片失败也有恢复依据。
                   .doOnSuccess(ignored -> token.set(receipts.recoveryToken(
                           request, 0, planHash, null, null, null)))
                   .thenMany(chunkFlux.concatMap(chunk -> {
                       receipts.updatePayload(digest, chunk);
                       return R2dbcBatchWriterChunks.trackActiveChunk(
                               activeChunk,
                               chunk,
                               chunks.executeChunk(resource, request, chunk, transportSql)
                                     .doOnNext(completed::add));
                   }, 0))
                   .then(Mono.defer(() -> completeReceiptAndCommit(
                           resource, request, planHash, token, completed, digest)))
                   .onErrorResume(error -> error instanceof BatchWriteException
                            ? Mono.error(error)
                            : rollback(resource, completed, error, token.get(), acceptedRows.get()));
    }

    private Mono<BatchWriteResult> recoverReceiptFailure(BatchWriteRequest request,
                                                         Mono<BatchWriteResult> replay,
                                                         Throwable failure) {
        VirtualMachineError fatal = findVirtualMachineError(failure);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        if (R2dbcBatchReceiptReservationConflict.find(failure) != null) {
            return replay.switchIfEmpty(Mono.error(failure));
        }
        return failure instanceof BatchWriteException batchFailure
                ? confirmer.confirmAtomic(request, batchFailure)
                : Mono.error(failure);
    }

    private Mono<BatchWriteResult> completeReceiptAndCommit(R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            String planHash,
            AtomicReference<BatchChunkResult.RecoveryToken> token,
            List<BatchChunkResult> completed,
            BatchReceiptDigest digest) {
        String payloadHash = receipts.finishPayload(digest);
        long rowCount = R2dbcExecutionCounts.sum(completed.stream().mapToLong(BatchChunkResult::inputCount));
        long affectedRows = R2dbcExecutionCounts.sum(completed.stream().mapToLong(BatchChunkResult::affectedRows));
        BatchChunkResult.RecoveryToken completedToken = receipts.recoveryToken(
                request, 0, planHash, payloadHash, rowCount, affectedRows);
        token.set(completedToken);
        return receiptStore.complete(resource.connection(),
                                     request.options().recovery(),
                                     0,
                                     payloadHash,
                                     rowCount,
                                     affectedRows)
                           .then(commit(resource, request, completed, completedToken));
    }

    private Mono<BatchWriteResult> commit(R2dbcBatchConnectionHandle resource,
                                          BatchWriteRequest request,
                                          List<BatchChunkResult> completed,
                                          BatchChunkResult.RecoveryToken recoveryToken) {
        if (connections.isExternal(resource)) {
            // SQL 已在外层事务连接上执行，但只有外层提交成功后才能叫 COMMITTED。
            return externalCompletion.enlist(resource, request, completed);
        }
        // 从 COMMITTING 开始，连接故障无法证明数据库没有提交，只能返回 UNKNOWN。
        return connections.commit(resource)
                   .thenReturn(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, completed))
                   .onErrorResume(error -> Mono.error(new BatchWriteException(
                           "atomic batch commit result is unknown",
                           error,
                           BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                 results.unknownChunks(completed, error, recoveryToken)))));
    }

    private Mono<BatchWriteResult> rollback(R2dbcBatchConnectionHandle resource,
                                            List<BatchChunkResult> completed,
                                            Throwable error,
                                            BatchChunkResult.RecoveryToken recoveryToken,
                                            long acceptedRows) {
        Throwable cause = results.failureCause(error);
        if (connections.isExternal(resource)) {
            BatchWriteResult unknown = results.externalUnknown(completed, error, recoveryToken);
            return Mono.error(new BatchWriteException(
                    "atomic batch failed inside an external transaction",
                    cause,
                    results.accountAcceptedRows(unknown, acceptedRows, error)));
        }
        if (resource.state() == BatchTransactionState.NEW) {
            // BEGIN 没有完成回执时不能假定服务端未开始事务；清理阶段会按 NEW 隔离该连接。
            return Mono.error(new BatchWriteException(
                    "atomic batch begin result is unknown",
                    cause,
                    BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                          results.accountAcceptedRows(
                                                  results.unknownChunks(completed, error, recoveryToken),
                                                  acceptedRows,
                                                  error))));
        }
        BatchWriteResult rolledBack = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                results.accountAcceptedRows(
                        results.rolledBackChunks(completed, error), acceptedRows, error));
        return connections.rollback(resource)
                   .onErrorResume(rollbackError -> {
                       BatchWriteException unknown = new BatchWriteException(
                               "atomic batch rollback result is unknown",
                               rollbackError,
                               BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                       results.accountAcceptedRows(
                                               results.unknownChunks(
                                                       completed, rollbackError, recoveryToken),
                                               acceptedRows,
                                               rollbackError)));
                       // rollback 未确认不能覆盖导致回滚的操作错误；最外层会在清理结束后恢复其中的 fatal。
                       unknown.addSuppressed(error);
                       return Mono.error(unknown);
                   })
                   .then(Mono.<BatchWriteResult>error(new BatchWriteException(
                           "atomic batch rolled back", cause, rolledBack)));
    }

    private Mono<BatchWriteResult> timeout(R2dbcBatchConnectionHandle resource,
                                           List<BatchChunkResult> completed,
                                           Throwable error,
                                           BatchChunkResult.RecoveryToken recoveryToken,
                                           long acceptedRows) {
        if (connections.isExternal(resource)) {
            BatchWriteResult unknown = results.externalUnknown(completed, error, recoveryToken);
            return Mono.error(new BatchWriteException(
                    "atomic batch timed out inside an external transaction",
                    error,
                    results.accountAcceptedRows(unknown, acceptedRows, error)));
        }
        return switch (resource.state()) {
            case COMMITTED -> Mono.just(BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, completed));
            case COMMITTING, ROLLING_BACK -> Mono.error(new BatchWriteException(
                    "atomic batch transaction result is unknown after timeout",
                    error,
                    BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                          results.accountAcceptedRows(
                                                  results.unknownChunks(completed, error, recoveryToken),
                                                  acceptedRows,
                                                  error))));
            case ACTIVE -> rollback(resource, completed, error, recoveryToken, acceptedRows);
            case NEW -> {
                // 与 BEGIN 错误一致：持有连接后的超时无法证明事务没有被服务端接受。
                yield Mono.error(new BatchWriteException(
                        "atomic batch begin result is unknown after timeout",
                        error,
                        BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                              results.accountAcceptedRows(
                                                      results.unknownChunks(
                                                              completed, error, recoveryToken),
                                                      acceptedRows,
                                                      error))));
            }
            case ROLLED_BACK -> Mono.error(new BatchWriteException(
                    "atomic batch rolled back after timeout",
                    error,
                    BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                          results.accountAcceptedRows(
                                                  results.rolledBackChunks(completed, error),
                                                  acceptedRows,
                                                  error))));
        };
    }

}
