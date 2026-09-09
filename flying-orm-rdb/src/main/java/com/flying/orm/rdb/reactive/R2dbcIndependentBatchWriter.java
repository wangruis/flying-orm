package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
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
    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcBatchResultAssembler results;
    private final R2dbcIndependentBatchFlow flow;

    R2dbcIndependentBatchWriter(R2dbcBatchWriterChunks chunks,
                                R2dbcBatchConnectionLifecycle connections,
                                R2dbcBatchResultAssembler results) {
        this.chunks = Objects.requireNonNull(chunks, "batch chunk writer must not be null");
        this.connections = Objects.requireNonNull(connections, "batch connection lifecycle must not be null");
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
        this.flow = new R2dbcIndependentBatchFlow(chunks, results);
    }

    Mono<BatchWriteResult> write(
            BatchWriteRequest request,
            ReactiveTransactionSourceResolver.Resolution resolution,
            String transportSql) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
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
        return flow.writeChunks(safeRequest,
                                chunk -> executeChunk(safeRequest, chunk, safeResolution, transportSql));
    }

    private Mono<BatchChunkResult> executeChunk(BatchWriteRequest request,
                                                 R2dbcBatchWriterChunks.BatchChunk chunk,
                                                 ReactiveTransactionSourceResolver.Resolution resolution,
                                                 String transportSql) {
        return Mono.usingWhen(
                connections.acquire(request.options(), resolution),
                resource -> connections.begin(resource)
                        .then(chunks.executeChunk(resource, request, chunk, transportSql))
                        .flatMap(result -> commit(request, resource, result))
                        .onErrorResume(error -> rollback(resource, chunk, error)),
                connections::closeAfterOutcome,
                (resource, ignored) -> connections.closeAfterOutcome(resource),
                resource -> connections.cancel(resource, "independent"));
    }

    private Mono<BatchChunkResult> commit(BatchWriteRequest request,
                                          R2dbcBatchConnectionHandle resource,
                                          BatchChunkResult result) {
        return connections.commit(resource)
                   .thenReturn(result)
                   .onErrorResume(error -> {
                       VirtualMachineError fatal = findVirtualMachineError(error);
                       if (fatal != null) {
                           return Mono.error(fatal);
                       }
                       BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                               result, error, null);
                       return Mono.just(unknown);
                   })
                   .doOnNext(committed -> {
                       // 通知仍在连接归还前，但通知故障不能改写驱动已经确认的提交结果。
                       if (committed.status() == BatchChunkResult.Status.COMMITTED
                               && request.completion() instanceof BatchChunkCompletion completion) {
                           try {
                               completion.afterChunk(committed);
                           } catch (Throwable failure) {
                               throw R2dbcBatchChunkWriteFailure.exact(committed, failure);
                           }
                       }
                   });
    }

    private Mono<BatchChunkResult> rollback(R2dbcBatchConnectionHandle resource,
                                            R2dbcBatchWriterChunks.BatchChunk chunk,
                                            Throwable error) {
        BatchTransactionState state = resource.state();
        VirtualMachineError fatal = findVirtualMachineError(results.failureCause(error));
        if (fatal != null) {
            if (state != BatchTransactionState.ACTIVE) {
                return Mono.error(fatal);
            }
            return connections.rollback(resource)
                    .onErrorResume(rollbackError -> Mono.error(promoteVirtualMachineError(fatal, rollbackError)))
                    .then(Mono.error(fatal));
        }
        if (state != BatchTransactionState.NEW && state != BatchTransactionState.ACTIVE) {
            return Mono.error(error);
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
                    chunk, error, null);
            return Mono.just(unknown);
        }
        return connections.rollback(resource)
                   .thenReturn(failed)
                   .onErrorResume(rollbackError -> {
                       VirtualMachineError rollbackFatal = promoteVirtualMachineError(error, rollbackError);
                       if (rollbackFatal != null) {
                           return Mono.error(rollbackFatal);
                       }
                       BatchChunkResult unknown = R2dbcBatchChunkWriteFailure.unknownResult(
                               chunk, rollbackError, null);
                       return Mono.just(unknown);
                     });
    }
}
