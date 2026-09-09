package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

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
import java.util.concurrent.atomic.AtomicLong;

/** Executes an atomic batch on the transaction supplied by the caller. */
final class R2dbcAtomicBatchWriter {
    private final R2dbcBatchWriterChunks chunks;
    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcBatchResultAssembler results;
    private final R2dbcExternalBatchCompletion externalCompletion;

    R2dbcAtomicBatchWriter(R2dbcBatchWriterChunks chunks,
                           R2dbcBatchConnectionLifecycle connections,
                           R2dbcBatchResultAssembler results,
                           R2dbcExternalBatchCompletion externalCompletion) {
        this.chunks = Objects.requireNonNull(chunks, "batch chunk writer must not be null");
        this.connections = Objects.requireNonNull(connections, "batch connection lifecycle must not be null");
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
        this.externalCompletion = Objects.requireNonNull(externalCompletion, "external completion must not be null");
    }

    Mono<BatchWriteResult> write(BatchWriteRequest request,
                                 ReactiveTransactionSourceResolver.Resolution resolution,
                                 String transportSql) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        return Mono.defer(() -> {
            AtomicLong acceptedRows = new AtomicLong();
            return chunks.chunks(safeRequest, acceptedRows::set).switchOnFirst((signal, chunkFlux) -> {
                if (signal.hasError()) {
                    return Flux.error(results.failureBeforeTransaction(
                            "atomic batch input failed before a transaction became active",
                            signal.getThrowable(), acceptedRows.get()));
                }
                if (!signal.hasValue()) {
                    return Flux.just(BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC));
                }
                return Flux.usingWhen(
                        connections.acquire(safeRequest.options(), safeResolution),
                        resource -> execute(resource, safeRequest, chunkFlux, acceptedRows, transportSql),
                        connections::closeAfterOutcome,
                        (resource, ignored) -> connections.closeAfterOutcome(resource),
                        resource -> connections.cancel(resource, "atomic"));
            }).single();
        });
    }

    private Mono<BatchWriteResult> execute(R2dbcBatchConnectionHandle resource,
                                           BatchWriteRequest request,
                                           Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
                                           AtomicLong acceptedRows,
                                           String transportSql) {
        List<BatchChunkResult> completed = new ArrayList<>();
        return chunkFlux.concatMap(chunk ->
                        chunks.executeChunk(resource, request, chunk, transportSql)
                                .doOnNext(completed::add), 0)
                .then(Mono.defer(() -> externalCompletion.enlist(resource, request, completed)))
                .onErrorResume(error -> error instanceof BatchWriteException
                        ? Mono.error(error) : externalFailure(completed, error, acceptedRows.get()));
    }

    private Mono<BatchWriteResult> externalFailure(List<BatchChunkResult> completed,
                                                    Throwable error,
                                                    long acceptedRows) {
        Throwable cause = results.failureCause(error);
        VirtualMachineError fatal = findVirtualMachineError(cause);
        if (fatal != null) {
            return Mono.error(fatal);
        }
        BatchWriteResult unknown = results.externalUnknown(completed, error, null);
        return Mono.error(new BatchWriteException(
                "atomic batch failed inside an external transaction",
                cause,
                results.accountAcceptedRows(unknown, acceptedRows, error)));
    }
}
