package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongConsumer;

/** 协调 INDEPENDENT 并发分片的停止、收口和结果汇总，不参与具体数据库事务。 */
final class R2dbcIndependentBatchFlow {

    private final R2dbcBatchWriterChunks chunks;
    private final R2dbcBatchResultAssembler results;

    R2dbcIndependentBatchFlow(R2dbcBatchWriterChunks chunks,
                              R2dbcBatchResultAssembler results) {
        this.chunks = Objects.requireNonNull(chunks, "batch chunk writer must not be null");
        this.results = Objects.requireNonNull(results, "batch result assembler must not be null");
    }

    Mono<BatchWriteResult> write(
            BatchWriteRequest request,
            Function<R2dbcBatchWriterChunks.BatchChunk, Mono<BatchChunkResult>> executor) {
        return Mono.defer(() -> {
            List<BatchChunkResult> settled = new ArrayList<>();
            List<Throwable> stopFailures = new ArrayList<>();
            AtomicLong acceptedRows = new AtomicLong();
            Flux<ChunkOutcome> outcomes = settledChunks(request, executor, acceptedRows::set)
                    .doOnNext(outcome -> remember(outcome, settled, stopFailures))
                    .onErrorMap(error -> {
                        if (error instanceof BatchWriteException) {
                            return error;
                        }
                        List<BatchChunkResult> failed = results.accountAcceptedRows(
                                results.withGlobalFailure(settled, error), acceptedRows.get(), error);
                        return new BatchWriteException(
                                "independent batch stopped", results.failureCause(error),
                                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, failed));
                    });
            // POST 的异常不进入 SQL 失败组装，不能把已提交的分片误标为写入失败。
            return completeChunks(request, outcomes)
                    .then(Mono.defer(() -> stopFailures.isEmpty()
                            ? Mono.just(result(settled))
                            : Mono.error(stoppedFailure(settled, stopFailures))));
        });
    }

    Flux<BatchChunkResult> writeChunks(
            BatchWriteRequest request,
            Function<R2dbcBatchWriterChunks.BatchChunk, Mono<BatchChunkResult>> executor) {
        return Flux.defer(() -> {
            List<BatchChunkResult> failed = new ArrayList<>();
            List<Throwable> stopFailures = new ArrayList<>();
            return completeChunks(request, settledChunks(request, executor, ignored -> {
            }))
                    .map(outcome -> {
                        if (outcome.stopFailure() != null) {
                            failed.add(outcome.result());
                            stopFailures.add(outcome.stopFailure());
                        }
                        return outcome.result();
                    })
                    .concatWith(Flux.defer(() -> stopFailures.isEmpty()
                            ? Flux.empty()
                            : Flux.error(stoppedFailure(failed, stopFailures))));
        });
    }

    /** 第一个不可继续的分片停止输入，但已经启动的兄弟事务必须自然收口，不能被 fail-fast 取消。 */
    private Flux<ChunkOutcome> settledChunks(
            BatchWriteRequest request,
            Function<R2dbcBatchWriterChunks.BatchChunk, Mono<BatchChunkResult>> executor,
            LongConsumer acceptedRows) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        Function<R2dbcBatchWriterChunks.BatchChunk, Mono<BatchChunkResult>> safeExecutor =
                Objects.requireNonNull(executor, "batch chunk executor must not be null");
        LongConsumer safeAcceptedRows = Objects.requireNonNull(
                acceptedRows, "accepted row tracker must not be null");
        return Flux.defer(() -> {
            Sinks.Empty<Void> stop = Sinks.empty();
            BatchChunkCompletion completion = safeRequest.completion() instanceof BatchChunkCompletion tracked
                    ? tracked : null;
            return chunks.chunks(safeRequest, safeAcceptedRows)
                    .takeUntilOther(stop.asMono())
                    .flatMapDelayError(chunk -> {
                        Flux<ChunkOutcome> outcome = Mono.defer(() -> safeExecutor.apply(chunk))
                                .map(ChunkOutcome::completed).flux()
                                .onErrorResume(error -> stoppedChunk(chunk, error, stop));
                        return completion == null ? outcome
                                : outcome.map(settled -> notifyCompletion(completion, settled, stop));
                    },
                             safeRequest.options().concurrency(), 1);
        });
    }

    /** 完成通知失败也必须保留该片已知结果；失败收口中的再次通知不能丢掉原异常。 */
    private static ChunkOutcome notifyCompletion(BatchChunkCompletion completion,
                                                  ChunkOutcome outcome,
                                                  Sinks.Empty<Void> stop) {
        try {
            completion.afterChunk(outcome.result());
            return outcome;
        } catch (Throwable failure) {
            stop.tryEmitEmpty();
            if (outcome.stopFailure() != null) {
                addSuppressedIfAcyclic(outcome.stopFailure(), failure);
                return outcome;
            }
            return ChunkOutcome.stopped(outcome.result(), failure);
        }
    }

    private Flux<ChunkOutcome> completeChunks(BatchWriteRequest request, Flux<ChunkOutcome> outcomes) {
        if (!(request.completion() instanceof BatchChunkCompletion completion)) {
            return outcomes;
        }
        // 并发结果已确认主键；POST 串行组合并在每片完成后归还输入需求。
        return outcomes.concatMap(outcome -> Mono.from(completion.afterChunkReleased(outcome.result()))
                .thenReturn(outcome));
    }

    private Flux<ChunkOutcome> stoppedChunk(R2dbcBatchWriterChunks.BatchChunk chunk,
                                            Throwable error,
                                            Sinks.Empty<Void> stop) {
        Throwable failure = wrapChunkFailure(chunk, error);
        stop.tryEmitEmpty();
        return Flux.fromIterable(results.withGlobalFailure(List.of(), failure))
                .map(result -> ChunkOutcome.stopped(result, failure));
    }

    private BatchWriteResult result(List<BatchChunkResult> settled) {
        return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, results.sortedChunks(settled));
    }

    private BatchWriteException stoppedFailure(List<BatchChunkResult> settled,
                                                List<Throwable> failures) {
        BatchWriteException outcome = new BatchWriteException(
                "independent batch stopped", failures.get(0), result(settled));
        for (int index = 1; index < failures.size(); index++) {
            addSuppressedIfAcyclic(outcome, failures.get(index));
        }
        return outcome;
    }

    private static void remember(ChunkOutcome outcome,
                                 List<BatchChunkResult> settled,
                                 List<Throwable> stopFailures) {
        settled.add(outcome.result());
        if (outcome.stopFailure() != null) {
            stopFailures.add(outcome.stopFailure());
        }
    }

    private static Throwable wrapChunkFailure(R2dbcBatchWriterChunks.BatchChunk chunk, Throwable error) {
        return error instanceof R2dbcBatchChunkWriteFailure || error instanceof R2dbcBatchChunkConflictFailure
                || error instanceof BatchWriteException && findVirtualMachineError(error) != null
                ? error
                : new R2dbcBatchChunkWriteFailure(chunk, error);
    }

    private record ChunkOutcome(BatchChunkResult result, Throwable stopFailure) {

        private static ChunkOutcome completed(BatchChunkResult result) {
            return new ChunkOutcome(result, null);
        }

        private static ChunkOutcome stopped(BatchChunkResult result, Throwable failure) {
            return new ChunkOutcome(result, failure);
        }
    }
}
