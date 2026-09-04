package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * R2DBC 批量写入的内部稳定门面。
 *
 * <p>门面只负责检查请求模式并选择 ATOMIC 或 INDEPENDENT 协调器。分片、参数绑定、连接清理、事务状态、
 * 结果拼装和回执身份各自只有一份实现，避免两种模式在安全边界上逐渐产生不同语义。</p>
 *
 * <p>本对象和内部协作者都不保存请求级状态，可以被多个订阅并发复用。每次订阅的截止时间、分片列表、
 * 事务状态和结果集合都在 Reactor 链内部创建。</p>
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
final class R2dbcBatchWriter {

    private final R2dbcAtomicBatchWriter atomicWriter;
    private final R2dbcIndependentBatchWriter independentWriter;
    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcBatchWriterChunks chunks;
    private final R2dbcBindMarkers bindMarkers;
    private final BatchExecutionObserver evidenceObserver;

    R2dbcBatchWriter(ConnectionFactory connectionFactory, BatchReceiptStore receiptStore,
                      R2dbcBindMarkers bindMarkers, SqlExecutionObserver cleanupObserver,
                      BatchExecutionObserver batchObserver,
                      R2dbcTransactionParticipant transactionParticipant) {
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
        this.evidenceObserver = Objects.requireNonNull(batchObserver, "batch execution observer must not be null");
        this.chunks = new R2dbcBatchWriterChunks(this.bindMarkers);
        R2dbcBatchReceiptSupport receipts = new R2dbcBatchReceiptSupport();
        this.connections = new R2dbcBatchConnectionLifecycle(connectionFactory, cleanupObserver, transactionParticipant);
        R2dbcBatchResultAssembler results = new R2dbcBatchResultAssembler();
        R2dbcExternalBatchCompletion externalCompletion = new R2dbcExternalBatchCompletion(results, batchObserver);
        this.atomicWriter = new R2dbcAtomicBatchWriter(
                this.chunks, receiptStore, receipts, connections, results, externalCompletion);
        this.independentWriter = new R2dbcIndependentBatchWriter(
                this.chunks, receiptStore, receipts, connections, results);
    }

    Mono<ReactiveTransactionSourceResolver.Resolution> resolveTransaction() {
        return connections.resolveTransaction();
    }

    Mono<BatchWriteResult> write(BatchWriteRequest request,
                                 ReactiveTransactionSourceResolver.Resolution resolution) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        return Mono.defer(() -> {
            String transportSql = bindMarkers.adapt(safeRequest);
            Mono<BatchWriteResult> execution = safeRequest.options().mode() == BatchWriteOptions.Mode.INDEPENDENT
                    ? independentWriter.write(safeRequest, safeResolution, transportSql)
                    : atomicWriter.write(safeRequest, safeResolution, transportSql);
            // 校验在获取连接、订阅输入和执行第一条 SQL 之前完成。
            return connections.validate(safeRequest.options(), safeResolution).then(execution);
        });
    }

    Flux<BatchChunkResult> writeChunks(BatchWriteRequest request,
                                       ReactiveTransactionSourceResolver.Resolution resolution) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        return Flux.defer(() -> {
            String transportSql = bindMarkers.adapt(safeRequest);
            return connections.validate(safeRequest.options(), safeResolution)
                    .thenMany(independentWriter.writeChunks(safeRequest, safeResolution, transportSql));
        });
    }

    Mono<BatchExecutionEvidence> writeEvidence(BatchWriteRequest request,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch evidence request must not be null");
        ReactiveTransactionSourceResolver.Resolution safeResolution = Objects.requireNonNull(
                resolution, "transaction resolution must not be null");
        if (safeRequest.options().mode() != BatchWriteOptions.Mode.ATOMIC) {
            return Mono.error(new UnsupportedOperationException(
                    "r2dbc batch evidence currently requires ATOMIC mode"));
        }
        if (safeRequest.options().recovery().mode() != BatchWriteOptions.RecoveryMode.NONE) {
            return Mono.error(new UnsupportedOperationException(
                    "r2dbc batch evidence does not support receipt recovery"));
        }
        return Mono.defer(() -> {
            String transportSql = bindMarkers.adapt(safeRequest);
            EvidenceContext context = new EvidenceContext();
            Mono<BatchExecutionEvidence> execution = chunks.chunks(
                            safeRequest, context.acceptedRows::set)
                    .switchOnFirst((signal, chunkFlux) -> {
                        if (signal.hasError()) {
                            Throwable failure = Objects.requireNonNull(signal.getThrowable());
                            return Flux.error(context.failure(
                                    "r2dbc batch input failed before a transaction became active",
                                    failure, BatchCommitFact.NOT_APPLICABLE,
                                    R2dbcBatchEvidenceFailure.failureState(failure)));
                        }
                        if (!signal.hasValue()) {
                            return Flux.just(context.success(BatchCommitFact.NOT_APPLICABLE));
                        }
                        return Flux.usingWhen(
                                connections.acquire(safeRequest.options(), safeResolution)
                                        .onErrorMap(failure -> failure instanceof Error ? failure : context.failure(
                                                "r2dbc batch connection acquisition failed",
                                                failure, BatchCommitFact.NOT_APPLICABLE,
                                                R2dbcBatchEvidenceFailure.failureState(failure))),
                                resource -> {
                                    context.resourceAcquired.set(true);
                                    context.resource = resource;
                                    return executeEvidence(
                                            resource, safeRequest, chunkFlux, transportSql, context);
                                },
                                connections::closeAfterOutcome,
                                (resource, ignored) -> connections.cancel(resource, "atomic evidence"),
                                resource -> connections.cancel(resource, "atomic evidence")
                                        .then(Mono.fromRunnable(() -> context.publish(
                                                cancellationEvidence(resource, context), evidenceObserver))))
                                // usingWhen 只会在错误清理完成后发布原始失败；此时才能读取确定的回滚终态。
                                .onErrorMap(PendingEvidenceFailure.class, failure -> context.failure(
                                        failure.evidenceMessage,
                                        failure.getCause(),
                                        failureCommitFact(context.resource(), context),
                                        failure.state));
                    })
                    .single();
            return connections.validate(safeRequest.options(), safeResolution)
                    .then(execution)
                    .doOnSuccess(evidence -> context.publish(evidence, evidenceObserver))
                    .doOnError(BatchExecutionEvidenceException.class,
                            failure -> context.publish(failure.evidence(), evidenceObserver))
                    .doOnCancel(() -> {
                        if (!context.resourceAcquired.get()) {
                            context.publish(context.evidence(
                                    BatchCommitFact.NOT_APPLICABLE,
                                    BatchExecutionState.CANCELLED,
                                    new CancellationException("r2dbc batch evidence subscription was cancelled")),
                                    evidenceObserver);
                        }
                    });
        });
    }

    private Mono<BatchExecutionEvidence> executeEvidence(
            R2dbcBatchConnectionHandle resource,
            BatchWriteRequest request,
            Flux<R2dbcBatchWriterChunks.BatchChunk> chunkFlux,
            String transportSql,
            EvidenceContext context) {
        R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(request.options().timeout());
        Mono<BatchExecutionEvidence> execution = connections.begin(resource)
                .thenMany(chunkFlux.concatMap(chunk -> Mono.defer(() -> {
                    R2dbcBatchEvidenceCounts counts = new R2dbcBatchEvidenceCounts();
                    EvidenceContext.ActiveChunk active = new EvidenceContext.ActiveChunk(chunk, counts);
                    context.activeChunk.set(active);
                    return chunks.executeBatchEvidence(resource, request, chunk, transportSql, counts)
                            .doOnNext(fact -> {
                                context.recordDatabaseWork(counts);
                                context.add(fact);
                                context.activeChunk.compareAndSet(active, null);
                            })
                            .doOnError(R2dbcBatchEvidenceFailure.class, failure -> {
                                context.recordDatabaseWork(counts);
                                context.add(failure.fact());
                                context.activeChunk.compareAndSet(active, null);
                            });
                }), 0))
                .then(Mono.defer(() -> finishEvidence(resource, context)));
        return deadline.protect(execution)
                .onErrorMap(TimeoutException.class, failure -> new PendingEvidenceFailure(
                        "r2dbc batch evidence timed out",
                        failure,
                        BatchExecutionState.TIMED_OUT))
                .onErrorMap(failure -> failure instanceof BatchExecutionEvidenceException
                        || failure instanceof PendingEvidenceFailure
                        ? failure
                        : new PendingEvidenceFailure(
                                "r2dbc batch evidence execution failed",
                                failure instanceof R2dbcBatchEvidenceFailure
                                        ? failure.getCause() : failure,
                                failure instanceof R2dbcBatchEvidenceFailure evidenceFailure
                                        ? evidenceFailure.fact().state()
                                        : R2dbcBatchEvidenceFailure.failureState(failure)));
    }

    private Mono<BatchExecutionEvidence> finishEvidence(
            R2dbcBatchConnectionHandle resource,
            EvidenceContext context) {
        BatchCommitFact commitFact = connections.isExternal(resource)
                ? BatchCommitFact.PENDING_EXTERNAL : BatchCommitFact.COMMITTED;
        Mono<Void> commit = connections.isExternal(resource)
                ? Mono.empty() : connections.commit(resource);
        return commit.thenReturn(context.success(commitFact));
    }

    private BatchCommitFact failureCommitFact(
            R2dbcBatchConnectionHandle resource,
            EvidenceContext context) {
        if (connections.isExternal(resource)) {
            return context.databaseWorkAttempted()
                    ? BatchCommitFact.PENDING_EXTERNAL : BatchCommitFact.NOT_APPLICABLE;
        }
        return switch (resource.state()) {
            case COMMITTED -> BatchCommitFact.COMMITTED;
            case ROLLED_BACK -> BatchCommitFact.ROLLED_BACK;
            default -> BatchCommitFact.UNKNOWN;
        };
    }

    private BatchExecutionEvidence cancellationEvidence(
            R2dbcBatchConnectionHandle resource,
            EvidenceContext context) {
        BatchCommitFact commitFact;
        if (connections.isExternal(resource)) {
            commitFact = context.databaseWorkAttempted()
                    ? BatchCommitFact.PENDING_EXTERNAL : BatchCommitFact.NOT_APPLICABLE;
        } else {
            commitFact = switch (resource.state()) {
                case COMMITTED -> BatchCommitFact.COMMITTED;
                case ROLLED_BACK -> BatchCommitFact.ROLLED_BACK;
                default -> BatchCommitFact.UNKNOWN;
            };
        }
        return context.evidence(
                commitFact,
                BatchExecutionState.CANCELLED,
                new CancellationException("r2dbc batch evidence subscription was cancelled"));
    }

    /**
     * 错误清理前只保存失败上下文，不提前冻结事务事实。usingWhen 完成回滚/关闭后，外层再生成公开 evidence。
     */
    private static final class PendingEvidenceFailure extends RuntimeException {

        private final String evidenceMessage;
        private final BatchExecutionState state;

        private PendingEvidenceFailure(String evidenceMessage,
                                       Throwable failure,
                                       BatchExecutionState state) {
            super(evidenceMessage, failure);
            this.evidenceMessage = Objects.requireNonNull(
                    evidenceMessage, "batch evidence failure message must not be null");
            this.state = Objects.requireNonNull(state, "batch evidence state must not be null");
        }
    }

    private static final class EvidenceContext {

        private final List<BatchChunkExecutionFact> facts = new ArrayList<>();
        private final AtomicLong acceptedRows = new AtomicLong();
        private final AtomicReference<ActiveChunk> activeChunk =
                new AtomicReference<>();
        private final AtomicBoolean resourceAcquired = new AtomicBoolean();
        private final AtomicBoolean databaseWorkAttempted = new AtomicBoolean();
        private final AtomicBoolean published = new AtomicBoolean();
        private volatile R2dbcBatchConnectionHandle resource;

        private R2dbcBatchConnectionHandle resource() {
            return Objects.requireNonNull(resource, "batch evidence resource must be available after cleanup");
        }

        private synchronized void add(BatchChunkExecutionFact fact) {
            facts.add(Objects.requireNonNull(fact, "batch chunk evidence must not be null"));
        }

        private void recordDatabaseWork(R2dbcBatchEvidenceCounts counts) {
            if (counts.databaseWorkAttempted()) {
                databaseWorkAttempted.set(true);
            }
        }

        private boolean databaseWorkAttempted() {
            if (databaseWorkAttempted.get()) {
                return true;
            }
            ActiveChunk active = activeChunk.get();
            return active != null && active.counts().databaseWorkAttempted();
        }

        private BatchExecutionEvidence success(BatchCommitFact commitFact) {
            return BatchExecutionEvidence.of(
                    BatchWriteOptions.Mode.ATOMIC,
                    BatchExecutionState.SUCCESS,
                    commitFact,
                    snapshotFacts(null, null));
        }

        private BatchExecutionEvidenceException failure(
                String message,
                Throwable failure,
                BatchCommitFact commitFact,
                BatchExecutionState state) {
            return new BatchExecutionEvidenceException(
                    message,
                    failure,
                    evidence(commitFact, state, failure));
        }

        private BatchExecutionEvidence evidence(
                BatchCommitFact commitFact,
                BatchExecutionState terminalState,
                Throwable failure) {
            List<BatchChunkExecutionFact> snapshot = snapshotFacts(terminalState, failure);
            BatchExecutionState state = terminalState;
            if (terminalState != BatchExecutionState.SUCCESS
                    && snapshot.stream().anyMatch(fact -> fact.state() == BatchExecutionState.SUCCESS
                            || fact.successfulCount() > 0)) {
                state = BatchExecutionState.PARTIAL;
            }
            return BatchExecutionEvidence.of(
                    BatchWriteOptions.Mode.ATOMIC,
                    state,
                    commitFact,
                    snapshot);
        }

        private synchronized List<BatchChunkExecutionFact> snapshotFacts(
                BatchExecutionState terminalState,
                Throwable failure) {
            List<BatchChunkExecutionFact> snapshot = new ArrayList<>(facts);
            if (terminalState == null) {
                return snapshot;
            }
            ActiveChunk active = activeChunk.get();
            if (active != null && snapshot.stream().noneMatch(
                    fact -> fact.chunkIndex() == active.chunk().chunkIndex())) {
                snapshot.add(active.counts().failureFact(
                        active.chunk(), terminalState, failure));
                return snapshot;
            }
            long accounted = snapshot.stream().mapToLong(BatchChunkExecutionFact::inputCount).sum();
            long missing = acceptedRows.get() - accounted;
            if (missing > 0L) {
                snapshot.add(terminalFact(
                        snapshot.size(),
                        accounted,
                        Math.toIntExact(missing),
                        terminalState,
                        failure));
            } else if (snapshot.isEmpty()) {
                snapshot.add(terminalFact(0, 0L, 0, terminalState, failure));
            }
            return snapshot;
        }

        private static BatchChunkExecutionFact terminalFact(
                int chunkIndex,
                long startOffset,
                int inputCount,
                BatchExecutionState state,
                Throwable failure) {
            return BatchChunkExecutionFact.of(
                    chunkIndex,
                    startOffset,
                    inputCount,
                    List.of(),
                    List.of(),
                    state,
                    com.flying.orm.rdb.batch.BatchAffectedRows.unknown(),
                    BatchChunkResult.Failure.from(failure));
        }

        private void publish(BatchExecutionEvidence evidence, BatchExecutionObserver observer) {
            if (observer.enabled() && published.compareAndSet(false, true)) {
                observer.onExecutionEvidence(evidence);
            }
        }

        private record ActiveChunk(R2dbcBatchWriterChunks.BatchChunk chunk,
                                   R2dbcBatchEvidenceCounts counts) {
        }
    }
}
