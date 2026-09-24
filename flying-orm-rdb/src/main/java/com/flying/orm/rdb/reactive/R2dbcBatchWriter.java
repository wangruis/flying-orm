package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.lifecycle.EntityPostWriteException;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongFunction;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;
import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;

/** One sequential, fail-stop ordinary batch execution chain. */
final class R2dbcBatchWriter {
    private final R2dbcExecutionSession session;
    private final R2dbcBindMarkers markers;
    private final R2dbcBatchWriterChunks statements;
    private final ReactiveBatchExecutionObservationSupport observations;
    private final BatchExecutionObserver observer;

    R2dbcBatchWriter(R2dbcExecutionSession session, R2dbcBindMarkers markers,
                     SqlExecutionObserver sqlObserver, BatchExecutionObserver batchObserver) {
        this.session = session;
        this.markers = markers;
        this.statements = new R2dbcBatchWriterChunks(markers);
        this.observer = BatchExecutionObserver.composite(BatchExecutionObserver.noop(), batchObserver);
        this.observations = new ReactiveBatchExecutionObservationSupport(
                ReactiveSqlExecutionObservationSupport.create(sqlObserver, BatchExecutionObserver.noop()), observer);
    }

    Mono<BatchExecutionEvidence> write(BatchWriteRequest request,
                                       LongFunction<? extends Publisher<Void>> rowCompleted) {
        return Mono.defer(() -> {
            BatchState state = new BatchState();
            long started = System.nanoTime();
            String sql = markers.adapt(request.sql(), request.parameterCount(), request.bindMarkerStyle());
            Mono<BatchExecutionEvidence> execution = R2dbcBatchChunker.chunks(request, state::accept)
                    .switchOnFirst((first, windows) -> {
                if (!first.hasValue()) return windows.then(Mono.fromSupplier(() ->
                        state.snapshot(BatchExecutionState.SUCCESS, null)));
                var firstWindow = Objects.requireNonNull(first.get());
                var firstRow = firstWindow.rows().getFirst();
                SqlRequest representative = firstRow.work() != null
                        && firstRow.work().kind() == ProtectedWriteWork.Kind.UPDATE
                        ? firstRow.work().ownerQuery() : new SqlRequest(request.statement(),
                                Arrays.asList(firstRow.row()).subList(0, firstRow.parameterCount()));
                return session.withConnection(representative, resources ->
                        windows.concatMap(window -> executeWindow(resources, request, window, sql,
                                state, rowCompleted, started), 0).then(Mono.fromSupplier(() ->
                                state.snapshot(BatchExecutionState.SUCCESS, null))),
                        SqlExecutionOperation.BATCH_WRITE);
            }).single().onErrorMap(error -> failure(state, error));
            return observations.observeResult(request, execution,
                    () -> state.terminal(BatchExecutionState.CANCELLED, null));
        });
    }

    private Mono<Void> executeWindow(R2dbcExecutionSession.Resources resources, BatchWriteRequest request,
                                      R2dbcBatchWriterChunks.BatchChunk window, String sql, BatchState state,
                                      LongFunction<? extends Publisher<Void>> rowCompleted, long started) {
        // Owner reads must observe every preceding row's completed business and side-index work.
        boolean sequential = window.rows().size() > 1 && window.rows().stream().anyMatch(row -> row.work() != null
                && row.work().kind() == ProtectedWriteWork.Kind.UPDATE);
        if (sequential) {
            return Flux.range(0, window.rows().size()).concatMap(index -> {
                var row = window.rows().get(index);
                var single = new R2dbcBatchWriterChunks.BatchChunk(window.chunkIndex(), window.startOffset() + index,
                        List.of(row), row.estimatedBytes());
                return executeWindow(resources, request, single, sql, state, rowCompleted, started);
            }, 0).then();
        }
        R2dbcBatchEvidenceCounts windowFacts = new R2dbcBatchEvidenceCounts(
                window.rows().size(), window.startOffset(), request.rowCountPolicy(), rowCompleted != null);
        state.activate(windowFacts);
        Mono<Void> work = statements.execute(resources.connection(), request, window, sql,
                        resources::largeObjects, windowFacts)
                .then(Mono.defer(() -> resources.cleanupLargeObjects(SignalType.ON_COMPLETE, null)))
                .doOnSuccess(ignored -> resources.clearCleanedLargeObjects());
        return work.onErrorResume(error -> {
            state.complete(windowFacts, false);
            return completedBeforeFailure(resources, window, windowFacts, rowCompleted, error);
        }).doOnSuccess(ignored -> state.complete(windowFacts, true))
                .then(completedRows(window, null, rowCompleted))
                .doOnSuccess(ignored -> {
                    if (observer.enabled()) observer.onExecution(BatchExecutionObservation.progress(
                            new BatchExecutionObservation.BatchWriteRequestView(
                                    request.sql(), request.parameterCount(), SqlExecutionBackend.R2DBC),
                            state.snapshot(BatchExecutionState.SUCCESS, null), System.nanoTime() - started));
                });
    }

    private static Mono<Void> completedBeforeFailure(R2dbcExecutionSession.Resources resources,
                                                     R2dbcBatchWriterChunks.BatchChunk window, R2dbcBatchEvidenceCounts facts,
                                                     LongFunction<? extends Publisher<Void>> completed, Throwable error) {
        VirtualMachineError fatal = findVirtualMachineError(error);
        if (fatal != null) return Mono.error(fatal);
        if (completed == null || !facts.hasReadyRows() || error instanceof java.util.concurrent.CancellationException)
            return Mono.error(error);
        return resources.cleanupLargeObjects(SignalType.ON_ERROR, error)
                .doOnSuccess(ignored -> resources.clearCleanedLargeObjects())
                .then(completedRows(window, facts, completed))
                .onErrorMap(secondary -> R2dbcExecutionSession.merge(error, secondary))
                .then(Mono.error(error));
    }

    private static Mono<Void> completedRows(R2dbcBatchWriterChunks.BatchChunk window, R2dbcBatchEvidenceCounts eligible,
                                            LongFunction<? extends Publisher<Void>> completed) {
        if (completed == null) return Mono.empty();
        return Mono.defer(() -> {
            CompletionFailures failures = new CompletionFailures();
            Flux<Integer> offsets = Flux.range(0, window.rows().size());
            if (eligible != null) offsets = offsets.filter(eligible::isRowReady);
            return offsets.concatMap(index ->
                    Mono.defer(() -> Mono.from(Objects.requireNonNull(
                            completed.apply(window.startOffset() + index), "row completion publisher must not be null")))
                            .onErrorResume(error -> {
                                VirtualMachineError fatal = findVirtualMachineError(error);
                                if (fatal != null || error instanceof java.util.concurrent.CancellationException)
                                    return Mono.error(fatal == null ? error : fatal);
                                failures.add(error);
                                return Mono.empty();
                            }), 0).then(Mono.defer(() ->
                                    failures.failure == null ? Mono.empty() : Mono.error(failures.failure)));
        });
    }

    private static Throwable failure(BatchState batchState, Throwable original) {
        Throwable error = R2dbcExecutionSession.unwrapCleanupFailure(original);
        VirtualMachineError fatal = findVirtualMachineError(error);
        if (fatal != null) return fatal;
        BatchExecutionEvidence.Failure safeFailure = BatchExecutionEvidence.Failure.from(error);
        BatchExecutionState executionState = switch (safeFailure.kind()) {
            case CANCELLED -> BatchExecutionState.CANCELLED;
            case TIMEOUT, LOCK_TIMEOUT -> BatchExecutionState.TIMED_OUT;
            default -> BatchExecutionState.FAILED;
        };
        BatchExecutionEvidence evidence = batchState.terminal(executionState, safeFailure);
        if (error instanceof EntityPostWriteException) {
            addSuppressedIfAcyclic(error, new BatchExecutionEvidenceException(
                    "batch SQL execution evidence", null, evidence));
            return error;
        }
        return new BatchExecutionEvidenceException("batch SQL execution failed", error, evidence);
    }

    /** Coordinates the current window with terminal callbacks through one synchronization policy. */
    private static final class BatchState {
        private final BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
        private R2dbcBatchEvidenceCounts active;

        private void accept(long count) {
            facts.accept(count);
        }

        private synchronized void activate(R2dbcBatchEvidenceCounts window) {
            active = window;
        }

        private synchronized void complete(R2dbcBatchEvidenceCounts window, boolean windowComplete) {
            if (active != window) {
                return;
            }
            active = null;
            window.appendTo(facts, windowComplete);
        }

        private BatchExecutionEvidence snapshot(BatchExecutionState state,
                                                BatchExecutionEvidence.Failure failure) {
            return facts.snapshot(state, failure);
        }

        private synchronized BatchExecutionEvidence terminal(
                BatchExecutionState state,
                BatchExecutionEvidence.Failure failure) {
            if (active != null) {
                active.appendTo(facts, false);
                active = null;
            }
            BatchExecutionEvidence snapshot = facts.snapshot(state, failure);
            if (state == BatchExecutionState.FAILED && snapshot.successfulCount() > 0) {
                return facts.snapshot(BatchExecutionState.PARTIAL, failure);
            }
            return snapshot;
        }
    }

    /** concatMap serializes callbacks, so this subscription-local accumulator needs no atomic state. */
    private static final class CompletionFailures {
        private Throwable failure;

        private void add(Throwable next) {
            if (failure == null) {
                failure = next;
            } else {
                addSuppressedIfAcyclic(failure, next);
            }
        }
    }
}
