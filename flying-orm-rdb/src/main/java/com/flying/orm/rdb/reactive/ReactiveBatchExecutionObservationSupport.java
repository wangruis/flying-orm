package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionBackend;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import reactor.core.publisher.Mono;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Supplier;

/** Observes the single ordinary batch evidence chain. */
final class ReactiveBatchExecutionObservationSupport {
    private final ReactiveSqlExecutionObservationSupport sql;
    private final BatchExecutionObserver observer;

    ReactiveBatchExecutionObservationSupport(ReactiveSqlExecutionObservationSupport sql,
                                               BatchExecutionObserver observer) {
        this.sql = sql;
        this.observer = observer;
    }

    Mono<BatchExecutionEvidence> observeResult(BatchWriteRequest request, Mono<BatchExecutionEvidence> source) {
        return observeResult(request, source, null);
    }

    Mono<BatchExecutionEvidence> observeResult(BatchWriteRequest request, Mono<BatchExecutionEvidence> source,
                                               Supplier<BatchExecutionEvidence> cancelled) {
        if (!sql.enabled() && !observer.enabled()) return source;
        return Mono.defer(() -> {
            long started = System.nanoTime();
            var view = new BatchExecutionObservation.BatchWriteRequestView(
                    request.sql(), request.parameterCount(), SqlExecutionBackend.R2DBC);
            var ordinary = sql.enabled() ? sql.start(new ReactiveSqlExecutionObservationSupport
                    .ReactiveSqlObservation.Request(SqlExecutionOperation.BATCH_WRITE,
                            request.sql(), request.parameterCount(), 0, null)) : null;
            return source.doOnSuccess(evidence -> {
                if (evidence == null) return;
                if (ordinary != null) {
                    if (evidence.state() == BatchExecutionState.SUCCESS) ordinary.success(rows(evidence), size(evidence));
                    else if (evidence.state() == BatchExecutionState.CANCELLED) ordinary.cancelled(rows(evidence), size(evidence));
                    else ordinary.error(rows(evidence), size(evidence),
                            new IllegalStateException("batch SQL execution was not successful"));
                }
                publish(view, evidence, started);
            }).doOnError(error -> {
                BatchExecutionEvidence evidence = evidence(error);
                if (ordinary != null) ordinary.error(evidence == null ? 0 : rows(evidence),
                        evidence == null ? 0 : size(evidence), error);
                if (evidence != null) publish(view, evidence, started);
                else if (observer.enabled()) observer.onExecution(
                        BatchExecutionObservation.failedSummary(view, System.nanoTime() - started, error));
            }).doOnCancel(() -> {
                BatchExecutionEvidence evidence = cancelled == null ? null : cancelled.get();
                if (ordinary != null) {
                    if (evidence == null) ordinary.cancelled();
                    else ordinary.cancelled(rows(evidence), size(evidence));
                }
                if (evidence != null) publish(view, evidence, started);
            });
        });
    }

    private void publish(BatchExecutionObservation.BatchWriteRequestView view,
                         BatchExecutionEvidence evidence, long started) {
        if (!observer.enabled()) return;
        observer.onExecution(BatchExecutionObservation.summary(view, evidence, System.nanoTime() - started));
        observer.onExecutionEvidence(evidence);
    }

    private static long rows(BatchExecutionEvidence evidence) {
        return evidence.affectedRows().isKnown() ? evidence.affectedRows().value() : 0L;
    }

    private static int size(BatchExecutionEvidence evidence) {
        return (int) Math.min(Integer.MAX_VALUE, evidence.inputCount());
    }

    static BatchExecutionEvidence evidence(Throwable error) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(error);
        while (!pending.isEmpty()) {
            Throwable next = pending.removeFirst();
            if (!seen.add(next)) continue;
            if (next instanceof BatchExecutionEvidenceException failure) return failure.evidence();
            if (next.getCause() != null) pending.add(next.getCause());
            Collections.addAll(pending, next.getSuppressed());
        }
        return null;
    }
}
