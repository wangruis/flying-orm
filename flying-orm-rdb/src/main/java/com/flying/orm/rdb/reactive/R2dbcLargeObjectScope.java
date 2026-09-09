package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 跟随一个 R2DBC 连接租约管理行内大字段句柄，确保连接归还前完成未订阅句柄的异步释放。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
final class R2dbcLargeObjectScope {
    private final List<R2dbcLargeObjectRow> rows = new ArrayList<>();
    private final AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
    private boolean rowsClosed;

    /** Captures and registers row resources before cleanup can take its snapshot. */
    synchronized Mono<DynamicRow> captureRow(Supplier<Mono<DynamicRow>> reader) {
        if (rowsClosed) {
            return Mono.error(new IllegalStateException("R2DBC large object scope is already closing"));
        }
        return reader.get();
    }

    Mono<DynamicRow> materialize(DynamicRow row, SqlExecutionOptions options) {
        DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
        R2dbcLargeObjectRow state = R2dbcLargeObjectRow.from(safeRow, Objects.requireNonNull(
                options, "sql execution options must not be null"));
        if (state.isEmpty()) {
            return Mono.just(safeRow);
        }
        if (register(state)) {
            return state.discardPending()
                    .doFinally(ignored -> unregister(state))
                    .then(Mono.error(new IllegalStateException(
                            "R2DBC large object scope is already closing")));
        }
        return state.read()
                .doOnSuccess(ignored -> unregister(state))
                .onErrorResume(primary -> state.discardAfterError(primary, this::recordCleanupFailure)
                        .doFinally(ignored -> unregister(state))
                        .then(Mono.defer(() -> {
                            VirtualMachineError fatal = findVirtualMachineError(primary);
                            return Mono.error(fatal == null ? primary : fatal);
                        })));
    }

    Mono<Void> discardCaptured(List<Object> locators,
                               SqlExecutionOptions options,
                               Throwable primary) {
        R2dbcLargeObjectRow state = R2dbcLargeObjectRow.captured(locators, options);
        register(state);
        return state.discardAfterError(primary, this::recordCleanupFailure)
                .doFinally(ignored -> unregister(state))
                .then(Mono.defer(() -> {
                    VirtualMachineError fatal = findVirtualMachineError(primary);
                    return fatal == null ? Mono.empty() : Mono.error(fatal);
                }));
    }

    Mono<Void> complete() {
        return cleanup(R2dbcLargeObjectRow::discardPending);
    }

    Mono<Void> cancel() {
        return cleanup(R2dbcLargeObjectRow::discardPending);
    }

    Mono<Void> error(Throwable primary) {
        Objects.requireNonNull(primary, "large object primary error must not be null");
        return cleanup(row -> row.discardAfterError(primary, this::recordCleanupFailure));
    }

    /** Returns a LOB release failure already attached to the operation error. */
    Throwable cleanupFailure() {
        return cleanupFailure.get();
    }

    private void recordCleanupFailure(Throwable failure) {
        R2dbcLargeObjectRow.merge(cleanupFailure, failure);
    }

    private synchronized boolean register(R2dbcLargeObjectRow row) {
        rows.add(Objects.requireNonNull(row, "large object row state must not be null"));
        return rowsClosed;
    }

    private synchronized void unregister(R2dbcLargeObjectRow row) {
        rows.remove(row);
    }

    private Mono<Void> cleanup(Function<R2dbcLargeObjectRow, Mono<Void>> action) {
        return Mono.defer(() -> {
            AtomicReference<Throwable> aggregateFailure = new AtomicReference<>();
            List<R2dbcLargeObjectRow> rowSnapshot;
            synchronized (this) {
                rowsClosed = true;
                rowSnapshot = List.copyOf(rows);
            }
            return Flux.fromIterable(rowSnapshot)
                       .concatMap(row -> action.apply(row)
                               .onErrorResume(error -> {
                                   R2dbcLargeObjectRow.merge(aggregateFailure, error);
                                   return Mono.empty();
                               })
                               .doFinally(ignored -> unregister(row)), 1)
                       .then()
                       .then(Mono.defer(() -> aggregateFailure.get() == null
                               ? Mono.empty() : Mono.error(aggregateFailure.get())));
        });
    }
}
