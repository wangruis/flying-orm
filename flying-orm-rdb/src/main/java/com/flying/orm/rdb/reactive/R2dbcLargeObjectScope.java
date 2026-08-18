package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

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
    private R2dbcCleanupDeadline activeCleanup;
    private boolean rowsClosed;

    Mono<DynamicRow> materialize(DynamicRow row, SqlExecutionOptions options) {
        DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
        R2dbcLargeObjectRow state = R2dbcLargeObjectRow.from(safeRow, Objects.requireNonNull(
                options, "sql execution options must not be null"));
        if (state.isEmpty()) {
            return Mono.just(safeRow);
        }
        R2dbcCleanupDeadline cleanup = register(state);
        if (cleanup != null) {
            return state.discardPending(cleanup)
                        .doFinally(ignored -> unregister(state))
                        .then(Mono.error(new IllegalStateException(
                                "R2DBC large object scope is already closing")));
        }
        return state.read()
                    .doOnSuccess(ignored -> unregister(state))
                    .onErrorResume(primary -> state.discardAfterError(
                                                           primary,
                                                           cleanupDeadline(state.cleanupTimeout()),
                                                           this::recordCleanupFailure)
                                                   .doFinally(ignored -> unregister(state))
                                                   .then(Mono.defer(() -> {
                                                       VirtualMachineError fatal =
                                                               ReactiveSqlExecutionProtection
                                                                       .findVirtualMachineError(primary);
                                                       return Mono.error(fatal == null ? primary : fatal);
                                                   })));
    }

    Mono<Void> discardCaptured(List<Object> locators,
                               SqlExecutionOptions options,
                               Throwable primary) {
        R2dbcLargeObjectRow state = R2dbcLargeObjectRow.captured(locators, options);
        R2dbcCleanupDeadline cleanup = register(state);
        R2dbcCleanupDeadline sharedDeadline = cleanup == null
                ? cleanupDeadline(state.cleanupTimeout()) : cleanup;
        Mono<Void> discard = state.discardAfterError(
                primary, sharedDeadline, this::recordCleanupFailure);
        return discard
                    .doFinally(ignored -> unregister(state))
                    .then(Mono.defer(() -> {
                        VirtualMachineError fatal =
                                ReactiveSqlExecutionProtection.findVirtualMachineError(primary);
                        return fatal == null ? Mono.empty() : Mono.error(fatal);
                    }));
    }

    Mono<Void> complete() {
        return complete(cleanupDeadline(cleanupTimeout()));
    }
    Mono<Void> complete(R2dbcCleanupDeadline deadline) {
        return cleanup((row, sharedDeadline) -> row.discardPending(sharedDeadline), deadline);
    }
    Mono<Void> cancel() {
        return cancel(cleanupDeadline(cleanupTimeout()));
    }
    Mono<Void> cancel(R2dbcCleanupDeadline deadline) {
        return cleanup((row, sharedDeadline) -> row.discardPending(sharedDeadline), deadline);
    }
    Mono<Void> error(Throwable primary) {
        return error(primary, cleanupDeadline(cleanupTimeout()));
    }
    Mono<Void> error(Throwable primary, R2dbcCleanupDeadline deadline) {
        Objects.requireNonNull(primary, "large object primary error must not be null");
        return cleanup((row, sharedDeadline) -> row.discardAfterError(
                primary, sharedDeadline, this::recordCleanupFailure), deadline);
    }

    /** @return 已经挂入业务异常、但仍要求淘汰连接的首个普通 LOB 清理失败 */
    Throwable cleanupFailure() {
        return cleanupFailure.get();
    }

    private void recordCleanupFailure(Throwable failure) {
        R2dbcLargeObjectRow.merge(cleanupFailure, failure);
    }

    private synchronized R2dbcCleanupDeadline register(R2dbcLargeObjectRow row) {
        rows.add(Objects.requireNonNull(row, "large object row state must not be null"));
        return rowsClosed ? activeCleanup : null;
    }

    private synchronized void unregister(R2dbcLargeObjectRow row) {
        rows.remove(row);
    }

    /** 首个清理动作建立连接级绝对截止点，后续阶段只能继续消费同一份预算。 */
    synchronized R2dbcCleanupDeadline cleanupDeadline(Duration timeout) {
        if (activeCleanup == null) {
            activeCleanup = R2dbcCleanupDeadline.start(Objects.requireNonNull(
                    timeout, "large object cleanup timeout must not be null"));
        }
        return activeCleanup;
    }

    synchronized R2dbcCleanupDeadline shareCleanupDeadline(R2dbcCleanupDeadline candidate) {
        if (activeCleanup == null) {
            activeCleanup = Objects.requireNonNull(
                    candidate, "large object cleanup deadline must not be null");
        }
        return activeCleanup;
    }

    private Mono<Void> cleanup(BiFunction<R2dbcLargeObjectRow, R2dbcCleanupDeadline, Mono<Void>> action,
                               R2dbcCleanupDeadline deadline) {
        R2dbcCleanupDeadline safeDeadline = shareCleanupDeadline(deadline);
        return Mono.defer(() -> {
            AtomicReference<Throwable> aggregateFailure = new AtomicReference<>();
            List<R2dbcLargeObjectRow> rowSnapshot;
            synchronized (this) {
                rowsClosed = true;
                rowSnapshot = List.copyOf(rows);
            }
            return Flux.fromIterable(rowSnapshot)
                       .concatMap(row -> action.apply(row, safeDeadline)
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

    private synchronized Duration cleanupTimeout() {
        Duration selected = Duration.ZERO;
        for (R2dbcLargeObjectRow row : rows) {
            selected = tighter(selected, row.cleanupTimeout());
        }
        return selected;
    }

    private static Duration tighter(Duration current, Duration candidate) {
        if (current.isZero()) {
            return candidate;
        }
        return candidate.isZero() || current.compareTo(candidate) <= 0 ? current : candidate;
    }

}
