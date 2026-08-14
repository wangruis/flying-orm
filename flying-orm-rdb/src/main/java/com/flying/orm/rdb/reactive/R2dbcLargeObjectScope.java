package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.codec.LargeObjectValueCodec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 跟随一个 R2DBC 连接租约管理行内大字段句柄，确保连接归还前完成未订阅句柄的异步释放。
 *
 * @author wangr
 * @date 2026-08-13
 * @version v1.0
 */
final class R2dbcLargeObjectScope {
    private final List<RowState> rows = new ArrayList<>();
    private final List<Drain> drains = new ArrayList<>();

    Mono<DynamicRow> materialize(DynamicRow row, SqlExecutionOptions options) {
        DynamicRow safeRow = Objects.requireNonNull(row, "dynamic row must not be null");
        RowState state = RowState.from(safeRow, Objects.requireNonNull(
                options, "sql execution options must not be null"));
        if (state.slots().isEmpty()) {
            return Mono.just(safeRow);
        }
        register(state);
        return state.read()
                    .doOnSuccess(ignored -> unregister(state))
                    .onErrorResume(primary -> state.discardAfterError(primary)
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
        RowState state = RowState.captured(locators, options);
        register(state);
        return state.discardAfterError(primary)
                    .doFinally(ignored -> unregister(state))
                    .then(Mono.defer(() -> {
                        VirtualMachineError fatal =
                                ReactiveSqlExecutionProtection.findVirtualMachineError(primary);
                        return fatal == null ? Mono.empty() : Mono.error(fatal);
                    }));
    }

    Mono<Void> complete() {
        return cleanup(RowState::discardPending);
    }
    Mono<Void> cancel() {
        return cleanup(RowState::discardPending);
    }
    Mono<Void> error(Throwable primary) {
        Objects.requireNonNull(primary, "large object primary error must not be null");
        return cleanup(row -> row.discardAfterError(primary));
    }

    synchronized void registerDrain(Mono<Void> completion, Runnable abort, Duration timeout) {
        drains.add(new Drain(Objects.requireNonNull(completion, "result drain completion must not be null"),
                             Objects.requireNonNull(abort, "result drain abort action must not be null"),
                             Objects.requireNonNull(timeout, "result drain timeout must not be null")));
    }

    synchronized boolean hasActiveRows() {
        return !rows.isEmpty();
    }
    private synchronized void register(RowState row) {
        rows.add(Objects.requireNonNull(row, "large object row state must not be null"));
    }

    private synchronized void unregister(RowState row) {
        rows.remove(row);
    }
    private Mono<Void> cleanup(Function<RowState, Mono<Void>> action) {
        List<Drain> drainSnapshot;
        synchronized (this) {
            drainSnapshot = List.copyOf(drains);
        }
        AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
        Mono<Void> awaitDrains = Flux.fromIterable(drainSnapshot)
                                     .concatMap(drain -> drain.await().onErrorResume(error -> {
                                         RowState.merge(cleanupFailure, error);
                                         return Mono.empty();
                                     }), 1)
                                     .then();
        return awaitDrains.then(Mono.defer(() -> {
            List<RowState> rowSnapshot;
            synchronized (this) {
                rowSnapshot = List.copyOf(rows);
            }
            return Flux.fromIterable(rowSnapshot)
                       .concatMap(row -> action.apply(row)
                               .onErrorResume(error -> {
                                   RowState.merge(cleanupFailure, error);
                                   return Mono.empty();
                               })
                               .doFinally(ignored -> unregister(row)), 1)
                       .then();
        }))
                   .then(Mono.defer(() -> cleanupFailure.get() == null
                           ? Mono.empty() : Mono.error(cleanupFailure.get())));
    }

    private record Drain(Mono<Void> completion, Runnable abort, Duration timeout) {

        private Mono<Void> await() {
            if (timeout.isZero()) {
                return completion;
            }
            return completion.timeout(timeout).doOnError(ignored -> abort.run());
        }
    }

    private enum State {
        PENDING,
        STREAMING,
        RELEASED,
        DISCARDING,
        DISCARDED
    }
    private static final class RowState {

        private final DynamicRow row;
        private final SqlExecutionOptions options;
        private final List<LobSlot> slots;
        private final Map<Integer, Object> replacements = new LinkedHashMap<>();

        private RowState(DynamicRow row, SqlExecutionOptions options, List<LobSlot> slots) {
            this.row = row;
            this.options = options;
            this.slots = slots;
        }

        static RowState from(DynamicRow row, SqlExecutionOptions options) {
            List<LobSlot> slots = new ArrayList<>();
            Map<Object, LobSlot> unique = new IdentityHashMap<>();
            for (int index = 0; index < row.columnCount(); index++) {
                Object value = row.value(index);
                if (value instanceof Blob || value instanceof Clob) {
                    LobSlot slot = unique.get(value);
                    if (slot == null) {
                        slot = new LobSlot(index, value, options);
                        unique.put(value, slot);
                        slots.add(slot);
                    } else {
                        slot.addIndex(index);
                    }
                }
            }
            return new RowState(row, options, List.copyOf(slots));
        }

        static RowState captured(List<Object> locators, SqlExecutionOptions options) {
            List<LobSlot> slots = new ArrayList<>(locators.size());
            Map<Object, Boolean> unique = new IdentityHashMap<>();
            for (Object locator : locators) {
                if (unique.put(locator, Boolean.TRUE) == null) {
                    slots.add(new LobSlot(-1, locator, options));
                }
            }
            return new RowState(null, options, List.copyOf(slots));
        }

        List<LobSlot> slots() {
            return slots;
        }

        Mono<DynamicRow> read() {
            return Flux.fromIterable(slots)
                       .concatMap(slot -> slot.read().doOnNext(value ->
                               slot.indexes().forEach(index -> replacements.put(index, value))), 1)
                       .then(Mono.fromSupplier(() -> row.withValues(replacements)));
        }

        Mono<Void> discardPending() {
            AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
            Mono<Void> cleanup = Flux.fromIterable(slots)
                                     .concatMap(slot -> slot.discard().onErrorResume(error -> {
                                         merge(cleanupFailure, error);
                                         return Mono.empty();
                                     }), 1)
                                     .then();
            Duration timeout = options.cleanupTimeout();
            if (!timeout.isZero()) {
                cleanup = cleanup.timeout(timeout).onErrorResume(error -> {
                    merge(cleanupFailure, error);
                    return Mono.empty();
                });
            }
            return cleanup.then(Mono.defer(() -> cleanupFailure.get() == null
                    ? Mono.empty() : Mono.error(cleanupFailure.get())));
        }

        Mono<Void> discardAfterError(Throwable primary) {
            return discardPending().onErrorResume(cleanup -> {
                VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                        primary, cleanup);
                if (fatal != null) {
                    return Mono.error(fatal);
                }
                ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primary, cleanup);
                return Mono.empty();
            });
        }

        private static void merge(AtomicReference<Throwable> target, Throwable failure) {
            Throwable current = target.get();
            if (current == null && target.compareAndSet(null, failure)) {
                return;
            }
            current = target.get();
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(current, failure);
            if (fatal != null && fatal != current) {
                target.set(fatal);
            } else if (fatal == null) {
                ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(current, failure);
            }
        }
    }

    private static final class LobSlot {

        private final List<Integer> indexes = new ArrayList<>();
        private final Object locator;
        private final SqlExecutionOptions options;
        private final AtomicReference<State> state = new AtomicReference<>(State.PENDING);

        private LobSlot(int index, Object locator, SqlExecutionOptions options) {
            indexes.add(index);
            this.locator = locator;
            this.options = options;
        }

        List<Integer> indexes() {
            return indexes;
        }

        void addIndex(int index) {
            indexes.add(index);
        }

        Mono<Object> read() {
            return Mono.defer(() -> {
                if (!state.compareAndSet(State.PENDING, State.STREAMING)) {
                    return Mono.error(new IllegalStateException("R2DBC large object has already been consumed"));
                }
                try {
                    String dataType = locator instanceof Blob ? "BLOB" : "CLOB";
                    return LargeObjectValueCodec.readReactive(locator, dataType, options)
                                                .doFinally(ignored -> state.set(State.RELEASED));
                } catch (Throwable failure) {
                    state.compareAndSet(State.STREAMING, State.PENDING);
                    return Mono.error(failure);
                }
            });
        }

        Mono<Void> discard() {
            return Mono.defer(() -> {
                if (!state.compareAndSet(State.PENDING, State.DISCARDING)) {
                    return Mono.empty();
                }
                Publisher<Void> cleanup;
                try {
                    cleanup = locator instanceof Blob blob ? blob.discard() : ((Clob) locator).discard();
                } catch (Throwable failure) {
                    state.set(State.DISCARDED);
                    return Mono.error(failure);
                }
                if (cleanup == null) {
                    state.set(State.DISCARDED);
                    return Mono.error(new NullPointerException(
                            "R2DBC large object discard publisher must not be null"));
                }
                return Mono.from(cleanup).doFinally(ignored -> state.set(State.DISCARDED));
            });
        }
    }
}
