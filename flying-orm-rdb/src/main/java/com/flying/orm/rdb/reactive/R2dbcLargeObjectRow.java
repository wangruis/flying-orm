package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import com.flying.orm.core.type.DatabaseType;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * 管理一行 R2DBC 结果中的 LOB locator 状态，只允许读取或释放一次。
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class R2dbcLargeObjectRow {

    private static final DatabaseType BLOB_TYPE = DatabaseType.of("BLOB");
    private static final DatabaseType CLOB_TYPE = DatabaseType.of("CLOB");

    private final DynamicRow row;
    private final SqlExecutionOptions options;
    private final List<LobSlot> slots;
    private final Map<Integer, Object> replacements = new LinkedHashMap<>();

    private R2dbcLargeObjectRow(DynamicRow row,
                                SqlExecutionOptions options,
                                List<LobSlot> slots) {
        this.row = row;
        this.options = options;
        this.slots = slots;
    }

    static R2dbcLargeObjectRow from(DynamicRow row, SqlExecutionOptions options) {
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
        return new R2dbcLargeObjectRow(row, options, List.copyOf(slots));
    }

    static R2dbcLargeObjectRow captured(List<Object> locators, SqlExecutionOptions options) {
        List<LobSlot> slots = new ArrayList<>(locators.size());
        Map<Object, Boolean> unique = new IdentityHashMap<>();
        for (Object locator : locators) {
            if (unique.put(locator, Boolean.TRUE) == null) {
                slots.add(new LobSlot(-1, locator, options));
            }
        }
        return new R2dbcLargeObjectRow(null, options, List.copyOf(slots));
    }

    boolean isEmpty() {
        return slots.isEmpty();
    }

    Mono<DynamicRow> read() {
        return Flux.fromIterable(slots)
                   .concatMap(slot -> slot.read().doOnNext(value ->
                           slot.indexes().forEach(index -> replacements.put(index, value))), 1)
                   .then(Mono.fromSupplier(() -> row.withValues(replacements)));
    }

    Duration cleanupTimeout() {
        return options.cleanupTimeout();
    }

    Mono<Void> discardPending(R2dbcCleanupDeadline deadline) {
        AtomicReference<Throwable> cleanupFailure = new AtomicReference<>();
        Mono<Void> cleanup = Flux.fromIterable(slots)
                                 .concatMap(slot -> slot.discard().onErrorResume(error -> {
                                     merge(cleanupFailure, error);
                                     return Mono.empty();
                                 }), 1)
                                 .then();
        return deadline.protect(cleanup).onErrorResume(error -> {
            merge(cleanupFailure, error);
            return Mono.empty();
        }).then(Mono.defer(() -> cleanupFailure.get() == null
                ? Mono.empty() : Mono.error(cleanupFailure.get())));
    }

    Mono<Void> discardAfterError(Throwable primary,
                                 R2dbcCleanupDeadline deadline,
                                 Consumer<Throwable> cleanupFailureRecorder) {
        return discardPending(deadline).onErrorResume(cleanup -> {
            VirtualMachineError fatal = promoteVirtualMachineError(primary, cleanup);
            if (fatal != null) {
                return Mono.error(fatal);
            }
            addSuppressedIfAcyclic(primary, cleanup);
            cleanupFailureRecorder.accept(cleanup);
            return Mono.empty();
        });
    }

    static void merge(AtomicReference<Throwable> target, Throwable failure) {
        Throwable current = target.get();
        if (current == null && target.compareAndSet(null, failure)) {
            return;
        }
        current = target.get();
        VirtualMachineError fatal = promoteVirtualMachineError(current, failure);
        if (fatal != null && fatal != current) {
            target.set(fatal);
        } else if (fatal == null) {
            addSuppressedIfAcyclic(current, failure);
        }
    }

    private enum State {
        PENDING,
        STREAMING,
        RELEASED,
        DISCARDING,
        DISCARDED
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
                    DatabaseType dataType = locator instanceof Blob ? BLOB_TYPE : CLOB_TYPE;
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
                return Mono.from(cleanup).doFinally(ignored -> state.set(State.DISCARDED));
            });
        }
    }
}
