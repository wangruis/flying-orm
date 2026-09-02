package com.flying.orm.rdb.repository;

import com.flying.orm.core.page.PageResult;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 收口响应式 Repository 的生命周期调用。
 *
 * <p>这里不执行 SQL，也不保存实体状态，只负责保证回调顺序：before 回调完成后才执行数据库操作，
 * 数据库操作成功后才执行 after 回调。查询结果使用 concatMap，因此后置回调不会改变数据库返回顺序。
 * 取消和异常仍由 Reactor 链路向下游传播，不在这里偷偷订阅或阻塞。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryLifecycleSupport<T> {

    private final EntityLifecycleDispatcher<T> dispatcher;

    ReactiveRepositoryLifecycleSupport(EntityMetadata<T> metadata,
                                       ReactiveEntityListener<T> listener) {
        this.dispatcher = new EntityLifecycleDispatcher<>(
                Objects.requireNonNull(metadata, "repository entity metadata must not be null"), listener);
    }

    EntityLifecycleDispatcher<T> dispatcher() {
        return dispatcher;
    }

    Mono<Void> fire(EntityLifecyclePhase phase, T entity, Object result) {
        return dispatcher.fire(phase, entity, result);
    }

    Mono<Void> fire(EntityLifecyclePhase phase, T entity, Object result, boolean committed) {
        return dispatcher.fire(phase, entity, result, committed);
    }

    boolean hasWork(EntityLifecyclePhase phase) {
        return dispatcher.hasWork(phase);
    }

    Mono<Long> persist(T entity,
                       Supplier<Mono<R2dbcTransactionContext>> currentTransaction,
                       Function<Boolean, Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_PERSIST, EntityLifecyclePhase.POST_PERSIST,
                     currentTransaction, operation);
    }

    Mono<Long> update(T entity,
                      Supplier<Mono<R2dbcTransactionContext>> currentTransaction,
                      Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_UPDATE, EntityLifecyclePhase.POST_UPDATE,
                     currentTransaction, ignored -> operation.get());
    }

    Mono<Long> remove(T entity,
                      Supplier<Mono<R2dbcTransactionContext>> currentTransaction,
                      Supplier<Mono<Long>> operation) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return write(safeEntity, EntityLifecyclePhase.PRE_REMOVE, EntityLifecyclePhase.POST_REMOVE,
                     currentTransaction, ignored -> operation.get());
    }

    Flux<T> postLoad(Flux<T> entities) {
        Flux<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            return safeEntities;
        }
        // 回调按数据库结果顺序串行完成，避免监听器看到乱序实体。
        return safeEntities.concatMap(entity ->
                fire(EntityLifecyclePhase.POST_LOAD, entity, null).thenReturn(entity));
    }

    Mono<PageResult<T>> postLoad(PageResult<T> page) {
        PageResult<T> safePage = Objects.requireNonNull(page, "repository page must not be null");
        if (!hasWork(EntityLifecyclePhase.POST_LOAD)) {
            return Mono.just(safePage);
        }
        return postLoad(Flux.fromIterable(safePage.rows()))
                .collectList()
                .map(rows -> new PageResult<>(rows, safePage.total(), safePage.page(), safePage.size()));
    }

    private Mono<Long> write(T entity,
                             EntityLifecyclePhase prePhase,
                             EntityLifecyclePhase postPhase,
                             Supplier<Mono<R2dbcTransactionContext>> currentTransaction,
                             Function<Boolean, Mono<Long>> operation) {
        Function<Boolean, Mono<Long>> safeOperation = Objects.requireNonNull(
                operation, "repository write operation must not be null");
        Supplier<Mono<R2dbcTransactionContext>> safeTransaction = Objects.requireNonNull(
                currentTransaction, "repository transaction lookup must not be null");
        return fire(prePhase, entity, null).then(Mono.defer(() -> {
            if (!hasWork(postPhase)) {
                return requireOperation(safeOperation.apply(false));
            }
            return Objects.requireNonNull(safeTransaction.get(), "repository transaction lookup must return a Mono")
                    .map(Optional::of)
                    .defaultIfEmpty(Optional.empty())
                    .flatMap(transaction -> transaction.isPresent()
                            ? executeEnlisted(entity, postPhase, transaction.orElseThrow(), safeOperation)
                            : requireOperation(safeOperation.apply(false))
                                    .contextWrite(context -> context.put(
                                            R2dbcTransactionParticipant.class,
                                            R2dbcTransactionParticipant.none()))
                                    .flatMap(rows -> fire(postPhase, entity, rows).thenReturn(rows)));
        }));
    }

    private Mono<Long> executeEnlisted(T entity,
                                       EntityLifecyclePhase postPhase,
                                       R2dbcTransactionContext transaction,
                                       Function<Boolean, Mono<Long>> operation) {
        AtomicReference<Long> result = new AtomicReference<>();
        AtomicBoolean executed = new AtomicBoolean();
        AtomicBoolean notified = new AtomicBoolean();
        final boolean registered;
        try {
            registered = transaction.completion().register(outcome -> {
                if (!notified.compareAndSet(false, true)
                        || outcome != TransactionOutcome.COMMITTED
                        || !executed.get()) {
                    return Mono.empty();
                }
                return fire(postPhase, entity, result.get());
            });
        } catch (RuntimeException failure) {
            Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(failure);
            return preferred instanceof VirtualMachineError fatal ? Mono.error(fatal) : Mono.error(failure);
        }
        if (!registered) {
            return Mono.error(new IllegalStateException(
                    "external transaction completion is required for POST entity lifecycle"));
        }
        return requireOperation(operation.apply(true))
                .contextWrite(context -> R2dbcTransactionParticipant.bind(context, transaction))
                .doOnNext(rows -> {
                    result.set(rows);
                    executed.set(true);
                });
    }

    private static Mono<Long> requireOperation(Mono<Long> operation) {
        return Objects.requireNonNull(operation, "repository write operation must return a Mono");
    }
}
