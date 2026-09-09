package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/** Releases LOB locators and logical connection leases without owning time budgets. */
final class R2dbcConnectionLeaseCleanup {
    private final SqlExecutionObserver observer;

    R2dbcConnectionLeaseCleanup(SqlExecutionObserver observer) {
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
    }

    Mono<Void> closeAfterResult(R2dbcConnectionLease lease,
                                SqlExecutionOperation operation,
                                boolean outcomeConfirmed) {
        R2dbcLargeObjectScope scope = lease.largeObjectsIfCreated();
        if (lease.external()) {
            return scope == null ? Mono.empty() : scope.complete();
        }
        if (scope == null) {
            return outcomeConfirmed ? closeReusableConnection(lease, operation)
                                    : closeOwnedConnection(lease, operation);
        }
        return completeLargeObjects(lease, error -> closeAfterCleanupFailure(
                lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                outcomeConfirmed, error))
                .flatMap(cleaned -> cleaned
                        ? outcomeConfirmed ? closeReusableConnection(lease, operation)
                                           : closeOwnedConnection(lease, operation)
                        : Mono.empty());
    }

    Mono<Void> closeConfirmedOwnedAfterResult(R2dbcConnectionLease lease,
                                              SqlExecutionOperation operation,
                                              Mono<Void> prepareForReturn) {
        if (lease.largeObjectsIfCreated() == null) {
            return prepareAndCloseReusable(lease, operation, prepareForReturn);
        }
        return completeLargeObjects(lease, error -> prepareThenCloseAfterFailure(
                lease, operation, error, prepareForReturn))
                .flatMap(cleaned -> cleaned
                        ? prepareAndCloseReusable(lease, operation, prepareForReturn)
                        : Mono.empty());
    }

    private Mono<Boolean> completeLargeObjects(R2dbcConnectionLease lease,
                                               Function<Throwable, Mono<Void>> closeAfterFailure) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        if (largeObjects == null) {
            return Mono.just(true);
        }
        return largeObjects.complete().thenReturn(true)
                .onErrorResume(error -> closeAfterFailure.apply(error).thenReturn(false))
                .flatMap(cleaned -> {
                    if (!cleaned) {
                        return Mono.just(false);
                    }
                    Throwable cleanupFailure = largeObjects.cleanupFailure();
                    return cleanupFailure == null
                            ? Mono.just(true)
                            : closeAfterFailure.apply(cleanupFailure).thenReturn(false);
                });
    }

    Mono<Void> cancelAfterResult(R2dbcConnectionLease lease, SqlExecutionOperation operation) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjects();
        if (lease.external()) {
            return largeObjects.cancel();
        }
        return largeObjects.cancel().thenReturn(true)
                .onErrorResume(error -> closeAfterCleanupFailure(
                        lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        false, error).thenReturn(false))
                .flatMap(cleaned -> {
                    if (!cleaned) {
                        return Mono.empty();
                    }
                    return operation == SqlExecutionOperation.QUERY
                            ? closeCancelledQuery(lease, operation) : closeOwnedConnection(lease, operation);
                });
    }

    Mono<Void> closeAfterError(R2dbcConnectionLease lease,
                               SqlExecutionOperation operation,
                               Throwable error) {
        R2dbcLargeObjectScope scope = lease.largeObjectsIfCreated();
        Mono<Void> cleanup = scope == null ? Mono.empty() : scope.error(error)
                .onErrorResume(cleanupError -> {
                    VirtualMachineError fatal = promoteVirtualMachineError(error, cleanupError);
                    if (fatal == null) {
                        return Mono.empty();
                    }
                    return closeAfterCleanupFailure(
                            lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                            false, fatal).then(Mono.error(fatal));
                });
        return cleanup.then(Mono.defer(() -> {
            if (lease.external()) {
                return Mono.empty();
            }
            Throwable cleanupFailure = scope == null ? null : scope.cleanupFailure();
            return cleanupFailure == null
                    ? closeOwnedConnection(lease, operation)
                    : closeAfterCleanupFailure(
                            lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                            false, cleanupFailure);
        }));
    }

    Mono<Void> closeAfterCleanupFailure(R2dbcConnectionLease lease,
                                        SqlExecutionOperation operation,
                                        ResourceCleanupObservation.Phase phase,
                                        boolean outcomeConfirmed,
                                        Throwable primaryError) {
        if (lease.external()) {
            return Mono.empty();
        }
        return Mono.defer(() -> Mono.from(lease.connection().close()))
                .onErrorResume(closeError -> {
                    VirtualMachineError fatal = promoteVirtualMachineError(primaryError, closeError);
                    if (fatal != null) {
                        observeCleanup(operation, phase, outcomeConfirmed, fatal);
                        return Mono.error(fatal);
                    }
                    addSuppressedIfAcyclic(primaryError, closeError);
                    return Mono.empty();
                }).then(observeCloseFailure(operation, phase, outcomeConfirmed, primaryError));
    }

    private Mono<Void> observeCloseFailure(SqlExecutionOperation operation,
                                           ResourceCleanupObservation.Phase phase,
                                           boolean outcomeConfirmed,
                                           Throwable primaryError) {
        return Mono.defer(() -> {
            VirtualMachineError fatal = findVirtualMachineError(primaryError);
            Throwable observationError = fatal == null ? primaryError : fatal;
            observeCleanup(operation, phase, outcomeConfirmed, observationError);
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        });
    }

    private Mono<Void> closeOwnedConnection(R2dbcConnectionLease lease,
                                            SqlExecutionOperation operation) {
        return Mono.defer(() -> Mono.from(lease.connection().close()))
                .onErrorResume(error -> {
                    observeCleanup(operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, false, error);
                    VirtualMachineError fatal = findVirtualMachineError(error);
                    return fatal == null ? Mono.empty() : Mono.error(fatal);
                });
    }

    private Mono<Void> closeCancelledQuery(R2dbcConnectionLease lease,
                                           SqlExecutionOperation operation) {
        return Mono.defer(() -> Mono.from(lease.connection().close()))
                .onErrorResume(error -> observeCloseFailure(
                        operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, false, error));
    }

    /** 关闭观测时不创建事件及其脱敏异常；fatal 判断和连接清理仍由原调用链完成。 */
    private void observeCleanup(SqlExecutionOperation operation,
                                ResourceCleanupObservation.Phase phase,
                                boolean outcomeConfirmed,
                                Throwable error) {
        if (observer.enabled()) {
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, phase, outcomeConfirmed, error));
        }
    }

    private Mono<Void> prepareAndCloseReusable(R2dbcConnectionLease lease,
                                               SqlExecutionOperation operation,
                                               Mono<Void> prepareForReturn) {
        return prepareForReturn.thenReturn(true)
                .onErrorResume(error -> closeAfterCleanupFailure(
                        lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true, error).thenReturn(false))
                .flatMap(prepared -> prepared ? closeReusableConnection(lease, operation) : Mono.empty());
    }

    private Mono<Void> prepareThenCloseAfterFailure(R2dbcConnectionLease lease,
                                                    SqlExecutionOperation operation,
                                                    Throwable primaryError,
                                                    Mono<Void> prepareForReturn) {
        return prepareForReturn.thenReturn(primaryError)
                .onErrorResume(prepareError -> Mono.just(mergeCleanupFailures(primaryError, prepareError)))
                .flatMap(error -> closeAfterCleanupFailure(
                        lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, true, error));
    }

    private static Throwable mergeCleanupFailures(Throwable primaryError,
                                                   Throwable secondaryError) {
        VirtualMachineError fatal = promoteVirtualMachineError(primaryError, secondaryError);
        if (fatal != null) {
            return fatal;
        }
        addSuppressedIfAcyclic(primaryError, secondaryError);
        return primaryError;
    }

    /**
     * 普通成功 SQL 的逻辑归还直接交给驱动或连接池。ORM 不对这个外部资源边界再叠加超时，
     * 只在归还入口已经失败时记录清理事实。
     */
    private Mono<Void> closeReusableConnection(R2dbcConnectionLease lease,
                                               SqlExecutionOperation operation) {
        return Mono.defer(() -> Mono.from(lease.connection().close()))
                .onErrorResume(closeError -> observeCloseFailure(
                        operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true, closeError));
    }
}
