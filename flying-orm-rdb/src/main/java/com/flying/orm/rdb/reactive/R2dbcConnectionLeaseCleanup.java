package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * 只负责 R2DBC 连接租约结束后的 LOB 清理和连接归还，不参与 SQL 执行或事务控制。
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class R2dbcConnectionLeaseCleanup {

    private final SqlExecutionObserver observer;

    R2dbcConnectionLeaseCleanup(SqlExecutionObserver observer) {
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
    }

    Mono<Void> closeAfterResult(R2dbcConnectionLease lease,
                                SqlExecutionOperation operation,
                                SqlExecutionOptions options,
                                boolean outcomeConfirmed) {
        return closeAfterResultWithTimeout(lease, operation, outcomeConfirmed, options.cleanupTimeout());
    }

    Mono<Void> closeAfterResultWithTimeout(R2dbcConnectionLease lease,
                                           SqlExecutionOperation operation,
                                           boolean outcomeConfirmed,
                                           Duration cleanupTimeout) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        if (lease.external() && largeObjects == null) {
            return Mono.empty();
        }
        if (!lease.external() && largeObjects == null && outcomeConfirmed) {
            return closeReusableConnection(lease, operation);
        }
        return closeAfterResult(lease, operation, outcomeConfirmed,
                                cleanupDeadline(lease, cleanupTimeout));
    }

    /**
     * 已确认终态的自有连接按 LOB、调用方状态恢复、连接归还的顺序清理。
     */
    Mono<Void> closeConfirmedOwnedAfterResult(R2dbcConnectionLease lease,
                                              SqlExecutionOperation operation,
                                              Duration cleanupTimeout,
                                              Mono<Void> prepareForReturn) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        if (largeObjects == null) {
            return prepareAndCloseReusable(
                    lease, operation, cleanupTimeout, prepareForReturn);
        }
        R2dbcCleanupDeadline deadline = cleanupDeadline(lease, cleanupTimeout);
        Mono<Boolean> cleanup = completeLargeObjects(
                lease,
                deadline,
                error -> prepareThenCloseAfterFailure(
                        lease, operation, error, deadline, prepareForReturn));
        return cleanup.flatMap(reusable -> reusable
                ? prepareAndCloseReusable(lease, operation, cleanupTimeout, prepareForReturn)
                : Mono.empty());
    }

    Mono<Void> closeAfterResult(R2dbcConnectionLease lease,
                                SqlExecutionOperation operation,
                                boolean outcomeConfirmed,
                                R2dbcCleanupDeadline deadline) {
        return closeResultWithDeadline(lease, operation, outcomeConfirmed,
                                       shareCleanupDeadline(lease, deadline));
    }

    private Mono<Void> closeResultWithDeadline(R2dbcConnectionLease lease,
                                               SqlExecutionOperation operation,
                                               boolean outcomeConfirmed,
                                               R2dbcCleanupDeadline deadline) {
        R2dbcLargeObjectScope largeObjectScope = lease.largeObjectsIfCreated();
        if (lease.external()) {
            return largeObjectScope == null ? Mono.empty() : largeObjectScope.complete(deadline);
        }
        Mono<Boolean> largeObjects = completeLargeObjects(
                lease,
                deadline,
                error -> closeAfterCleanupFailure(
                        lease,
                        operation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        outcomeConfirmed,
                        error,
                        deadline));
        if (!outcomeConfirmed) {
            return largeObjects.flatMap(reusable -> reusable
                    ? closeOwnedConnection(lease, operation, deadline)
                    : Mono.empty());
        }
        return largeObjects.flatMap(reusable -> reusable
                ? closeReusableConnection(lease, operation)
                : Mono.empty());
    }

    private Mono<Boolean> completeLargeObjects(
            R2dbcConnectionLease lease,
            R2dbcCleanupDeadline deadline,
            Function<Throwable, Mono<Void>> closeAfterFailure) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        if (largeObjects == null) {
            return Mono.just(true);
        }
        return largeObjects.complete(deadline).thenReturn(true)
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

    /** 查询取消后归还自有逻辑连接；写入或 DDL 取消的结果不确定，仍然保留该事实。 */
    Mono<Void> cancelAfterResult(R2dbcConnectionLease lease,
                                 SqlExecutionOperation operation,
                                 SqlExecutionOptions options) {
        R2dbcLargeObjectScope largeObjectScope = lease.largeObjects();
        R2dbcCleanupDeadline deadline = largeObjectScope.cleanupDeadline(options.cleanupTimeout());
        if (lease.external()) {
            return largeObjectScope.cancel(deadline);
        }
        Mono<Boolean> largeObjects = largeObjectScope.cancel(deadline).thenReturn(true)
                .onErrorResume(error -> closeAfterCleanupFailure(
                        lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        false, error, deadline).thenReturn(false));
        return largeObjects.flatMap(cleaned -> {
            if (!cleaned) {
                return Mono.empty();
            }
            if (operation == SqlExecutionOperation.QUERY) {
                return closeCancelledQuery(lease, operation, deadline);
            }
            return closeOwnedConnection(lease, operation, deadline);
        });
    }

    Mono<Void> closeAfterError(R2dbcConnectionLease lease,
                               SqlExecutionOperation operation,
                               SqlExecutionOptions options,
                               Throwable error) {
        R2dbcCleanupDeadline deadline = cleanupDeadline(lease, options.cleanupTimeout());
        R2dbcLargeObjectScope largeObjectScope = lease.largeObjectsIfCreated();
        Mono<Void> largeObjectCleanup = largeObjectScope == null
                ? Mono.empty()
                : largeObjectScope.error(error, deadline)
                        .onErrorResume(cleanupError -> {
                            VirtualMachineError fatal = promoteVirtualMachineError(error, cleanupError);
                            if (fatal == null) {
                                return Mono.empty();
                            }
                            return closeAfterCleanupFailure(
                                    lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                                    false, fatal, deadline).then(Mono.error(fatal));
                        });
        return largeObjectCleanup.then(Mono.defer(() -> {
            if (lease.external()) {
                return Mono.empty();
            }
            Throwable cleanupFailure = largeObjectScope == null ? null : largeObjectScope.cleanupFailure();
            if (cleanupFailure != null) {
                return closeAfterCleanupFailure(
                        lease,
                        operation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        false,
                        cleanupFailure,
                        deadline);
            }
            return closeOwnedConnection(lease, operation, deadline);
        }));
    }

    Mono<Void> closeAfterCleanupFailure(R2dbcConnectionLease lease,
                                        SqlExecutionOperation operation,
                                        ResourceCleanupObservation.Phase phase,
                                        boolean outcomeConfirmed,
                                        Throwable primaryError,
                                        R2dbcCleanupDeadline deadline) {
        return closeCleanupFailureWithDeadline(
                lease, operation, phase, outcomeConfirmed, primaryError,
                shareCleanupDeadline(lease, deadline));
    }

    private Mono<Void> closeCleanupFailureWithDeadline(R2dbcConnectionLease lease,
                                                       SqlExecutionOperation operation,
                                                       ResourceCleanupObservation.Phase phase,
                                                       boolean outcomeConfirmed,
                                                       Throwable primaryError,
                                                       R2dbcCleanupDeadline deadline) {
        if (lease.external()) {
            return Mono.empty();
        }
        Mono<Void> closePublisher = Mono.defer(() -> Mono.from(lease.connection().close()));
        Mono<Void> closeAttempt = outcomeConfirmed ? closePublisher : deadline.protect(closePublisher);
        return closeAttempt.onErrorResume(closeError -> {
            VirtualMachineError fatal = promoteVirtualMachineError(primaryError, closeError);
            if (fatal != null) {
                observer.onResourceCleanup(new ResourceCleanupObservation(
                        operation, phase, outcomeConfirmed, fatal));
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
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, phase, outcomeConfirmed, observationError));
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        });
    }

    private Mono<Void> closeOwnedConnection(R2dbcConnectionLease lease,
                                            SqlExecutionOperation operation,
                                            R2dbcCleanupDeadline deadline) {
        Mono<Void> closeAttempt = deadline.protect(Mono.defer(() ->
                Mono.from(lease.connection().close())));
        return closeAttempt.onErrorResume(error -> {
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, false, error));
            VirtualMachineError fatal = findVirtualMachineError(error);
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        });
    }

    private Mono<Void> closeCancelledQuery(R2dbcConnectionLease lease,
                                           SqlExecutionOperation operation,
                                           R2dbcCleanupDeadline deadline) {
        Mono<Void> close = deadline.protect(Mono.defer(() -> Mono.from(lease.connection().close())));
        return close.onErrorResume(error -> observeCloseFailure(
                operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE, false, error));
    }

    private Mono<Void> prepareAndCloseReusable(R2dbcConnectionLease lease,
                                               SqlExecutionOperation operation,
                                               Duration cleanupTimeout,
                                               Mono<Void> prepareForReturn) {
        return prepareForReturn.thenReturn(true)
                .onErrorResume(error -> closeAfterCleanupFailure(
                        lease,
                        operation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true,
                        error,
                        cleanupDeadline(lease, cleanupTimeout)).thenReturn(false))
                .flatMap(prepared -> prepared
                        ? closeReusableConnection(lease, operation)
                        : Mono.empty());
    }

    private Mono<Void> prepareThenCloseAfterFailure(R2dbcConnectionLease lease,
                                                    SqlExecutionOperation operation,
                                                    Throwable primaryError,
                                                    R2dbcCleanupDeadline deadline,
                                                    Mono<Void> prepareForReturn) {
        return prepareForReturn.thenReturn(primaryError)
                .onErrorResume(prepareError -> Mono.just(
                        mergeCleanupFailures(primaryError, prepareError)))
                .flatMap(error -> closeAfterCleanupFailure(
                        lease,
                        operation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true,
                        error,
                        deadline));
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

    private static R2dbcCleanupDeadline cleanupDeadline(
            R2dbcConnectionLease lease,
            Duration timeout) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        return largeObjects == null
                ? R2dbcCleanupDeadline.start(timeout)
                : largeObjects.cleanupDeadline(timeout);
    }

    private static R2dbcCleanupDeadline shareCleanupDeadline(
            R2dbcConnectionLease lease,
            R2dbcCleanupDeadline deadline) {
        R2dbcLargeObjectScope largeObjects = lease.largeObjectsIfCreated();
        return largeObjects == null ? deadline : largeObjects.shareCleanupDeadline(deadline);
    }
}
