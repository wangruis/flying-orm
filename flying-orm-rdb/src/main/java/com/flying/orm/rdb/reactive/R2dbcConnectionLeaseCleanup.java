package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 只负责 R2DBC 连接租约结束后的 LOB 清理、归还和失效，不参与 SQL 执行或事务控制。
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class R2dbcConnectionLeaseCleanup {

    private final SqlExecutionObserver observer;
    private final R2dbcConnectionInvalidator connectionInvalidator;

    R2dbcConnectionLeaseCleanup(SqlExecutionObserver observer,
                                R2dbcConnectionInvalidator connectionInvalidator) {
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.connectionInvalidator = Objects.requireNonNull(
                connectionInvalidator, "connection invalidator must not be null");
    }

    Mono<Void> closeAfterResult(R2dbcExecutionSession.ConnectionLease lease,
                                SqlExecutionOperation operation,
                                SqlExecutionOptions options,
                                boolean outcomeConfirmed) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        return closeAfterResult(safeLease, operation, options, outcomeConfirmed,
                                safeLease.largeObjects().cleanupDeadline(options.cleanupTimeout()));
    }

    Mono<Void> closeAfterResult(R2dbcExecutionSession.ConnectionLease lease,
                                SqlExecutionOperation operation,
                                SqlExecutionOptions options,
                                boolean outcomeConfirmed,
                                R2dbcCleanupDeadline deadline) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        R2dbcCleanupDeadline sharedDeadline = safeLease.largeObjects().shareCleanupDeadline(deadline);
        if (safeLease.external()) {
            return safeLease.largeObjects().complete(sharedDeadline);
        }
        Connection connection = safeLease.connection();
        Mono<Void> close = sharedDeadline.protect(Mono.defer(() -> Mono.from(
                connectionInvalidator.close(connection))));
        Mono<Boolean> largeObjects = safeLease.largeObjects().complete(sharedDeadline).thenReturn(true)
                .onErrorResume(error -> invalidateAfterCleanupFailure(
                        safeLease, operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        outcomeConfirmed, error, sharedDeadline).thenReturn(false));
        if (!outcomeConfirmed) {
            return largeObjects.flatMap(reusable -> reusable
                    ? invalidateOwnedConnection(safeLease, operation, sharedDeadline)
                    : Mono.empty());
        }
        return largeObjects.flatMap(reusable -> reusable
                ? close.onErrorResume(error -> invalidateAfterCleanupFailure(
                        safeLease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        true, error, sharedDeadline))
                : Mono.empty());
    }

    /** 查询取消后归还连接池；写入或 DDL 取消的结果不确定，仍然隔离自有连接。 */
    Mono<Void> cancelAfterResult(R2dbcExecutionSession.ConnectionLease lease,
                                 SqlExecutionOperation operation,
                                 SqlExecutionOptions options) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        R2dbcCleanupDeadline deadline = safeLease.largeObjects().cleanupDeadline(options.cleanupTimeout());
        if (safeLease.external()) {
            return safeLease.largeObjects().cancel(deadline);
        }
        Mono<Boolean> largeObjects = safeLease.largeObjects().cancel(deadline).thenReturn(true)
                .onErrorResume(error -> invalidateAfterCleanupFailure(
                        safeLease, operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        false, error, deadline).thenReturn(false));
        return largeObjects.flatMap(cleaned -> {
            if (!cleaned) {
                return Mono.empty();
            }
            if (operation == SqlExecutionOperation.QUERY) {
                return closeCancelledQuery(safeLease, operation, deadline);
            }
            return invalidateOwnedConnection(safeLease, operation, deadline);
        });
    }

    Mono<Void> closeAfterError(R2dbcExecutionSession.ConnectionLease lease,
                               SqlExecutionOperation operation,
                               SqlExecutionOptions options,
                               Throwable error) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        R2dbcCleanupDeadline deadline = safeLease.largeObjects().cleanupDeadline(options.cleanupTimeout());
        Mono<Void> largeObjectCleanup = safeLease.largeObjects().error(error, deadline)
                .onErrorResume(cleanupError -> {
                    VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                            error, cleanupError);
                    if (fatal == null) {
                        return Mono.empty();
                    }
                    return invalidateAfterCleanupFailure(
                            safeLease, operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                            false, fatal, deadline).then(Mono.error(fatal));
                });
        return largeObjectCleanup.then(Mono.defer(() -> {
            if (safeLease.external()) {
                return Mono.empty();
            }
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            boolean reusable = translated instanceof RdbException rdbException
                    && reusableAfterError(rdbException.kind());
            return closeAfterResult(safeLease, operation, options, reusable, deadline);
        }));
    }

    Mono<Void> invalidateAfterCleanupFailure(R2dbcExecutionSession.ConnectionLease lease,
                                             SqlExecutionOperation operation,
                                             ResourceCleanupObservation.Phase phase,
                                             SqlExecutionOptions options,
                                             boolean outcomeConfirmed,
                                             Throwable primaryError) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        return invalidateAfterCleanupFailure(
                safeLease, operation, phase, outcomeConfirmed, primaryError,
                safeLease.largeObjects().cleanupDeadline(options.cleanupTimeout()));
    }

    Mono<Void> invalidateAfterCleanupFailure(R2dbcExecutionSession.ConnectionLease lease,
                                             SqlExecutionOperation operation,
                                             ResourceCleanupObservation.Phase phase,
                                             SqlExecutionOptions options,
                                             boolean outcomeConfirmed,
                                             Throwable primaryError,
                                             R2dbcCleanupDeadline deadline) {
        Objects.requireNonNull(options, "sql execution options must not be null");
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        return invalidateAfterCleanupFailure(
                safeLease, operation, phase, outcomeConfirmed, primaryError,
                safeLease.largeObjects().shareCleanupDeadline(deadline));
    }

    private Mono<Void> invalidateAfterCleanupFailure(R2dbcExecutionSession.ConnectionLease lease,
                                                      SqlExecutionOperation operation,
                                                      ResourceCleanupObservation.Phase phase,
                                                      boolean outcomeConfirmed,
                                                      Throwable primaryError,
                                                      R2dbcCleanupDeadline deadline) {
        R2dbcExecutionSession.ConnectionLease safeLease = requireLease(lease);
        if (safeLease.external()) {
            return Mono.empty();
        }
        Mono<Void> invalidation = deadline.protectInvalidation(Mono.defer(() ->
                Mono.from(connectionInvalidator.invalidate(safeLease.connection()))));
        return invalidation.onErrorResume(invalidationError -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                    primaryError, invalidationError);
            if (fatal != null) {
                observer.onResourceCleanup(new ResourceCleanupObservation(
                        operation, phase, outcomeConfirmed, fatal));
                return Mono.error(fatal);
            }
            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primaryError, invalidationError);
            return Mono.empty();
        }).then(Mono.defer(() -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(primaryError);
            Throwable observationError = fatal == null ? primaryError : fatal;
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, phase, outcomeConfirmed, observationError));
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        }));
    }

    private Mono<Void> invalidateOwnedConnection(R2dbcExecutionSession.ConnectionLease lease,
                                                 SqlExecutionOperation operation,
                                                 R2dbcCleanupDeadline deadline) {
        Mono<Void> invalidation = deadline.protectInvalidation(Mono.defer(() ->
                Mono.from(connectionInvalidator.invalidate(lease.connection()))));
        return invalidation.onErrorResume(error -> {
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE, false, error));
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(error);
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        });
    }

    private Mono<Void> closeCancelledQuery(R2dbcExecutionSession.ConnectionLease lease,
                                           SqlExecutionOperation operation,
                                           R2dbcCleanupDeadline deadline) {
        Mono<Void> close = deadline.protect(Mono.defer(() -> Mono.from(
                connectionInvalidator.close(lease.connection()))));
        return close.onErrorResume(error -> invalidateAfterCleanupFailure(
                lease, operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                false, error, deadline));
    }

    private static boolean reusableAfterError(RdbErrorKind kind) {
        return switch (kind) {
            case DUPLICATE_KEY, CONSTRAINT, BAD_SQL, DEADLOCK, LOCK_TIMEOUT -> true;
            case CONNECTION, TIMEOUT, CANCELLED, UNKNOWN -> false;
        };
    }

    private static R2dbcExecutionSession.ConnectionLease requireLease(
            R2dbcExecutionSession.ConnectionLease lease) {
        return Objects.requireNonNull(lease, "connection lease must not be null");
    }
}
