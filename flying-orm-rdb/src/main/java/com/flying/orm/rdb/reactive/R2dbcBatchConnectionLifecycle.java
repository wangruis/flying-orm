package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 批量事务共用的连接获取、取消清理和归还边界。
 *
 * <p>只在提交或回滚得到明确回执后正常关闭连接。BEGIN、COMMIT、ROLLBACK 任一阶段结果不确定时，
 * ORM 仍会结束自己的连接租约，但是否复用或物理断开由驱动和连接池决定。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchConnectionLifecycle {

    private final ConnectionFactory connectionFactory;
    private final R2dbcConnectionLeaseCleanup leaseCleanup;
    private final R2dbcTransactionParticipant transactionParticipant;

    R2dbcBatchConnectionLifecycle(ConnectionFactory connectionFactory,
                                  SqlExecutionObserver cleanupObserver,
                                  R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connection factory must not be null");
        this.leaseCleanup = new R2dbcConnectionLeaseCleanup(Objects.requireNonNull(
                cleanupObserver, "batch cleanup observer must not be null"));
        this.transactionParticipant = Objects.requireNonNull(transactionParticipant,
                                                              "transaction participant must not be null");
    }

    Mono<R2dbcBatchConnectionHandle> acquireExternal() {
        return currentTransaction()
                .map(this::externalHandle)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "protected writes require an external transaction")));
    }

    Mono<R2dbcBatchConnectionHandle> acquire(
            BatchWriteOptions options,
            ReactiveTransactionSourceResolver.Resolution resolution) {
        if (resolution.transaction() != null) {
            return Mono.just(externalHandle(resolution.transaction()));
        }
        if (options.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            return Mono.error(new IllegalStateException(
                    "non-empty ATOMIC batches require an external transaction"));
        }
        return acquireOwned();
    }

    Mono<Void> validate(BatchWriteOptions options,
                        ReactiveTransactionSourceResolver.Resolution resolution) {
        return resolution.transaction() == null ? Mono.empty() : validateExternalOptions(options);
    }

    Mono<ReactiveTransactionSourceResolver.Resolution> resolveTransaction() {
        return currentTransaction()
                .map(transaction -> new ReactiveTransactionSourceResolver.Resolution(
                        SqlTransactionSource.EXTERNAL, transaction))
                .defaultIfEmpty(new ReactiveTransactionSourceResolver.Resolution(
                        SqlTransactionSource.INTERNAL, null));
    }

    private Mono<Void> validateExternalOptions(BatchWriteOptions options) {
        if (options.mode() == BatchWriteOptions.Mode.INDEPENDENT) {
            return Mono.error(new R2dbcTransactionParticipationException(
                    R2dbcTransactionParticipationException.Reason.INDEPENDENT_BATCH_NOT_ALLOWED));
        }
        return Mono.empty();
    }

    /** 每次订阅都向上层参与者读取当前事务；数据源选择由上层在绑定连接前完成。 */
    private Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.defer(() -> Objects.requireNonNull(
                transactionParticipant.currentTransaction(),
                "current transaction publisher must not be null"));
    }

    Mono<Void> begin(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            resource.rememberAutoCommit(resource.connection().isAutoCommit());
            return Mono.from(resource.connection().beginTransaction())
                    .doOnSuccess(ignored -> resource.markActive());
        });
    }

    Mono<Void> commit(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            resource.markCommitting();
            return Mono.from(resource.connection().commitTransaction())
                    .doOnSuccess(ignored -> resource.markCommitted());
        });
    }

    Mono<Void> rollback(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        return Mono.defer(() -> {
            resource.markRollingBack();
            return Mono.from(resource.connection().rollbackTransaction())
                    .doOnSuccess(ignored -> resource.markRolledBack());
        });
    }

    boolean isExternal(R2dbcBatchConnectionHandle resource) {
        return Objects.requireNonNull(resource, "batch connection must not be null").external();
    }

    private R2dbcBatchConnectionHandle externalHandle(R2dbcTransactionContext transaction) {
        return new R2dbcBatchConnectionHandle(transaction);
    }

    private Mono<R2dbcBatchConnectionHandle> acquireOwned() {
        Mono<Connection> connection = Mono.defer(() -> Mono.from(connectionFactory.create()));
        return connection.map(R2dbcBatchConnectionHandle::new);
    }

    Mono<Void> cancel(R2dbcBatchConnectionHandle resource, String modeName) {
        return cancel(resource, modeName, SqlExecutionOperation.CHUNKED_BATCH_WRITE);
    }

    /** 复用事务清理状态机，同时保留实际调用入口的观测操作类型。 */
    Mono<Void> cancel(R2dbcBatchConnectionHandle resource,
                      String modeName,
                      SqlExecutionOperation operation) {
        R2dbcBatchConnectionHandle safeResource = Objects.requireNonNull(resource,
                                                                         "batch connection must not be null");
        SqlExecutionOperation safeOperation = Objects.requireNonNull(
                operation, "cleanup SQL operation must not be null");
        R2dbcLargeObjectScope largeObjects = safeResource.largeObjects();
        if (isExternal(safeResource)) {
            return largeObjects.cancel();
        }
        Mono<Boolean> largeObjectCleanup = largeObjects.cancel()
                .thenReturn(true)
                .onErrorResume(error -> closeUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        error).thenReturn(false));
        return largeObjectCleanup.flatMap(clean -> clean ? Mono.defer(() -> {
            if (safeResource.state() == BatchTransactionState.COMMITTING) {
                return closeUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        new IllegalStateException(modeName + " batch cancelled while commit outcome is unknown"));
            }
            if (safeResource.state() != BatchTransactionState.ACTIVE) {
                return closeAfterOutcome(safeResource, safeOperation);
            }
            return rollback(safeResource)
                       .then(closeAfterOutcome(safeResource, safeOperation))
                       .onErrorResume(error -> closeUncertainConnection(
                               safeResource, safeOperation,
                               ResourceCleanupObservation.Phase.TRANSACTION_ROLLBACK, error));
        }) : Mono.empty());
    }

    Mono<Void> closeAfterOutcome(R2dbcBatchConnectionHandle resource) {
        return closeAfterOutcome(resource, SqlExecutionOperation.CHUNKED_BATCH_WRITE);
    }

    /** 归还自有连接，并把清理故障归类到实际 SQL 操作。 */
    Mono<Void> closeAfterOutcome(R2dbcBatchConnectionHandle resource, SqlExecutionOperation operation) {
        R2dbcBatchConnectionHandle safeResource = Objects.requireNonNull(resource,
                                                                         "batch connection must not be null");
        SqlExecutionOperation safeOperation = Objects.requireNonNull(
                operation, "cleanup SQL operation must not be null");
        if (isExternal(safeResource)) {
            if (safeResource.largeObjectsIfCreated() == null) {
                return Mono.empty();
            }
            return leaseCleanup.closeAfterResult(
                    safeResource, safeOperation, true);
        }
        // defer 很重要：取消清理中的 rollback 真正结束后，才能读取最终状态。
        return Mono.defer(() -> {
            BatchTransactionState state = safeResource.state();
            if (state != BatchTransactionState.COMMITTED && state != BatchTransactionState.ROLLED_BACK) {
                return closeUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        new IllegalStateException("batch transaction outcome is not confirmed: state=" + state));
            }
            return leaseCleanup.closeConfirmedOwnedAfterResult(
                    safeResource,
                    safeOperation,
                    restoreAutoCommit(safeResource));
        });
    }

    /** Restore only the auto-commit state changed by the local chunk transaction. */
    private Mono<Void> restoreAutoCommit(R2dbcBatchConnectionHandle resource) {
        return Mono.defer(() -> resource.requiresAutoCommitRestore()
                ? Mono.from(resource.connection().setAutoCommit(true))
                : Mono.empty());
    }

    private Mono<Void> closeUncertainConnection(R2dbcBatchConnectionHandle resource,
                                                 SqlExecutionOperation operation,
                                                 ResourceCleanupObservation.Phase phase,
                                                 Throwable cleanupError) {
        return leaseCleanup.closeAfterCleanupFailure(
                resource, operation, phase, false, cleanupError);
    }
}
