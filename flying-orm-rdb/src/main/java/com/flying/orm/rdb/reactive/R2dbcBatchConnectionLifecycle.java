package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Objects;

/**
 * 批量事务共用的连接获取、取消清理、关闭和物理淘汰边界。
 *
 * <p>只在提交或回滚得到明确回执后正常关闭连接。BEGIN、COMMIT、ROLLBACK 任一阶段结果不确定时，
 * 连接都会被物理淘汰，绝不带着未知事务重新进入连接池。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchConnectionLifecycle {

    private final ConnectionFactory connectionFactory;
    private final SqlExecutionObserver cleanupObserver;
    private final R2dbcConnectionInvalidator connectionInvalidator;
    private final R2dbcTransactionParticipant transactionParticipant;

    R2dbcBatchConnectionLifecycle(ConnectionFactory connectionFactory,
                                  SqlExecutionObserver cleanupObserver,
                                  R2dbcConnectionInvalidator connectionInvalidator,
                                  R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connection factory must not be null");
        this.cleanupObserver = Objects.requireNonNull(cleanupObserver, "batch cleanup observer must not be null");
        this.connectionInvalidator = Objects.requireNonNull(connectionInvalidator,
                                                             "connection invalidator must not be null");
        this.transactionParticipant = Objects.requireNonNull(transactionParticipant,
                                                              "transaction participant must not be null");
    }

    Mono<R2dbcBatchConnectionHandle> acquire(BatchWriteOptions options) {
        return acquire(options, SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT);
    }

    Mono<R2dbcBatchConnectionHandle> acquire(BatchWriteOptions options, Duration cleanupTimeout) {
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        Duration safeCleanupTimeout = Objects.requireNonNull(cleanupTimeout,
                                                              "cleanup timeout must not be null");
        // 自定义事务参与者可能不是 Reactor Context 实现，所以不能假设入口校验和真正取连接时一定看到同一结果。
        // 在使用外部连接前再做一次同样的限制，避免 INDEPENDENT 或回执恢复误入上层事务。
        return currentTransaction()
                .flatMap(transaction -> validateExternalOptions(safeOptions)
                        .thenReturn(externalHandle(transaction, safeCleanupTimeout)))
                .switchIfEmpty(acquireOwned(safeCleanupTimeout));
    }

    /** INDEPENDENT 需要自行提交分片，回执重放也会在业务写入前额外取连接；两者都不能绕过外部事务。 */
    Mono<Void> validate(BatchWriteOptions options) {
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        return currentTransaction().flatMap(ignored -> validateExternalOptions(safeOptions)).then();
    }

    private Mono<Void> validateExternalOptions(BatchWriteOptions options) {
        if (options.mode() == BatchWriteOptions.Mode.INDEPENDENT) {
            return Mono.error(new R2dbcTransactionParticipationException(
                    R2dbcTransactionParticipationException.Reason.INDEPENDENT_BATCH_NOT_ALLOWED));
        }
        if (options.recovery().mode() == BatchWriteOptions.RecoveryMode.RECEIPT) {
            return Mono.error(new R2dbcTransactionParticipationException(
                    R2dbcTransactionParticipationException.Reason.RECEIPT_RECOVERY_NOT_ALLOWED));
        }
        return Mono.empty();
    }

    /** 每次订阅都用当前隔离上下文核对事务路由，事务期间改库会在连接和输入流被使用前失败。 */
    private Mono<R2dbcTransactionContext> currentTransaction() {
        return Mono.deferContextual(context -> Mono.defer(() -> Objects.requireNonNull(
                transactionParticipant.currentTransaction(IsolationContexts.currentDatabaseKey(context)),
                "current transaction publisher must not be null")));
    }

    Mono<Void> begin(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        return Mono.from(resource.connection().beginTransaction()).doOnSuccess(ignored -> resource.markActive());
    }

    Mono<Void> commit(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        resource.markCommitting();
        return Mono.from(resource.connection().commitTransaction()).doOnSuccess(ignored -> resource.markCommitted());
    }

    Mono<Void> rollback(R2dbcBatchConnectionHandle resource) {
        if (isExternal(resource)) {
            return Mono.empty();
        }
        // 回滚属于清理动作，不能因为驱动不再响应就让调用永久挂住。超时后状态保持 ACTIVE，
        // closeAfterOutcome 会把结果未确认的连接直接淘汰，避免它重新进入连接池。
        return resource.cleanupDeadline().protect(Mono.from(resource.connection().rollbackTransaction()))
                   .doOnSuccess(ignored -> resource.markRolledBack());
    }

    boolean isExternal(R2dbcBatchConnectionHandle resource) {
        return Objects.requireNonNull(resource, "batch connection must not be null").external();
    }

    private R2dbcBatchConnectionHandle externalHandle(R2dbcTransactionContext transaction,
                                                       Duration cleanupTimeout) {
        R2dbcTransactionContext safeTransaction = Objects.requireNonNull(transaction,
                                                                           "transaction context must not be null");
        return new R2dbcBatchConnectionHandle(safeTransaction, cleanupTimeout);
    }

    private Mono<R2dbcBatchConnectionHandle> acquireOwned(Duration cleanupTimeout) {
        // 外部事务命中时不能提前触发备用连接池，尤其不能让 eager Publisher 偷跑一次连接获取。
        Mono<Connection> connection = Mono.defer(() -> Mono.from(connectionFactory.create()));
        return connection.map(owned -> new R2dbcBatchConnectionHandle(owned, cleanupTimeout));
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
        if (isExternal(safeResource)) {
            return safeResource.largeObjects().cancel(safeResource.cleanupDeadline())
                    .then(releaseExternal(safeResource));
        }
        return safeResource.largeObjects().cancel(safeResource.cleanupDeadline())
                .thenReturn(true)
                .onErrorResume(error -> invalidateUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        error).thenReturn(false))
                .flatMap(clean -> clean ? Mono.defer(() -> {
            if (safeResource.state() == BatchTransactionState.COMMITTING) {
                return invalidateUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        new IllegalStateException(modeName + " batch cancelled while commit outcome is unknown"));
            }
            if (safeResource.state() != BatchTransactionState.ACTIVE) {
                return closeAfterOutcome(safeResource, safeOperation);
            }
            return safeResource.cleanupDeadline().protect(
                            Mono.from(safeResource.connection().rollbackTransaction()))
                       .doOnSuccess(ignored -> safeResource.markRolledBack())
                       .then(closeAfterOutcome(safeResource, safeOperation))
                       .onErrorResume(error -> invalidateUncertainConnection(
                               safeResource, safeOperation,
                               ResourceCleanupObservation.Phase.TRANSACTION_ROLLBACK, error));
        }) : Mono.empty());
    }

    Mono<Void> closeAfterOutcome(R2dbcBatchConnectionHandle resource) {
        return closeAfterOutcome(resource, SqlExecutionOperation.CHUNKED_BATCH_WRITE);
    }

    /** 关闭或淘汰连接，并把清理故障归类到实际 SQL 操作。 */
    Mono<Void> closeAfterOutcome(R2dbcBatchConnectionHandle resource, SqlExecutionOperation operation) {
        R2dbcBatchConnectionHandle safeResource = Objects.requireNonNull(resource,
                                                                         "batch connection must not be null");
        SqlExecutionOperation safeOperation = Objects.requireNonNull(
                operation, "cleanup SQL operation must not be null");
        if (isExternal(safeResource)) {
            return safeResource.largeObjects().complete(safeResource.cleanupDeadline())
                    .then(releaseExternal(safeResource));
        }
        // defer 很重要：取消清理中的 rollback 真正结束后，才能读取最终状态。
        return releaseLargeObjects(safeResource, safeOperation).flatMap(reusable -> {
            if (!reusable) {
                return Mono.empty();
            }
            return Mono.defer(() -> {
            BatchTransactionState state = safeResource.state();
            if (state != BatchTransactionState.COMMITTED && state != BatchTransactionState.ROLLED_BACK) {
                return invalidateUncertainConnection(
                        safeResource,
                        safeOperation,
                        ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        new IllegalStateException("batch connection outcome is not reusable: state=" + state));
            }
            return safeResource.cleanupDeadline().protect(
                            Mono.from(connectionInvalidator.close(safeResource.connection())))
                       .onErrorResume(closeError -> invalidateAfterCloseFailure(
                               safeResource, safeOperation, closeError));
            });
        });
    }

    /** LOB 清理失败时先淘汰连接；事务终态已确认时不得用普通清理错误覆盖结果。 */
    private Mono<Boolean> releaseLargeObjects(R2dbcBatchConnectionHandle resource,
                                              SqlExecutionOperation operation) {
        return resource.largeObjects().complete(resource.cleanupDeadline()).thenReturn(true).onErrorResume(error -> {
            BatchTransactionState state = resource.state();
            Mono<Void> invalidation = state == BatchTransactionState.COMMITTED
                    || state == BatchTransactionState.ROLLED_BACK
                    ? invalidateAfterCloseFailure(resource, operation, error)
                    : invalidateUncertainConnection(resource, operation,
                            ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE, error);
            return invalidation.thenReturn(false);
        });
    }

    private Mono<Void> invalidateUncertainConnection(R2dbcBatchConnectionHandle resource,
                                                      SqlExecutionOperation operation,
                                                      ResourceCleanupObservation.Phase phase,
                                                      Throwable cleanupError) {
        Mono<Void> invalidation = resource.cleanupDeadline().protectInvalidation(
                Mono.defer(() -> Mono.from(connectionInvalidator.invalidate(resource.connection()))));
        return finishInvalidation(invalidation, operation, phase, false, cleanupError);
    }

    private Mono<Void> invalidateAfterCloseFailure(R2dbcBatchConnectionHandle resource,
                                                   SqlExecutionOperation operation,
                                                   Throwable closeError) {
        Mono<Void> invalidation = resource.cleanupDeadline().protectInvalidation(
                Mono.defer(() -> Mono.from(connectionInvalidator.invalidate(resource.connection()))));
        boolean outcomeConfirmed = resource.state() == BatchTransactionState.COMMITTED
                || resource.state() == BatchTransactionState.ROLLED_BACK;
        return finishInvalidation(invalidation,
                                  operation,
                                  ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                                  outcomeConfirmed,
                                  closeError);
    }

    private Mono<Void> finishInvalidation(Mono<Void> invalidation,
                                          SqlExecutionOperation operation,
                                          ResourceCleanupObservation.Phase phase,
                                          boolean outcomeConfirmed,
                                          Throwable primaryError) {
        return invalidation.onErrorResume(invalidationError -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                    primaryError, invalidationError);
            if (fatal != null) {
                observeCleanup(operation, phase, outcomeConfirmed, fatal);
                return Mono.error(fatal);
            }
            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primaryError, invalidationError);
            return Mono.empty();
        }).then(Mono.defer(() -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(primaryError);
            if (fatal != null) {
                observeCleanup(operation, phase, outcomeConfirmed, fatal);
                return Mono.error(fatal);
            }
            observeCleanup(operation, phase, outcomeConfirmed, primaryError);
            return Mono.empty();
        }));
    }

    private void observeCleanup(SqlExecutionOperation operation,
                                ResourceCleanupObservation.Phase phase,
                                boolean outcomeConfirmed,
                                Throwable error) {
        cleanupObserver.onResourceCleanup(new ResourceCleanupObservation(
                operation, phase, outcomeConfirmed, error));
    }

    /** 外部连接只解除 ORM 的临时引用，提交、回滚和关闭仍由上层事务管理器负责。 */
    private Mono<Void> releaseExternal(R2dbcBatchConnectionHandle resource) {
        Objects.requireNonNull(resource, "batch connection must not be null");
        return Mono.empty();
    }
}
