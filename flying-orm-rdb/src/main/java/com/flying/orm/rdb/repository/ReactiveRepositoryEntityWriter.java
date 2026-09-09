package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 负责单实体写入和自动版本锁，逻辑删除使用绑定表单的统一规则。
 *
 * <p>它只拼出 {@link WriteSpec} 并交给 {@link ReactiveFormClient}。Scope、安全校验、租户校验、SQL 渲染、
 * 执行保护和事务仍由表单客户端及其下层执行器完成，这里没有第二套写入实现。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryEntityWriter<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryEntityWriter(ReactiveFormClient client,
                                   DynamicForm form,
                                   EntityMetadata<T> metadata,
                                   EntityValues<T> entityValues,
                                   ReactiveRepositoryLifecycleSupport<T> lifecycle) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = RepositoryEntityIdSupport.create(metadata, client.entityModels());
    }

    Mono<Long> insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        if (lifecycle.hasWork(EntityLifecyclePhase.POST_PERSIST)) {
            return lifecycle.persist(safeEntity, client::currentTransaction,
                                     external -> executeInsert(safeEntity, external));
        }
        return lifecycle.fire(EntityLifecyclePhase.PRE_PERSIST, safeEntity, null)
                .then(Mono.defer(() -> {
                    if (!ids.databaseGenerated()) {
                        return executeInsert(safeEntity, false);
                    }
                    return client.currentTransaction()
                            .map(Optional::of)
                            .defaultIfEmpty(Optional.empty())
                            .flatMap(transaction -> transaction
                                    .map(context -> executeInsert(safeEntity, true)
                                            .contextWrite(current -> R2dbcTransactionParticipant.bind(
                                                    current, context)))
                                    .orElseGet(() -> executeInsert(safeEntity, false)
                                            .contextWrite(current -> current.put(
                                                    R2dbcTransactionParticipant.class,
                                                    R2dbcTransactionParticipant.none()))));
                }));
    }

    Mono<Long> update(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, client::currentTransaction, () -> {
            Map<String, Object> allValues = metadata.versionField().isPresent()
                    ? entityValues.read(safeEntity) : null;
            Map<String, Object> row = entityValues.readForUpdate(safeEntity);
            OptimisticLockOptions lock = allValues == null ? null
                    : RepositoryOptimisticLocks.incrementLock(metadata, allValues).orElse(null);
            if (lock != null) {
                row = RepositoryOptimisticLocks.withoutLockField(row, lock);
            }
            WriteSpec spec = WriteSpec.updateOwned(form, row, where);
            return client.update(lock == null ? spec : spec.withLock(lock));
        });
    }

    Mono<Long> delete(ConditionGroup where) {
        return client.delete(WriteSpec.delete(form, where));
    }

    Mono<Long> delete(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, client::currentTransaction, () -> {
            OptimisticLockOptions lock = metadata.versionField().isPresent()
                    ? RepositoryOptimisticLocks.incrementLock(metadata, entityValues.read(safeEntity)).orElse(null)
                    : null;
            WriteSpec spec = WriteSpec.delete(form, where);
            return client.delete(lock == null ? spec : spec.withLock(lock));
        });
    }

    Mono<Long> physicalDelete(ConditionGroup where) {
        return client.physicalDelete(WriteSpec.delete(form, where));
    }

    private Mono<Long> executeInsert(T entity, boolean externalTransaction) {
        ids.prepare(entity);
        WriteSpec spec = WriteSpec.insertOwned(form, entityValues.readForInsert(entity));
        if (!ids.databaseGenerated()) {
            return client.insert(spec);
        }
        return client.insertReturningKeys(spec)
                .onErrorMap(GeneratedKeyReadException.class, failure -> {
                    Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(failure);
                    if (preferred instanceof VirtualMachineError fatal) {
                        return fatal;
                    }
                    return new GeneratedKeyResolutionException(
                            failure.affectedRows(), externalTransaction
                                    ? GeneratedKeyResolutionException.WriteState.ENLISTED
                                    : GeneratedKeyResolutionException.WriteState.UNKNOWN,
                            failure);
                })
                .map(result -> {
                    applyGeneratedKey(entity, result, externalTransaction);
                    return result.affectedRows();
                });
    }

    private void applyGeneratedKey(T entity,
                                   com.flying.orm.rdb.execution.SqlWriteResult result,
                                   boolean externalTransaction) {
        try {
            ids.applyGeneratedKey(entity, result);
        } catch (RuntimeException failure) {
            Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(failure);
            if (preferred instanceof VirtualMachineError fatal) {
                throw fatal;
            }
            throw new GeneratedKeyResolutionException(
                    result.affectedRows(), externalTransaction
                    ? GeneratedKeyResolutionException.WriteState.ENLISTED
                    : GeneratedKeyResolutionException.WriteState.COMMITTED,
                    failure);
        }
    }

}
