package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * 同步单实体写入协作者。
 *
 * <p>它沿用响应式 Repository 的规则：实体版本列自动转成乐观锁条件，版本列不重复进入 SET；实体逻辑删除
 * 转成 update；字段、Scope、租户和执行保护仍由 {@link SyncFormClient} 的表单执行路径统一校验。</p>
 */
final class SyncRepositoryEntityWriter<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final Function<T, Map<String, Object>> values;
    private final Function<T, Map<String, Object>> insertValues;
    private final Function<T, Map<String, Object>> updateValues;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    SyncRepositoryEntityWriter(SyncFormClient client,
                               DynamicForm form,
                               EntityMetadata<T> metadata,
                               Function<T, Map<String, Object>> values,
                               Function<T, Map<String, Object>> insertValues,
                               Function<T, Map<String, Object>> updateValues,
                               SyncRepositoryLifecycleSupport<T> lifecycle) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
        this.insertValues = Objects.requireNonNull(insertValues, "repository insert values must not be null");
        this.updateValues = Objects.requireNonNull(updateValues, "repository update values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = RepositoryEntityIdSupport.create(metadata, client.entityModels().idGenerator());
    }

    long insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        if (lifecycle.hasWork(EntityLifecyclePhase.POST_PERSIST)) {
            return lifecycle.persist(safeEntity, client::currentTransaction,
                                     external -> executeInsert(safeEntity, external));
        }
        lifecycle.fire(EntityLifecyclePhase.PRE_PERSIST, safeEntity, null);
        boolean external = ids.databaseGenerated() && client.currentTransaction().isPresent();
        return executeInsert(safeEntity, external);
    }

    long update(T entity, ConditionGroup where, Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, client::currentTransaction, () -> {
            Map<String, Object> allValues = values.apply(safeEntity);
            Map<String, Object> row = updateValues.apply(safeEntity);
            ConditionGroup activeWhere = RepositoryLogicDeletes.activeWhere(metadata, form, where);
            Optional<OptimisticLockOptions> lock = RepositoryOptimisticLocks.incrementLock(metadata, allValues);
            return lock.map(value -> executeUpdate(RepositoryOptimisticLocks.withoutLockField(row, value),
                                                   activeWhere, prepend(value, modifiers)))
                    .orElseGet(() -> executeUpdate(row, activeWhere, modifiers));
        });
    }

    long updateWithLock(T entity, ConditionGroup where, OptimisticLockOptions lock, Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, client::currentTransaction, () -> executeUpdate(
                RepositoryOptimisticLocks.withoutLockField(updateValues.apply(safeEntity), lock),
                RepositoryLogicDeletes.activeWhere(metadata, form, where), prepend(lock, modifiers)));
    }

    long delete(ConditionGroup where, Object... modifiers) {
        ConditionGroup activeWhere = RepositoryLogicDeletes.activeWhere(metadata, form, where);
        return RepositoryLogicDeletes.deleteValues(metadata, form)
                .map(values -> executeUpdate(values, activeWhere, modifiers))
                .orElseGet(() -> executeDelete(where, modifiers));
    }

    long delete(T entity, ConditionGroup where, Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, client::currentTransaction, () -> {
            ConditionGroup activeWhere = RepositoryLogicDeletes.activeWhere(metadata, form, where);
            Optional<OptimisticLockOptions> lock = RepositoryOptimisticLocks.incrementLock(
                    metadata, values.apply(safeEntity));
            Optional<Map<String, Object>> logicDelete = RepositoryLogicDeletes.deleteValues(metadata, form);
            if (logicDelete.isPresent()) {
                return lock.map(value -> executeUpdate(logicDelete.get(), activeWhere, prepend(value, modifiers)))
                        .orElseGet(() -> executeUpdate(logicDelete.get(), activeWhere, modifiers));
            }
            return lock.map(value -> executeDelete(where, prepend(value, modifiers)))
                    .orElseGet(() -> executeDelete(where, modifiers));
        });
    }

    long physicalDelete(ConditionGroup where, Object... modifiers) {
        return client.physicalDelete(applyWriteModifiers(WriteSpec.delete(form, where), modifiers));
    }

    private long executeUpdate(Map<String, Object> row, ConditionGroup where, Object... modifiers) {
        return client.update(applyWriteModifiers(WriteSpec.update(form, row, where), modifiers));
    }

    private long executeDelete(ConditionGroup where, Object... modifiers) {
        return client.delete(applyWriteModifiers(WriteSpec.delete(form, where), modifiers));
    }

    private long executeInsert(T entity, boolean externalTransaction) {
        ids.prepare(entity);
        WriteSpec spec = WriteSpec.insert(form, insertValues.apply(entity));
        if (!ids.databaseGenerated()) {
            return client.insert(spec);
        }
        final com.flying.orm.rdb.execution.SqlWriteResult result;
        try {
            result = client.insertReturningKeys(spec);
        } catch (GeneratedKeyReadException failure) {
            Throwable preferred = RepositoryFailureSupport.preferVirtualMachineError(failure);
            if (preferred instanceof VirtualMachineError fatal) {
                throw fatal;
            }
            throw new GeneratedKeyResolutionException(
                    failure.affectedRows(), externalTransaction
                    ? GeneratedKeyResolutionException.WriteState.ENLISTED
                    : GeneratedKeyResolutionException.WriteState.UNKNOWN,
                    failure);
        }
        applyGeneratedKey(entity, result, externalTransaction);
        return result.affectedRows();
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

    private static Object[] prepend(Object first, Object[] modifiers) {
        Object[] safeModifiers = Objects.requireNonNull(modifiers, "repository write modifiers must not be null");
        Object[] combined = new Object[safeModifiers.length + 1];
        combined[0] = Objects.requireNonNull(first, "repository write modifier must not be null");
        System.arraycopy(safeModifiers, 0, combined, 1, safeModifiers.length);
        return combined;
    }

    private WriteSpec applyWriteModifiers(WriteSpec initial, Object... modifiers) {
        WriteSpec current = initial;
        for (Object modifier : modifiers) {
            current = switch (Objects.requireNonNull(modifier, "repository write modifier must not be null")) {
                case DataScope scope -> current.withScope(scope);
                case OptimisticLockOptions lock -> current.withLock(lock);
                case SqlExecutionOptions options -> current.withExecutionOptions(options);
                default -> throw new IllegalArgumentException(
                        "unsupported repository write modifier: " + modifier.getClass().getName());
            };
        }
        return current;
    }
}
