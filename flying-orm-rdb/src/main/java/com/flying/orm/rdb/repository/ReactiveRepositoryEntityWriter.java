package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * 负责单实体写入以及逻辑删除、乐观锁条件的组合。
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
    private final Function<T, Map<String, Object>> values;
    private final Function<T, Map<String, Object>> insertValues;
    private final Function<T, Map<String, Object>> updateValues;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryEntityWriter(ReactiveFormClient client,
                                   DynamicForm form,
                                   EntityMetadata<T> metadata,
                                   Function<T, Map<String, Object>> values,
                                   Function<T, Map<String, Object>> insertValues,
                                   Function<T, Map<String, Object>> updateValues,
                                   ReactiveRepositoryLifecycleSupport<T> lifecycle) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
        this.insertValues = Objects.requireNonNull(insertValues, "repository insert values must not be null");
        this.updateValues = Objects.requireNonNull(updateValues, "repository update values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = RepositoryEntityIdSupport.create(metadata, client.entityModels().idGenerator());
    }

    Mono<Long> insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.fire(EntityLifecyclePhase.PRE_PERSIST, safeEntity, null)
                .then(Mono.defer(() -> {
                    ids.prepare(safeEntity);
                    WriteSpec spec = WriteSpec.insert(form, insertValues.apply(safeEntity));
                    if (!ids.databaseGenerated()) {
                        return client.insert(spec);
                    }
                    return client.insertReturningKeys(spec).map(result -> {
                        ids.applyGeneratedKey(safeEntity, result);
                        return result.affectedRows();
                    });
                }))
                .flatMap(rows -> lifecycle.fire(EntityLifecyclePhase.POST_PERSIST, safeEntity, rows).thenReturn(rows));
    }

    Mono<Long> update(T entity, ConditionGroup where, Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, () -> {
            Map<String, Object> allValues = values.apply(safeEntity);
            Map<String, Object> row = updateValues.apply(safeEntity);
            ConditionGroup activeWhere = RepositoryLogicDeletes.activeWhere(metadata, form, where);
            Optional<OptimisticLockOptions> lock = RepositoryOptimisticLocks.incrementLock(metadata, allValues);
            return lock.map(value -> executeUpdate(RepositoryOptimisticLocks.withoutLockField(row, value),
                                                   activeWhere, prepend(value, modifiers)))
                    .orElseGet(() -> executeUpdate(row, activeWhere, modifiers));
        });
    }

    Mono<Long> updateWithLock(T entity,
                              ConditionGroup where,
                              OptimisticLockOptions lock,
                              Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, () -> executeUpdate(
                RepositoryOptimisticLocks.withoutLockField(updateValues.apply(safeEntity), lock),
                RepositoryLogicDeletes.activeWhere(metadata, form, where),
                prepend(lock, modifiers)));
    }

    Mono<Long> delete(ConditionGroup where, Object... modifiers) {
        ConditionGroup activeWhere = RepositoryLogicDeletes.activeWhere(metadata, form, where);
        return RepositoryLogicDeletes.deleteValues(metadata, form)
                .map(values -> executeUpdate(values, activeWhere, modifiers))
                .orElseGet(() -> executeDelete(where, modifiers));
    }

    Mono<Long> delete(T entity, ConditionGroup where, Object... modifiers) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, () -> {
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

    Mono<Long> physicalDelete(ConditionGroup where, Object... modifiers) {
        return client.physicalDelete(applyWriteModifiers(WriteSpec.delete(form, where), modifiers));
    }

    private Mono<Long> executeUpdate(Map<String, Object> row,
                                     ConditionGroup where,
                                     Object... modifiers) {
        return client.update(applyWriteModifiers(WriteSpec.update(form, row, where), modifiers));
    }

    private Mono<Long> executeDelete(ConditionGroup where, Object... modifiers) {
        return client.delete(applyWriteModifiers(WriteSpec.delete(form, where), modifiers));
    }

    /**
     * 乐观锁要和调用方传入的 Scope、执行保护放进同一个扁平数组。直接把 {@code modifiers}
     * 当成另一个 varargs 参数会得到嵌套的 {@code Object[]}，后面的类型检查就会把它当成未知修饰项。
     */
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
